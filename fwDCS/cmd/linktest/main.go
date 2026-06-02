// Command linktest is a standalone integration test for link-event correlation.
//
// Link events now live in the `events` table: a linkDown/linkUp that DCS
// correlates to a topology_links row is stored as ONE enriched event carrying the
// peer endpoint (dst_*) + link_id; the peer's duplicate trap is suppressed.
//
// It reproduces the wrong-neighbour / off-by-one bug and proves the fix, using the
// REAL port convention: topology_links.port_name is the 0-based interface index,
// while a trap carries the 1-based IF-MIB ifIndex (port_name + 1) plus an "ethN"
// name.
//
//	SCENARIO — DC1-LB2 has TWO network links:
//	    DC1-FW1[port3/eth3] <-> DC1-LB2[port0/eth0]   (the one we break)
//	    DC1-FW2[port3]      <-> DC1-LB2[port1]         (DECOY)
//	Break FW1<->LB2: FW1 ifIndex=4 (eth3), LB2 ifIndex=1 (eth0). The OLD bug matched
//	LB2's RAW ifIndex 1 against LB2 port 1 -> the FW2 decoy. The fix derives the
//	0-based port (eth0 -> 0). The enriched event MUST be FW1<->LB2, never FW2.
//
// This drives the SAME store calls the ingest pipeline's handleLinkTrap uses.
// Run:  go run ./cmd/linktest
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/faberwork/fwdcs/internal/store"
	v1 "github.com/faberwork/fwdcs/proto/v1"
)

const (
	org = "faber"
	net = "net-prod"
	grp = "grp-core"
)

func main() {
	dsn := os.Getenv("DCS_DSN")
	if dsn == "" {
		dsn = "postgresql://fwdcim:fwdcim@127.0.0.1:5438/fwdcim?sslmode=disable"
	}
	ctx := context.Background()
	db, err := store.New(ctx, dsn) // opens pool + runs migrations
	if err != nil {
		fmt.Println("FATAL: connect/migrate:", err)
		os.Exit(1)
	}
	defer db.Close()

	pass := true
	step := func(name string, ok bool, detail string) {
		mark := "PASS"
		if !ok {
			mark = "FAIL"
			pass = false
		}
		fmt.Printf("[%s] %-38s %s\n", mark, name, detail)
	}

	// Clean any prior TST-* rows first so counts are deterministic.
	cleanup(ctx, db)

	// ── Seed three devices ─────────────────────────────────────────────────────
	mkDev := func(host, mgmt string) string {
		pkt := &v1.TelemetryPacket{
			OrgId: org, NetworkId: net, GroupId: grp,
			SourceId: host, Name: "system.uptime_centiseconds", Kind: "metric",
			TimestampNs: time.Now().UnixNano(),
			Meta: map[string]string{
				"hostname": host, "mgmt_ip": mgmt, "device_type": "switch",
				"snmp_enabled": "true", "collector_agent": "EDR",
			},
		}
		_ = db.UpsertDevice(ctx, pkt)
		id, _ := db.DeviceIDByIP(ctx, org, "", "", net, grp, mgmt)
		return id
	}
	fw1 := mkDev("TST-FW1", "10.99.0.10")
	lb2 := mkDev("TST-LB2", "10.99.0.11")
	fw2 := mkDev("TST-FW2", "10.99.0.12")
	step("seed devices", fw1 != "" && lb2 != "" && fw2 != "",
		fmt.Sprintf("fw1=%s lb2=%s fw2=%s", short(fw1), short(lb2), short(fw2)))
	if fw1 == "" || lb2 == "" || fw2 == "" {
		os.Exit(1)
	}

	// ── Seed TWO links sharing LB2 (0-based ports) ─────────────────────────────
	if err := db.UpsertLinks(ctx, []store.TopologyLink{
		{Layer: "network", SrcDeviceID: fw1, SrcPortName: "3", DstDeviceID: lb2, DstPortName: "0", Protocol: "topo"},
		{Layer: "network", SrcDeviceID: fw2, SrcPortName: "3", DstDeviceID: lb2, DstPortName: "1", Protocol: "topo"},
	}); err != nil {
		step("seed topology_links", false, err.Error())
		os.Exit(1)
	}
	step("seed topology_links", true, "FW1[3]->LB2[0] and FW2[3]->LB2[1]")

	linkID, _ := linkIDOf(ctx, db, fw1, lb2)

	// ── Break FW1<->LB2: 1-based ifIndex (port+1) + ethN names ─────────────────
	w1 := applyTrap(ctx, db, fw1, "TST-FW1", 4, "eth3", true) // FW1's trap
	w2 := applyTrap(ctx, db, lb2, "TST-LB2", 1, "eth0", true) // LB2's trap (peer)
	step("exactly one enriched event written", w1 != w2, fmt.Sprintf("fw1_wrote=%v lb2_wrote=%v", w1, w2))

	// ── Assert the enriched linkDown event is FW1<->LB2, never FW2 ──────────────
	ev, evErr := latestLinkEvent(ctx, db, linkID)
	step("linkDown event exists for the FW1<->LB2 link", evErr == nil && ev.name == "linkDown", ev.describe())
	step("event endpoints are FW1 and LB2 (NOT FW2)",
		evErr == nil && ev.pair("TST-FW1", "TST-LB2") && ev.src != "TST-FW2" && ev.dst != "TST-FW2", ev.describe())
	nFW2, _ := countLinkEventsTouching(ctx, db, fw2)
	step("NO link event references FW2", nFW2 == 0, fmt.Sprintf("fw2_link_events=%d", nFW2))
	nForLink, _ := countLinkEvents(ctx, db, linkID)
	step("exactly ONE event row for this transition", nForLink == 1, fmt.Sprintf("rows=%d", nForLink))

	// ── Assert topology_links is_active ────────────────────────────────────────
	brokenActive, _ := linkActive(ctx, db, fw1, lb2)
	decoyActive, _ := linkActive(ctx, db, fw2, lb2)
	step("FW1<->LB2 is_active=false", brokenActive == false, fmt.Sprintf("is_active=%v", brokenActive))
	step("FW2<->LB2 decoy is_active=true (untouched)", decoyActive == true, fmt.Sprintf("is_active=%v", decoyActive))

	// ── Restore: linkUp ────────────────────────────────────────────────────────
	u1 := applyTrap(ctx, db, fw1, "TST-FW1", 4, "eth3", false)
	u2 := applyTrap(ctx, db, lb2, "TST-LB2", 1, "eth0", false)
	step("linkUp writes exactly one event", u1 != u2, fmt.Sprintf("u1=%v u2=%v", u1, u2))
	evUp, _ := latestLinkEvent(ctx, db, linkID)
	step("linkUp event is FW1<->LB2", evUp.name == "linkUp" && evUp.pair("TST-FW1", "TST-LB2"), evUp.describe())
	upActive, _ := linkActive(ctx, db, fw1, lb2)
	step("FW1<->LB2 is_active=true again", upActive == true, fmt.Sprintf("is_active=%v", upActive))

	cleanup(ctx, db)

	fmt.Println("--------------------------------------------------")
	if pass {
		fmt.Println("RESULT: PASS — enriched link event is FW1<->LB2; FW2 never involved;")
		fmt.Println("        one event per transition; is_active correct on both links.")
	} else {
		fmt.Println("RESULT: FAIL — see failing steps.")
		os.Exit(1)
	}
}

// applyTrap mirrors Pipeline.handleLinkTrap: correlate (exact, off-by-one safe),
// flip is_active, and on a real transition write ONE enriched event. Returns
// whether it wrote an event (false = suppressed peer/repeat or no correlation).
func applyTrap(ctx context.Context, db *store.DB, deviceID, srcHost string, ifIndex int, ifName string, down bool) bool {
	row, found, err := db.CorrelateLinkByPort(ctx, deviceID, ifIndex, ifName)
	if err != nil || !found {
		return false
	}
	peerID, peerHost, peerPort := row.DstDeviceID, row.DstHostname, row.DstPort
	senderPort := row.SrcPort
	if row.DstDeviceID == deviceID {
		peerID, peerHost, peerPort = row.SrcDeviceID, row.SrcHostname, row.SrcPort
		senderPort = row.DstPort
	}
	changed, err := db.SetLinkActive(ctx, row, down)
	if err != nil || !changed {
		return false
	}
	name := "linkUp"
	if down {
		name = "linkDown"
	}
	pkt := &v1.TelemetryPacket{
		OrgId: org, NetworkId: net, GroupId: grp,
		SourceId: srcHost, Name: name, Kind: "trap", Severity: "major",
		TimestampNs: time.Now().UnixNano(),
		Meta: map[string]string{
			"vb.1.3.6.1.2.1.2.2.1.1.1": strconv.Itoa(ifIndex),
			"vb.1.3.6.1.2.1.2.2.1.2.1": ifName,
			"trap_oid":                 "1.3.6.1.6.3.1.1.5.3",
			"collector_agent":          "EDR",
			"src_port_name":            senderPort,
			"dst_device_id":            peerID,
			"dst_hostname":             peerHost,
			"dst_port_name":            peerPort,
			"link_id":                  row.LinkID,
		},
	}
	_ = db.WriteEvent(ctx, deviceID, srcHost, pkt)
	return true
}

// ─── assertions / helpers ──────────────────────────────────────────────────────

type linkEvent struct {
	name, src, dst string
	found          bool
}

func (e linkEvent) describe() string {
	if !e.found {
		return "NOT FOUND"
	}
	return fmt.Sprintf("%s: %s<->%s", e.name, e.src, e.dst)
}
func (e linkEvent) pair(h1, h2 string) bool {
	return (e.src == h1 && e.dst == h2) || (e.src == h2 && e.dst == h1)
}

func linkIDOf(ctx context.Context, db *store.DB, a, b string) (string, error) {
	var id string
	err := db.Pool().QueryRow(ctx, `
		SELECT id::text FROM topology_links
		WHERE layer='network' AND (
		      (src_device_id=$1::uuid AND dst_device_id=$2::uuid)
		   OR (src_device_id=$2::uuid AND dst_device_id=$1::uuid))
		LIMIT 1`, a, b).Scan(&id)
	return id, err
}

func latestLinkEvent(ctx context.Context, db *store.DB, linkID string) (linkEvent, error) {
	var e linkEvent
	err := db.Pool().QueryRow(ctx, `
		SELECT event_name, COALESCE(source_hostname,''), COALESCE(dst_hostname,'')
		FROM events WHERE link_id=$1::uuid ORDER BY ts DESC LIMIT 1`, linkID).Scan(&e.name, &e.src, &e.dst)
	if err != nil {
		return e, err
	}
	e.found = true
	return e, nil
}

func countLinkEvents(ctx context.Context, db *store.DB, linkID string) (int, error) {
	var n int
	err := db.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE link_id=$1::uuid`, linkID).Scan(&n)
	return n, err
}

func countLinkEventsTouching(ctx context.Context, db *store.DB, dev string) (int, error) {
	var n int
	err := db.Pool().QueryRow(ctx, `
		SELECT count(*) FROM events
		WHERE link_id IS NOT NULL AND (device_id=$1::uuid OR dst_device_id=$1::uuid)`, dev).Scan(&n)
	return n, err
}

func linkActive(ctx context.Context, db *store.DB, a, b string) (bool, error) {
	var active bool
	err := db.Pool().QueryRow(ctx, `
		SELECT is_active FROM topology_links
		WHERE layer='network' AND (
		      (src_device_id=$1::uuid AND dst_device_id=$2::uuid)
		   OR (src_device_id=$2::uuid AND dst_device_id=$1::uuid))
		LIMIT 1`, a, b).Scan(&active)
	return active, err
}

func cleanup(ctx context.Context, db *store.DB) {
	_, _ = db.Pool().Exec(ctx, `DELETE FROM events
		WHERE source_hostname LIKE 'TST-%' OR dst_hostname LIKE 'TST-%'`)
	_, _ = db.Pool().Exec(ctx, `DELETE FROM devices WHERE hostname LIKE 'TST-%'`)
}

func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
