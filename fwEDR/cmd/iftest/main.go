// Command iftest connects to a simulated device's SNMP exactly like the EDR
// poller does and walks the interface OIDs — proving whether the sim returns
// interface data and whether EDR's collection works. If it returns interfaces,
// the empty interfaces/interface_addresses tables were a POLL problem (throttle),
// not the collection code.  Run:  go run ./cmd/iftest -community 192.168.1.8
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/faberwork/fwedr/internal/snmp"
	"github.com/faberwork/fwedr/internal/target"
	"github.com/faberwork/fwedr/pkg/config"
)

func main() {
	ip := flag.String("ip", "127.0.0.1", "SNMP socket target (loopback in sim mode)")
	comm := flag.String("community", "192.168.1.8", "community = device mgmt IP (sim routing key)")
	shards := flag.Int("shards", 1, "shards (1 = port 161; >1 = shard_base_port + hash%shards)")
	flag.Parse()

	cfg := config.SNMPConfig{
		Version: 2, Timeout: 5000, Retries: 1,
		Shards: *shards, ShardBasePort: 16100, MaxConcurrent: 4,
	}
	fmt.Printf("(shards=%d → port %d)\n", *shards, snmp.ShardPort(*comm, *shards, 16100))
	t := &target.Target{
		IP: *ip, MgmtIP: *comm, Community: *comm,
		SNMPVersion: 2, DeviceType: "firewall",
	}
	fmt.Printf("connecting %s:161 community=%s ...\n", *ip, *comm)
	cl, err := snmp.NewClient(t, cfg)
	if err != nil {
		fmt.Println("CONNECT FAILED:", err)
		os.Exit(1)
	}
	defer cl.Close()

	walks := []struct{ name, oid string }{
		{"ifDescr   (2.2.1.2)", snmp.OIDIfDescr},
		{"ifName    (31.1.1.1.1)", snmp.OIDIfName},
		{"ifAdminStatus", snmp.OIDIfAdminStatus},
		{"ifOperStatus", snmp.OIDIfOperStatus},
		{"ifHighSpeed", snmp.OIDIfHighSpeed},
	}
	total := 0
	for _, w := range walks {
		pdus, werr := cl.Walk(w.oid)
		fmt.Printf("%-24s -> %2d entries  err=%v\n", w.name, len(pdus), werr)
		total += len(pdus)
		for i, p := range pdus {
			if i < 3 {
				fmt.Printf("        %s = %v\n", p.Name, snmp.PDUString(p))
			}
		}
	}
	fmt.Println("--------------------------------------------------")
	if total == 0 {
		fmt.Println("RESULT: FAIL — sim returned NO interface data on this socket/community.")
		fmt.Println("        The SNMP responder isn't answering EDR here → that's why")
		fmt.Println("        interfaces/interface_addresses are empty (no MEDIUM data).")
		os.Exit(1)
	}
	fmt.Printf("RESULT: PASS — sim returns interface data (%d OID rows).\n", total)
	fmt.Println("        EDR's interface collection works. Empty tables were the poll")
	fmt.Println("        config (max_concurrent/rate too low) — now 4/40. Re-run the walk.")
}
