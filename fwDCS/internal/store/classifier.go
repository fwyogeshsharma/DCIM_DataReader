package store

// classifier.go — fabric-role inference for switches.
//
// device_type tells you the physical kind (router/switch/server/...), but every
// CORE/SPINE/LEAF box reports device_type='switch', so the fabric tier is lost.
// This computes that tier (the "role") from three signals already in the DB:
//
//	Signal 1  hostname (sysName)            weight 3   — DC1-SP1 → spine, etc.
//	Signal 2  model / sys_description        weight 2   — vendor platform tier
//	Signal 3  LLDP-neighbor profile          weight 3   — topology_links + neighbor
//	                                                      device_type (server/switch ratios)
//
// role = argmax(score); confidence = score[role] / sum(scores). Thresholds:
// >=0.6 inferred, 0.4–0.6 suggested, <0.4 unclassified. Signals degrade
// gracefully — any missing signal just contributes nothing; hostname alone still
// classifies. Rows with role_overridden=TRUE are never touched (admin override
// persists across re-discovery).
//
// Runs in DCS right after RecomputeHierarchy (graph + neighbor types are ready).

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// roleRules holds the (optionally externally-configurable) lookup tables and
// scoring knobs. Defaults are compiled in; LoadRoleRules can override them from
// a YAML file without a code change.
type roleRules struct {
	hostPatterns map[string][]*regexp.Regexp
	modelMap     map[string][]string
	wHost        int
	wModel       int
	wNeighbor    int
	thInferred   float64
	thSuggested  float64
}

// activeRoleRules is read by ClassifyRoles. Mutated once at startup by
// LoadRoleRules (before the recompute goroutine starts) — not concurrency-safe
// to change at runtime.
var activeRoleRules = defaultRoleRules()

func defaultRoleRules() *roleRules {
	host := map[string][]string{
		// Tuned for the simulator's DC{n}-{ROLE}{num} naming plus common real-world
		// conventions. Matched case-insensitively against sysName.
		"core":         {`(^|[-_])core\d*`, `(^|[-_])cr\d`},
		"spine":        {`(^|[-_])spine`, `(^|[-_])sp\d`},
		"leaf":         {`(^|[-_])leaf`, `(^|[-_])lf\d`, `(^|[-_])tor(\d|$)`},
		"access":       {`(^|[-_])acc(ess)?\d*`, `(^|[-_])a-?sw`},
		"distribution": {`(^|[-_])dist`, `(^|[-_])agg`},
	}
	compiled := make(map[string][]*regexp.Regexp, len(host))
	for role, pats := range host {
		for _, p := range pats {
			compiled[role] = append(compiled[role], regexp.MustCompile(`(?i)`+p))
		}
	}
	return &roleRules{
		hostPatterns: compiled,
		modelMap: map[string][]string{
			"spine":  {"nexus 93", "nexus 96", "7050", "7060", "qfx5200", "ptx"},
			"core":   {"nexus 95", "nexus 77", "asr 9", "qfx10", "mx960", "mx480"},
			"leaf":   {"nexus 5", "93180", "qfx5100", "7020", "7010"},
			"access": {"catalyst 9300", "catalyst 2960", "2930", "2540", "ex2300", "ex3300"},
		},
		wHost:       3,
		wModel:      2,
		wNeighbor:   3,
		thInferred:  0.6,
		thSuggested: 0.4,
	}
}

// roleRulesFile is the on-disk (YAML) form of the lookup tables + knobs. All
// fields optional — anything omitted keeps its compiled-in default.
type roleRulesFile struct {
	HostPatterns map[string][]string `yaml:"host_patterns"`
	ModelMap     map[string][]string `yaml:"model_map"`
	Weights      struct {
		Hostname int `yaml:"hostname"`
		Model    int `yaml:"model"`
		Neighbor int `yaml:"neighbor"`
	} `yaml:"weights"`
	Thresholds struct {
		Inferred  float64 `yaml:"inferred"`
		Suggested float64 `yaml:"suggested"`
	} `yaml:"thresholds"`
}

// LoadRoleRules overrides the compiled-in classifier lookup tables/knobs from a
// YAML file (the "configurable without a code change" requirement). Call once at
// startup, before the recompute goroutine begins — it mutates a package global.
func LoadRoleRules(path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("role rules: read %s: %w", path, err)
	}
	var f roleRulesFile
	if err := yaml.Unmarshal(b, &f); err != nil {
		return fmt.Errorf("role rules: parse %s: %w", path, err)
	}
	r := defaultRoleRules()
	if len(f.HostPatterns) > 0 {
		compiled := make(map[string][]*regexp.Regexp, len(f.HostPatterns))
		for role, pats := range f.HostPatterns {
			for _, p := range pats {
				re, err := regexp.Compile(`(?i)` + p)
				if err != nil {
					return fmt.Errorf("role rules: bad pattern %q for %q: %w", p, role, err)
				}
				compiled[role] = append(compiled[role], re)
			}
		}
		r.hostPatterns = compiled
	}
	if len(f.ModelMap) > 0 {
		r.modelMap = f.ModelMap
	}
	if f.Weights.Hostname > 0 {
		r.wHost = f.Weights.Hostname
	}
	if f.Weights.Model > 0 {
		r.wModel = f.Weights.Model
	}
	if f.Weights.Neighbor > 0 {
		r.wNeighbor = f.Weights.Neighbor
	}
	if f.Thresholds.Inferred > 0 {
		r.thInferred = f.Thresholds.Inferred
	}
	if f.Thresholds.Suggested > 0 {
		r.thSuggested = f.Thresholds.Suggested
	}
	activeRoleRules = r
	return nil
}

// roleResult mirrors the spec's RoleResult.
type roleResult struct {
	role       string
	confidence float64
	source     string   // inferred | suggested | unclassified
	signals    []string // e.g. ["hostname:spine","lldp:spine"] — logged, not stored
}

// typeToRole maps a non-switch device_type straight to a role. The bool reports
// whether the device is a fabric switch that needs multi-signal scoring.
func typeToRole(dt string) (role string, isFabricSwitch bool) {
	switch dt {
	case "switch":
		return "", true
	case "router":
		return "edge_router", false
	case "firewall":
		return "firewall", false
	case "load_balancer":
		return "load_balancer", false
	case "server":
		return "server", false
	case "oob_switch":
		return "oob_switch", false
	case "ups":
		return "ups", false
	case "pdu":
		return "pdu", false
	case "floor_pdu":
		return "floor_pdu", false
	case "sensor":
		return "sensor", false
	case "energy_monitor":
		return "energy_monitor", false
	default:
		return "unclassified", false
	}
}

// classifySwitch scores a single fabric switch across the three signals.
// nbTypes = neighbor device_type → count; total = neighbor count.
func classifySwitch(hostname, model, descr string, nbTypes map[string]int, total int, r *roleRules) roleResult {
	score := map[string]int{}
	var signals []string
	add := func(role string, w int, tag string) {
		score[role] += w
		signals = append(signals, tag)
	}

	// ── Signal 1: hostname ────────────────────────────────────────────────────
	hostRole := ""
	hl := strings.ToLower(hostname)
	// deterministic role order so a hostname that somehow matches two roles is stable
	for _, role := range roleOrder {
		for _, re := range r.hostPatterns[role] {
			if re.MatchString(hl) {
				add(role, r.wHost, "hostname:"+role)
				if hostRole == "" {
					hostRole = role
				}
				break
			}
		}
	}

	// ── Signal 2: model / sys_description ─────────────────────────────────────
	ml := strings.ToLower(model + " " + descr)
	for _, role := range roleOrder {
		for _, sub := range r.modelMap[role] {
			if sub != "" && strings.Contains(ml, sub) {
				add(role, r.wModel, "model:"+role)
				break
			}
		}
	}

	// ── Signal 3: LLDP-neighbor profile ───────────────────────────────────────
	switches := nbTypes["switch"]
	servers := nbTypes["server"]
	endpoints := nbTypes["workstation"] + nbTypes["ap"]
	uplinkers := nbTypes["router"] + nbTypes["firewall"] + nbTypes["load_balancer"]
	if total > 0 {
		swRatio := float64(switches) / float64(total)
		srvRatio := float64(servers) / float64(total)
		epRatio := float64(endpoints) / float64(total)
		if swRatio > 0.85 {
			add("spine", r.wNeighbor, "lldp:spine")
		}
		if srvRatio > 0.5 {
			add("leaf", r.wNeighbor, "lldp:leaf")
		}
		if epRatio > 0.5 {
			add("access", r.wNeighbor, "lldp:access")
		}
		if total >= 16 && swRatio > 0.7 {
			add("spine", 2, "lldp:spine-fanout")
		}
		if total <= 4 {
			add("access", 2, "lldp:access-small")
		}
		if switches >= 2 && servers >= 4 {
			add("leaf", 2, "lldp:tor")
		}
		// Core is the only switch tier that fans up into routers/firewalls (the
		// fabric edge). Without this, a core that mostly faces spines scores as a
		// spine. Not in the original spec — added to separate core from spine.
		if uplinkers >= 1 && switches >= 1 {
			add("core", 2, "lldp:core-uplink")
		}
	}

	// ── argmax + confidence ───────────────────────────────────────────────────
	sum := 0
	for _, v := range score {
		sum += v
	}
	if sum == 0 {
		return roleResult{role: "unclassified", confidence: 0, source: "unclassified", signals: signals}
	}
	best, bestScore := "", -1
	for _, role := range roleOrder { // deterministic tie order: core>spine>leaf>distribution>access
		if score[role] > bestScore {
			best, bestScore = role, score[role]
		}
	}
	// Hostname is the strongest human-intent signal: on a tie, prefer it. This is
	// what separates core (hostname) from spine (neighbor profile) in a Clos fabric.
	if hostRole != "" && score[hostRole] == bestScore {
		best = hostRole
	}
	conf := float64(bestScore) / float64(sum)

	src := "unclassified"
	switch {
	case conf >= r.thInferred:
		src = "inferred"
	case conf >= r.thSuggested:
		src = "suggested"
	}
	if src == "unclassified" {
		best = "unclassified"
	}
	return roleResult{role: best, confidence: conf, source: src, signals: signals}
}

// roleOrder is the deterministic precedence for argmax tie-breaks and for
// iterating the lookup tables (so a hostname matching two roles is stable).
var roleOrder = []string{"core", "spine", "leaf", "distribution", "access"}

// ClassifyRoles infers and stores device_role / role_confidence / role_source for
// every device in the tenant. Rows with role_overridden=TRUE are skipped. Returns
// the number of rows whose role actually changed.
func (db *DB) ClassifyRoles(ctx context.Context, orgID, netID, grpID string) (int, error) {
	r := activeRoleRules

	// ── 1. Load devices ───────────────────────────────────────────────────────
	type dev struct {
		id, hostname, dtype, model, descr string
	}
	devRows, err := db.pool.Query(ctx, `
		SELECT id, hostname, device_type,
		       COALESCE(model_name,''), COALESCE(sys_description,'')
		FROM devices
		WHERE org_id=$1 AND network_id=$2 AND group_id=$3`,
		orgID, netID, grpID)
	if err != nil {
		return 0, fmt.Errorf("classify: load devices: %w", err)
	}
	devs := make([]dev, 0, 256)
	typeByID := map[string]string{}
	for devRows.Next() {
		var d dev
		if err := devRows.Scan(&d.id, &d.hostname, &d.dtype, &d.model, &d.descr); err != nil {
			devRows.Close()
			return 0, fmt.Errorf("classify: scan device: %w", err)
		}
		devs = append(devs, d)
		typeByID[d.id] = d.dtype
	}
	devRows.Close()
	if err := devRows.Err(); err != nil {
		return 0, err
	}
	if len(devs) == 0 {
		return 0, nil
	}

	// ── 2. Build undirected adjacency from topology_links ─────────────────────
	// Production fabric only — role inference (core/spine/leaf) is a data-plane
	// concept. management/power links would inflate degree and misclassify roles.
	adj := map[string]map[string]struct{}{}
	addEdge := func(a, b string) {
		if a == "" || b == "" || a == b {
			return
		}
		if adj[a] == nil {
			adj[a] = map[string]struct{}{}
		}
		adj[a][b] = struct{}{}
	}
	edgeRows, err := db.pool.Query(ctx, `
		SELECT tl.src_device_id, tl.dst_device_id
		FROM topology_links tl
		JOIN devices sd ON sd.id = tl.src_device_id
		WHERE sd.org_id=$1 AND sd.network_id=$2 AND sd.group_id=$3
		  AND tl.layer = 'network'`,
		orgID, netID, grpID)
	if err != nil {
		return 0, fmt.Errorf("classify: load edges: %w", err)
	}
	for edgeRows.Next() {
		var s, d string
		if err := edgeRows.Scan(&s, &d); err != nil {
			edgeRows.Close()
			return 0, fmt.Errorf("classify: scan edge: %w", err)
		}
		addEdge(s, d)
		addEdge(d, s)
	}
	edgeRows.Close()
	if err := edgeRows.Err(); err != nil {
		return 0, err
	}

	// ── 3. Score every device ─────────────────────────────────────────────────
	ids := make([]string, 0, len(devs))
	roles := make([]string, 0, len(devs))
	confs := make([]float64, 0, len(devs))
	srcs := make([]string, 0, len(devs))
	for _, d := range devs {
		role, isSwitch := typeToRole(d.dtype)
		var res roleResult
		if isSwitch {
			nbTypes := map[string]int{}
			total := 0
			for nb := range adj[d.id] {
				nbTypes[typeByID[nb]]++
				total++
			}
			res = classifySwitch(d.hostname, d.model, d.descr, nbTypes, total, r)
		} else {
			res = roleResult{role: role, confidence: 1.0, source: "inferred"}
		}
		ids = append(ids, d.id)
		roles = append(roles, res.role)
		confs = append(confs, res.confidence)
		srcs = append(srcs, res.source)
	}

	// ── 4. Persist (skip overridden rows; only write real changes to stay quiet) ──
	tag, err := db.pool.Exec(ctx, `
		UPDATE devices d SET
			device_role     = v.role,
			role_confidence = v.conf,
			role_source     = v.src,
			updated_at      = now()
		FROM unnest($1::uuid[], $2::text[], $3::real[], $4::text[]) AS v(id, role, conf, src)
		WHERE d.id = v.id
		  AND d.role_overridden = FALSE
		  AND (d.device_role IS DISTINCT FROM v.role
		       OR d.role_source IS DISTINCT FROM v.src)`,
		ids, roles, confs, srcs)
	if err != nil {
		return 0, fmt.Errorf("classify: persist: %w", err)
	}
	return int(tag.RowsAffected()), nil
}
