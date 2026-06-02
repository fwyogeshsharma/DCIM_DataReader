package store

// hierarchy.go — dynamic, topology-agnostic parent-child computation.
//
// The network is stored as an adjacency graph in topology_links. This builds a
// spanning tree over that graph by BFS from a root, assigning each device a
// parent_device_id + topology_depth. It makes NO assumptions about device
// roles or hostnames, so it works for any graph shape (tree, star, ring, mesh).
// Redundant / cross edges that aren't part of the spanning tree are marked
// relation='peer'; tree edges get 'uplink'/'downlink'.

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// RecomputeHierarchy rebuilds the spanning tree for one tenant. rootHint is a
// hostname or any IP of the desired root; empty (or not found) → the highest-
// degree node is auto-selected per connected component. Returns the resolved
// root hostname and the number of devices whose parent was written.
func (db *DB) RecomputeHierarchy(ctx context.Context,
	orgID, netID, grpID, rootHint string) (string, int, error) {

	// ── 1. Load devices (id, hostname, IPs) for the tenant ────────────────────
	devRows, err := db.pool.Query(ctx, `
		SELECT id, hostname,
		       COALESCE(mgmt_ip::text,''), COALESCE(prod_ip::text,''),
		       COALESCE(loopback_ip::text,''), COALESCE(oob_ip::text,'')
		FROM devices
		WHERE org_id=$1 AND network_id=$2 AND group_id=$3`,
		orgID, netID, grpID)
	if err != nil {
		return "", 0, fmt.Errorf("hierarchy: load devices: %w", err)
	}
	hostByID := map[string]string{}
	var rootID string // resolved from rootHint
	allIDs := []string{}
	hint := strings.TrimSpace(rootHint)
	for devRows.Next() {
		var id, host, mgmt, prod, lo, oob string
		if err := devRows.Scan(&id, &host, &mgmt, &prod, &lo, &oob); err != nil {
			devRows.Close()
			return "", 0, fmt.Errorf("hierarchy: scan device: %w", err)
		}
		hostByID[id] = host
		allIDs = append(allIDs, id)
		if rootID == "" && hint != "" {
			if host == hint || ipMatch(mgmt, hint) || ipMatch(prod, hint) ||
				ipMatch(lo, hint) || ipMatch(oob, hint) {
				rootID = id
			}
		}
	}
	devRows.Close()
	if err := devRows.Err(); err != nil {
		return "", 0, err
	}
	if len(allIDs) == 0 {
		return "", 0, nil
	}
	sort.Strings(allIDs) // deterministic iteration

	// ── 2. Load edges → undirected adjacency ──────────────────────────────────
	// Production fabric only. management (device↔OOB) and power (PDU/UPS chain)
	// links also live in topology_links now; including them would make an OOB
	// switch or floor PDU a graph super-hub and wreck core/spine/leaf inference
	// and parent/child relations. The spanning tree is a production concept.
	edgeRows, err := db.pool.Query(ctx, `
		SELECT tl.src_device_id, tl.dst_device_id
		FROM topology_links tl
		JOIN devices sd ON sd.id = tl.src_device_id
		WHERE sd.org_id=$1 AND sd.network_id=$2 AND sd.group_id=$3
		  AND tl.layer = 'network'`,
		orgID, netID, grpID)
	if err != nil {
		return "", 0, fmt.Errorf("hierarchy: load edges: %w", err)
	}
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
	for edgeRows.Next() {
		var s, d string
		if err := edgeRows.Scan(&s, &d); err != nil {
			edgeRows.Close()
			return "", 0, fmt.Errorf("hierarchy: scan edge: %w", err)
		}
		addEdge(s, d)
		addEdge(d, s)
	}
	edgeRows.Close()
	if err := edgeRows.Err(); err != nil {
		return "", 0, err
	}

	// Sorted neighbour lists → deterministic BFS parent for mesh nodes.
	neighbours := func(id string) []string {
		ns := make([]string, 0, len(adj[id]))
		for n := range adj[id] {
			ns = append(ns, n)
		}
		sort.Strings(ns)
		return ns
	}
	degree := func(id string) int { return len(adj[id]) }

	// ── 3. BFS spanning tree (multi-component) ────────────────────────────────
	parent := make(map[string]string, len(allIDs))
	visited := make(map[string]bool, len(allIDs))

	bfs := func(root string) {
		visited[root] = true
		parent[root] = ""
		queue := []string{root}
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			for _, n := range neighbours(cur) {
				if visited[n] {
					continue
				}
				visited[n] = true
				parent[n] = cur
				queue = append(queue, n)
			}
		}
	}

	// Primary root from config hint, else highest-degree node overall.
	if rootID == "" {
		rootID = highestDegreeUnvisited(allIDs, visited, degree)
	}
	if rootID != "" {
		bfs(rootID)
	}
	// Remaining components (disconnected graphs / isolated nodes): each gets its
	// own root = highest-degree unvisited node.
	for {
		next := highestDegreeUnvisited(allIDs, visited, degree)
		if next == "" {
			break
		}
		bfs(next)
	}

	// ── 4. Persist the tree into topology_links.relation (only table touched) ──
	// Tree edges (child↔parent) are flattened into two arrays; everything else
	// is reset to 'peer'. The parent of any device is then derivable purely from
	// topology_links — no devices column needed.
	children := make([]string, 0, len(parent))
	parents := make([]string, 0, len(parent))
	for _, id := range allIDs {
		if parent[id] != "" {
			children = append(children, id)
			parents = append(parents, parent[id])
		}
	}

	// Reset all tenant edges to peer.
	if _, err := db.pool.Exec(ctx, `
		UPDATE topology_links tl SET relation='peer', updated_at=now()
		FROM devices sd
		WHERE sd.id = tl.src_device_id
		  AND sd.org_id=$1 AND sd.network_id=$2 AND sd.group_id=$3
		  AND tl.relation <> 'peer'`,
		orgID, netID, grpID); err != nil {
		return "", 0, fmt.Errorf("hierarchy: reset relation: %w", err)
	}

	// Mark the spanning-tree edges. A row reported by the child (src=child,
	// dst=parent) is an 'uplink'; reported by the parent (src=parent, dst=child)
	// is a 'downlink'. Both describe the same physical parent→child edge.
	// The `relation <> ...` guards bump updated_at ONLY when the relation actually
	// changes. Without them, every recompute (every 30s) re-stamps the whole tree
	// → the forwarder re-pushes every link every cycle even on a stable topology.
	// With them, a settled tree produces zero updated rows → near-zero forwarder load.
	if len(children) > 0 {
		if _, err := db.pool.Exec(ctx, `
			UPDATE topology_links tl SET relation='uplink', updated_at=now()
			FROM unnest($1::uuid[], $2::uuid[]) AS v(child, parent)
			WHERE tl.src_device_id = v.child AND tl.dst_device_id = v.parent
			  AND tl.relation <> 'uplink'`,
			children, parents); err != nil {
			return "", 0, fmt.Errorf("hierarchy: mark uplink: %w", err)
		}
		if _, err := db.pool.Exec(ctx, `
			UPDATE topology_links tl SET relation='downlink', updated_at=now()
			FROM unnest($1::uuid[], $2::uuid[]) AS v(child, parent)
			WHERE tl.src_device_id = v.parent AND tl.dst_device_id = v.child
			  AND tl.relation <> 'downlink'`,
			children, parents); err != nil {
			return "", 0, fmt.Errorf("hierarchy: mark downlink: %w", err)
		}
	}

	return hostByID[rootID], len(allIDs), nil
}

// highestDegreeUnvisited returns the unvisited node with the most neighbours
// (ties broken by the pre-sorted id order for determinism). "" when all visited.
func highestDegreeUnvisited(ids []string, visited map[string]bool, degree func(string) int) string {
	best := ""
	bestDeg := -1
	for _, id := range ids {
		if visited[id] {
			continue
		}
		if d := degree(id); d > bestDeg {
			best, bestDeg = id, d
		}
	}
	return best
}

// ipMatch compares a stored INET text (which may carry a /mask) against a hint.
func ipMatch(stored, hint string) bool {
	if stored == "" {
		return false
	}
	if i := strings.IndexByte(stored, '/'); i >= 0 {
		stored = stored[:i]
	}
	return stored == hint
}
