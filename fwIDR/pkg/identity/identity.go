// Package identity builds and parses the 5-level resource path used to
// uniquely identify every device in the fwDCIM hierarchy.
package identity

import (
	"fmt"
	"strings"
)

// ResourcePath carries the five mandatory hierarchy levels plus the device's
// effective SNMP/gNMI address (mgmt_ip when present, ip_address otherwise).
type ResourcePath struct {
	OrgID        string
	DatacenterID string
	FloorID      string
	NetworkID    string
	GroupID      string
	SourceID     string // hostname | MAC | serial | mgmt_ip
}

// String returns the canonical slash-separated path used as a log/trace label.
func (r ResourcePath) String() string {
	return fmt.Sprintf("%s/%s/%s/%s/%s/%s",
		r.OrgID, r.DatacenterID, r.FloorID, r.NetworkID, r.GroupID, r.SourceID)
}

// Validate returns an error if any mandatory field is empty.
func (r ResourcePath) Validate() error {
	for field, val := range map[string]string{
		"org_id":        r.OrgID,
		"datacenter_id": r.DatacenterID,
		"floor_id":      r.FloorID,
		"network_id":    r.NetworkID,
		"group_id":      r.GroupID,
		"source_id":     r.SourceID,
	} {
		if strings.TrimSpace(val) == "" {
			return fmt.Errorf("identity: %s must not be empty", field)
		}
	}
	return nil
}
