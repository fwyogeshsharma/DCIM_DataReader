package bacnet

// plantTypes is the set of BACnet cooling-plant device types. This is device
// taxonomy (which types are plant gear), NOT a sim-specific mapping — the actual
// per-type object maps live in the BACnet Profile (see profile.go). Kept here so
// isPlantType works without a profile reference (e.g. the COV poll-only check).
var plantTypes = map[string]bool{
	"chiller":       true,
	"pump":          true,
	"cooling_tower": true,
	"valve":         true,
	"cdu":           true,
	"crah":          true,
}

// isPlantType reports whether a device_type is a chiller-plant BACnet device.
func isPlantType(dt string) bool {
	return plantTypes[dt]
}
