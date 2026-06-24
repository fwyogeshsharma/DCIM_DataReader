package target

import "strings"

// countryCode maps a device's country to a short stable code used in the network
// id. Add new countries here — the value MUST be stable per country (it is part
// of the DCS device identity key), so never derive it from anything that changes
// per poll.
func countryCode(country string) string {
	switch strings.ToUpper(strings.TrimSpace(country)) {
	case "USA", "US", "UNITED STATES", "UNITED STATES OF AMERICA":
		return "usa"
	case "INDIA", "IN":
		return "in"
	default:
		return ""
	}
}

// cityTitle normalizes a datacenter city to Title case with single-dash word
// separators (e.g. "new york" → "New-York", "Chicago" → "Chicago").
func cityTitle(city string) string {
	var b strings.Builder
	atWordStart := true
	prevDash := false
	for _, r := range strings.ToLower(strings.TrimSpace(city)) {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			if atWordStart && r >= 'a' && r <= 'z' {
				b.WriteRune(r - ('a' - 'A'))
			} else {
				b.WriteRune(r)
			}
			atWordStart = false
			prevDash = false
		default:
			if !prevDash && b.Len() > 0 {
				b.WriteByte('-')
				prevDash = true
			}
			atWordStart = true
		}
	}
	return strings.TrimRight(b.String(), "-")
}

// NetworkIDForLocation builds a device's logical network id from its datacenter,
// country and city, so each datacenter is its own network —
// <DATACENTER>-<COUNTRY>-<City> (e.g. DC1-USA-Chicago): datacenter uppercased,
// country as its uppercased short code, city Title-cased. Falls back to
// <DATACENTER>-<COUNTRY> when the city is unknown, and to the caller's configured
// global id (identity.network_id) when the datacenter or country is unknown/empty.
func NetworkIDForLocation(datacenter, country, city, fallback string) string {
	dc := strings.ToUpper(strings.TrimSpace(datacenter))
	cc := strings.ToUpper(countryCode(country))
	if dc == "" || cc == "" {
		return fallback
	}
	if ct := cityTitle(city); ct != "" {
		return dc + "-" + cc + "-" + ct
	}
	return dc + "-" + cc
}

// NetworkID returns this target's network id derived from its datacenter +
// country + city, falling back to the given global id when the datacenter or
// country is empty/unknown.
func (t *Target) NetworkID(fallback string) string {
	return NetworkIDForLocation(t.DatacenterName, t.Country, t.DatacenterCity, fallback)
}
