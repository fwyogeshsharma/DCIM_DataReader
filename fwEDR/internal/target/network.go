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

// citySlug normalizes a datacenter city into a url-ish token: lowercased, runs of
// non-alphanumerics collapsed to a single dash, trimmed (e.g. "New York" →
// "new-york").
func citySlug(city string) string {
	var b strings.Builder
	prevDash := false
	for _, r := range strings.ToLower(strings.TrimSpace(city)) {
		switch {
		case (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9'):
			b.WriteRune(r)
			prevDash = false
		default:
			if !prevDash && b.Len() > 0 {
				b.WriteByte('-')
				prevDash = true
			}
		}
	}
	return strings.TrimRight(b.String(), "-")
}

// NetworkIDForLocation builds a device's logical network id from its country and
// datacenter city, so each datacenter is its own network — net-<country>-<city>
// (e.g. net-usa-dallas). Falls back to net-<country> when the city is unknown,
// and to the caller's configured global id (identity.network_id) when the country
// itself is unknown/empty.
func NetworkIDForLocation(country, city, fallback string) string {
	cc := countryCode(country)
	if cc == "" {
		return fallback
	}
	if cs := citySlug(city); cs != "" {
		return "net-" + cc + "-" + cs
	}
	return "net-" + cc
}

// NetworkID returns this target's network id derived from its country + datacenter
// city, falling back to the given global id when the country is empty/unknown.
func (t *Target) NetworkID(fallback string) string {
	return NetworkIDForLocation(t.Country, t.DatacenterCity, fallback)
}
