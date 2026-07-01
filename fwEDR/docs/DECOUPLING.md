# Simulator Decoupling — EDR / DCS

## Goal

The same EDR and DCS binaries must run against **real datacenter hardware** and
against the **Datacenter Network Simulator**, with no code changes between them.
Historically EDR hardcoded simulator-specific addressing — placeholder enterprise
OID trees, nonstandard MIB placements, the simulator's Redfish `Oem` namespace,
positional BACnet instance numbers. This document describes how those couplings
were externalized so switching to real hardware is a **configuration** change.

## Principle: standardize the pipeline, not the protocol

Every collector still speaks its own wire protocol (SNMP / gNMI / Redfish /
BACnet) — that cannot be unified. What was unified is **where each reading is
mapped from**. Each protocol's simulator-specific mappings now live in a
**profile** (or config knob), loaded at startup. With no profile configured, the
built-in **`DefaultProfile()`** reproduces the exact previous simulator values, so
default behavior is **byte-identical**. Point a profile at real hardware and the
same binary polls real devices.

```
  profile / config  ──▶  collector.Collect()  ──▶  TelemetryPacket  ──▶  DCS
  (what to read)          (how to read it)         (normalized, protocol-agnostic)
```

Nothing about scheduling or the downstream pipeline (queue → publisher → DCS →
tables) changed. The decoupling touches only the decode/map step.

## What is decoupled, per protocol

| Protocol | Simulator-specific coupling | Now driven by | Config key |
|---|---|---|---|
| SNMP (enterprise) | PDU/generator/UPS `1.3.6.1.4.1.99999` trees | SNMP profile | `snmp.profile_path` |
| SNMP (server) | hrStorage `.25.2.2` placement, UCD CPU/mem columns | SNMP profile | `snmp.profile_path` |
| SNMP (sensors) | Raritan/Vertiv/APC OID tables + type codes | SNMP profile | `snmp.profile_path` |
| SNMP (write) | asset/threshold SET OIDs (`99999.3/.4`) | SNMP profile | `snmp.profile_path` |
| BACnet | positional plant object instance maps | BACnet profile | `bacnet.profile_path` |
| Redfish | `Oem.Simulator.*` OS-usage fields | Redfish profile | `redfish.profile_path` |
| gNMI | `state/uptime` unit (sim serves centiseconds) | config scale | `gnmi.uptime_scale` |
| DCS | role classifier naming; trap identity | already generic | `topology.role_rules_path` |

DCS required no code change: the role classifier is already overridable via
`topology.role_rules_path` (and its compiled defaults are generic core/spine/leaf
patterns + real vendor model names, not sim-specific), and trap identity is
resolved by matching the trap IP against the device's mgmt/prod/loopback/oob
columns — not the community=IP convention.

## Profile mechanism

Each protocol package (`internal/snmp`, `internal/bacnet`, `internal/redfish`)
provides:

- `DefaultProfile() *Profile` — the built-in **simulator** profile. Exact previous
  hardcoded values, guaranteeing unchanged behavior when no file is set.
- `LoadProfile(path string) (*Profile, error)` — parses a YAML file. An **empty
  path returns the default**. A file overrides only the sections/keys it defines;
  omitted sections inherit the default (partial profiles are valid). Map-style
  sections that are provided **replace** the whole default map (so a real device's
  field set never inherits stray simulator OIDs).
- A `*_test.go` that **pins** the default to the previous values (fails loudly on
  drift) and verifies empty==default plus override behavior.

If a profile file fails to load, the collector logs a warning and **falls back to
the default** — a bad config file never stops collection.

Example profiles live in [`fwEDR/profiles/`](../profiles):

- `example.snmp-profile.yaml`
- `example.bacnet-profile.yaml`
- `example.redfish-profile.yaml`

## Writing a real-vendor profile

### SNMP (`snmp.profile_path`)

Real PDUs/generators/UPSs answer under **vendor MIBs**, not the `99999` tree, and
real net-snmp agents use standard `hrStorageTable` `.25.2.3`. Map the enterprise
scalar columns, the server HR/UCD OIDs, the sensor tables, and the SET write map
to the target's real OIDs. Example:

```yaml
name: acme-fleet
enterprise:
  pdu_base: "1.3.6.1.4.1.318.1.1.12"      # real vendor PDU subtree
  pdu_scalars:
    - {col: "1", name: "pdu.load_percent", tag: "", scale: 1}
    # ... vendor's real columns ...
server:
  hr_storage_size: "1.3.6.1.2.1.25.2.3.1.5"   # standard hrStorage
  hr_storage_used: "1.3.6.1.2.1.25.2.3.1.6"
sensors:
  raritan_type_temp: 10
write:
  oids:
    name:     {oid: "1.3.6.1.2.1.1.5.0", is_int: false}
    rack_unit:{oid: "<vendor OID>",      is_int: true}
```

Every section is optional; unset sections keep the built-in default.

### BACnet (`bacnet.profile_path`)

Real chiller/CRAH/CDU firmware assigns arbitrary object instances. Discover the
device's instances (e.g. a BACnet browser reading `Object_Name`, property 77) and
map each point per device type. A provided device type replaces that type's whole
object list:

```yaml
name: acme-plant
plant:
  chiller:
    - {type: analogInput, inst: 1, name: cooling.chw_supply_temp_c, scale: 1}
    - {type: binaryInput, inst: 1, name: cooling.chiller_running}
```

### Redfish (`redfish.profile_path`)

Standard resources (PowerState, `/Thermal`, `/Power`, Managers) are already
portable — no override needed. Only the OS-usage fields are simulator-specific.
Map each `server.*` metric to a dotted path inside the ComputerSystem document.
Providing `os_usage` replaces the whole default list:

```yaml
name: acme-bmc
power_state_path: PowerState
os_usage:
  - {metric: server.cpu_percent, path: "Oem.Vendor.CpuUtilizationPercent"}
```

### gNMI (`gnmi.uptime_scale`)

The only unit coupling. The simulator serves `/system/state/uptime` in
centiseconds → `uptime_scale: 1.0` (default). A real target serving seconds → set
`100`; nanoseconds → `1e-7`.

### DCS (`topology.role_rules_path`)

Ship a YAML of role classification rules to override the compiled defaults for a
real fleet's naming/model conventions. Optional — the defaults are already
generic.

## Limitations (not fully decoupled)

- **Redfish MetricReports.** A dotted path only reaches fields *within* the
  ComputerSystem doc. Real BMCs that expose CPU/mem in a **separate MetricReport
  resource** (a second GET) need a richer collector — out of scope here.
- **BACnet positional maps.** The profile is still keyed by object **instance**,
  which is positional. True immunity to reordering needs **runtime `Object_Name`
  discovery** (property 77) — a follow-up (2b) requiring BACnet CharacterString
  decode in the codec plus a live test.

## Correctness fixes bundled with decoupling

Two changes are **not** byte-identical — they correct pre-existing defects and
should be verified on the VM after deploy:

1. **cdu BACnet mapping.** The default now maps `cooling.tcs_loop_pressure_kpa` to
   AI:7 (the object the simulator actually serves there), shifting heat_load /
   pump_power / pump_speed / approach_temp / filter_dp / run_hours to their real
   instances. Previously those six read the neighboring object's value.
2. **gNMI uptime.** Router/switch `system.uptime_centiseconds` is now the true
   value; the previous hardcoded ×100 made it 100× too large.

## Rollout

1. Deploy the branch with **empty profile paths** — behavior is byte-identical to
   the simulator baseline except the two fixes above. Confirm cdu metrics and
   router/switch uptime on the VM.
2. For real hardware, write per-protocol profiles (templates in
   `fwEDR/profiles/`) and set the `*.profile_path` keys. No rebuild.
3. Watch the `snmp poll metrics` log and DB freshness to confirm the real
   mappings resolve.

## Source

| Area | Files |
|---|---|
| SNMP profile | `internal/snmp/profile.go`, `profile_test.go` |
| BACnet profile | `internal/bacnet/profile.go`, `profile_test.go` |
| Redfish profile | `internal/redfish/profile.go`, `profile_test.go` |
| gNMI uptime | `internal/gnmi/client.go`, `pkg/config/config.go` |
| DCS classifier | `fwDCS/internal/store/classifier.go` (`LoadRoleRules`) |
| DCS trap identity | `fwDCS/internal/store/timescale.go` (`DeviceByIP`) |
| Config keys | `pkg/config/config.go` |
| Example profiles | `fwEDR/profiles/example.*.yaml` |
