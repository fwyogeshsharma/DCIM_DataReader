# Metrics Reference — per device type

**Scope:** every `metric_name` EDR emits to DCS (and on to the Aggregator), broken
down by device type, with meaning, unit, protocol, tag, and source OID/path.

This is the authoritative list of what lands in the `metrics` table's `metric_name`
column. Compiled from the EDR collectors:
`internal/snmp/metrics.go`, `internal/gnmi/client.go`, `internal/redfish/manager.go`,
`internal/bacnet/objects.go`, `internal/bacnet/plant_objects.go` — cross-checked
against the simulator docs (`Datacenter_Network_Simulator/docs/*_ARCHITECTURE.md`).

---

## Contents

1. [Conventions](#1-conventions)
2. [Protocol matrix — what EDR actually collects](#2-protocol-matrix--what-edr-actually-collects)
3. [Universal metrics (every SNMP-capable device)](#3-universal-metrics-every-snmp-capable-device)
4. [router / switch](#4-router--switch)
5. [server](#5-server)
6. [sensor](#6-sensor-vendor-mibs-raritan--vertiv--apc)
7. [ups](#7-ups)
8. [pdu / floor_pdu](#8-pdu--floor_pdu)
9. [generator](#9-generator)
10. [energy_monitor](#10-energy_monitor-ev2--bacnet-kindenergy)
11. [chiller](#11-chiller-bacnet-kindenergy-scopecooling)
12. [pump](#12-pump-bacnet-scopecooling)
13. [cooling_tower](#13-cooling_tower-bacnet-scopecooling)
14. [valve](#14-valve-bacnet-scopecooling)
15. [cdu](#15-cdu-bacnet-scopecooling)
16. [crah](#16-crah-bacnet-scopecooling)
17. [rpp](#17-rpp)
18. [Source files](#18-source-files)

---

## 1. Conventions

- **metric_name** — the dotted name; prefix = domain (`system.`, `interface.`,
  `server.`, `environment.`, `pdu.`, `generator.`, `bmc.`, `energy.`, `cooling.`).
- **tag** — sub-key for repeated series under one metric_name (interface name,
  phase, CPU core, sensor name, circuit). Empty when scalar.
- **kind** — packet routing label: `metric` (default), `interface_address`,
  `topology`, `device_state`, `energy`, `alarm`/`event`.
- **unit** — physical unit after EDR un-scales any simulator ×10/×100 encoding.

### Identify a series correctly
The same `metric_name` (e.g. `environment.temperature_c`) is emitted by several
device types. Key any per-metric logic on **(device_type, metric_name)** or
**(metric_name, tag)** — never `metric_name` alone.

---

## 2. Protocol matrix — what EDR actually collects

| Device type | SNMP | gNMI | Redfish | BACnet |
|---|---|---|---|---|
| router | ✔ (mgmt) | ✔ (mgmt) | — | — |
| switch | ✔ (mgmt) | ✔ (mgmt) | — | — |
| firewall | ✔ (mgmt) | — | — | — |
| load_balancer | ✔ (mgmt) | — | — | — |
| oob_switch | ✔ (mgmt) | — | — | — |
| server | ✔ (OS prod IP + BMC mgmt IP) | — | ✔ (mgmt) | — |
| sensor | ✔ (mgmt) | — | — | — |
| ups | ✔ (mgmt) | — | — | — |
| pdu / floor_pdu | ✔ (mgmt) | — | — | — |
| generator | ✔ (mgmt) | — | — | — |
| crah | — | — | — | ✔ |
| cdu | — | — | — | ✔ |
| chiller | — | — | — | ✔ |
| pump | — | — | — | ✔ |
| cooling_tower | — | — | — | ✔ |
| valve | — | — | — | ✔ |
| energy_monitor (EV2) | — | — | — | ✔ |
| rpp | — | — | — | — |

**Gaps to know:**
- **sFlow is not collected.** The simulator exports sFlow for routers/switches/
  firewalls/LBs, but EDR has no sFlow collector — no flow metrics reach the DB.
- **CRAH/CDU:** the simulator also exposes an SNMP enterprise tree (`.24`/`.25`),
  but EDR collects them via **BACnet** `cooling.*`, not SNMP.
- **rpp:** passive breaker panel, no agent — appears only in the topology/power
  graph, emits no metrics.

---

## 3. Universal metrics (every SNMP-capable device)

Emitted for all device types that answer SNMP (router, switch, firewall,
load_balancer, oob_switch, server, sensor, ups, pdu, floor_pdu, generator).

| metric_name | meaning | unit | tag | source OID |
|---|---|---|---|---|
| `system.uptime_centiseconds` | system uptime | centiseconds | — | 1.3.6.1.2.1.1.3.0 |
| `interface.admin_status` | admin status (1=up) | enum | ifname | 1.3.6.1.2.1.2.2.1.7 |
| `interface.operational_status` | oper status (1=up/2=down) | enum | ifname | 1.3.6.1.2.1.2.2.1.8 |
| `interface.speed_mbps` | link speed | Mbps | ifname | 1.3.6.1.2.1.31.1.1.1.15 |
| `interface.bytes_received` | RX octets (32-bit) | bytes | ifname | 1.3.6.1.2.1.2.2.1.10 |
| `interface.bytes_sent` | TX octets (32-bit) | bytes | ifname | 1.3.6.1.2.1.2.2.1.16 |
| `interface.bytes_received_hc` | RX octets (64-bit) | bytes | ifname | 1.3.6.1.2.1.31.1.1.1.6 |
| `interface.bytes_sent_hc` | TX octets (64-bit) | bytes | ifname | 1.3.6.1.2.1.31.1.1.1.10 |
| `interface.packets_received_unicast` | RX unicast packets | packets | ifname | 1.3.6.1.2.1.2.2.1.11 |
| `interface.packets_sent_unicast` | TX unicast packets | packets | ifname | 1.3.6.1.2.1.2.2.1.17 |
| `interface.discards_received` | RX discards | packets | ifname | 1.3.6.1.2.1.2.2.1.13 |
| `interface.discards_sent` | TX discards | packets | ifname | 1.3.6.1.2.1.2.2.1.19 |
| `interface.errors_received` | RX errors | packets | ifname | 1.3.6.1.2.1.2.2.1.14 |
| `interface.errors_sent` | TX errors | packets | ifname | 1.3.6.1.2.1.2.2.1.20 |
| `interface.address` | interface IP (kind=interface_address) | — | IP | 1.3.6.1.2.1.4.20.1.1 |

---

## 4. router / switch

Universal **+** topology **+** gNMI (steady-state live source).

**SNMP/topology extra**

| metric_name | meaning | unit | tag | source |
|---|---|---|---|---|
| `lldp.neighbor` | LLDP neighbor link (kind=topology) | — | local_port | LLDP-MIB walk |

**gNMI** (OpenConfig; routers/switches only). Interface + system names mirror the
SNMP set; gNMI-only additions:

| metric_name | meaning | unit | tag | gNMI path |
|---|---|---|---|---|
| `system.uptime_centiseconds` | uptime | centiseconds | — | /system/state |
| `system.memory_total_bytes` | total memory | bytes | — | /system/memory |
| `system.memory_used_bytes` | used memory | bytes | — | /system/memory |
| `system.memory_utilization_percent` | memory used | % | — | derived |
| `system.cpu_utilization_percent` | per-CPU load | % | cpu_index | /components cpu |
| `environment.temperature_c` | component temp | °C | component | /components |
| `environment.temperature_alarm` | temp alarm (1=alarm) | enum | component | /components |
| `interface.operational_status` | oper status | enum | ifname | /interfaces |
| `interface.admin_status` | admin status | enum | ifname | /interfaces |
| `interface.bytes_received_hc` / `interface.bytes_sent_hc` | RX/TX octets | bytes | ifname | /interfaces |
| `interface.packets_received_unicast` / `interface.packets_sent_unicast` | RX/TX unicast | packets | ifname | /interfaces |
| `interface.errors_received` / `interface.errors_sent` | RX/TX errors | packets | ifname | /interfaces |
| `interface.discards_received` / `interface.discards_sent` | RX/TX discards | packets | ifname | /interfaces |

> firewall / load_balancer / oob_switch = **universal + `lldp.neighbor` only**
> (SNMP only — no gNMI).

---

## 5. server

Universal **+** OS-SNMP **+** Redfish (BMC). Redfish is the primary health source.

**OS via SNMP (HOST-RESOURCES / UCD / ENTITY-SENSOR)**

| metric_name | meaning | unit | tag | source OID |
|---|---|---|---|---|
| `server.cpu_per_core_percent` | per-core CPU load | % | core | 1.3.6.1.2.1.25.3.3.1.2 |
| `server.cpu_user_percent` | CPU user | % | — | 1.3.6.1.4.1.2021.11.9.0 |
| `server.cpu_system_percent` | CPU system | % | — | 1.3.6.1.4.1.2021.11.10.0 |
| `server.cpu_idle_percent` | CPU idle | % | — | 1.3.6.1.4.1.2021.11.11.0 |
| `server.memory_total_kb` | total memory | KB | — | 1.3.6.1.4.1.2021.4.5.0 |
| `server.memory_available_kb` | available memory | KB | — | 1.3.6.1.4.1.2021.4.6.0 |
| `server.memory_cached_kb` | cached memory | KB | — | 1.3.6.1.4.1.2021.4.11.0 |
| `server.memory_buffer_kb` | buffer memory | KB | — | 1.3.6.1.4.1.2021.4.12.0 |
| `server.storage_size_kb` | partition size | KB | mount | 1.3.6.1.2.1.25.2.3.1.5 |
| `server.storage_used_kb` | partition used | KB | mount | 1.3.6.1.2.1.25.2.3.1.6 |
| `server.storage_available_kb` | partition free (derived) | KB | mount | derived |
| `environment.temperature_c` | chassis/CPU temp | °C | CHASSIS / CPU | 1.3.6.1.2.1.99.1.1.1.4 |

**via Redfish (BMC, HTTP)**

| metric_name | meaning | unit | tag | kind |
|---|---|---|---|---|
| `power_state` | chassis power (1=On/2=Off) | enum | — | device_state |
| `server.cpu_percent` | CPU utilization | % | — | metric |
| `server.memory_used_percent` | memory utilization | % | — | metric |
| `server.memory_used_bytes` | memory used | bytes | — | metric |
| `server.disk_used_percent` | disk utilization | % | — | metric |
| `server.disk_used_bytes` | disk used | bytes | — | metric |
| `server.disk_total_bytes` | disk total | bytes | — | metric |
| `server.network_rx_mbps` | network RX | Mbps | — | metric |
| `server.network_tx_mbps` | network TX | Mbps | — | metric |
| `server.alarm_count` | active alarms | count | — | metric |
| `bmc.temp_c` | thermal sensor | °C | sensor | metric |
| `bmc.fan_rpm` | fan speed | RPM | fan | metric |
| `bmc.power_draw_w` | total power draw | W | — | metric |
| `bmc.psu_output_w` | PSU output | W | psu | metric |
| `bmc.info` | BMC fw/model/vendor (in attributes) | — | — | metric |

---

## 6. sensor (vendor MIBs: Raritan / Vertiv / APC)

| metric_name | meaning | unit | tag | source OID |
|---|---|---|---|---|
| `environment.temperature_c` | temperature | °C | sensor | Raritan 13742.6.5.5.3.1.4 / Vertiv 21239.5.1.4.1.4 / APC 318.1.1.10.4.2.2.1.10 |
| `environment.humidity_percent` | relative humidity | % | sensor | Raritan/Vertiv/APC humidity row |
| `environment.dew_point_c` | dew point (Vertiv only) | °C | sensor | 21239.5.1.6.1.4 |
| `environment.airflow_cfm` | airflow (APC only) | CFM | sensor | 318.1.1.10.4.2.2.1.10 |

Vendor gating: Raritan → Vertiv → APC. Values un-scaled ×10 (except APC humidity).

---

## 7. ups

Universal **+**:

| metric_name | meaning | unit | tag | source OID |
|---|---|---|---|---|
| `environment.ups_battery_status` | battery status code | enum | — | 1.3.6.1.2.1.33.1.2.3.0 |
| `environment.ups_seconds_on_battery` | seconds on battery | s | — | 1.3.6.1.2.1.33.1.2.2.0 |
| `environment.ups_minutes_remaining` | runtime remaining | min | — | 1.3.6.1.2.1.33.1.2.4.0 |
| `environment.ups_charge_percent` | battery charge | % | — | 1.3.6.1.2.1.33.1.2.5.0 |
| `environment.ups_battery_voltage_v` | battery voltage | V | — | 1.3.6.1.2.1.33.1.2.6.0 |
| `environment.ups_input_voltage_v` | input voltage | V | phase | 1.3.6.1.2.1.33.1.3.3.1.3 |
| `environment.ups_output_voltage_v` | output voltage | V | phase | 1.3.6.1.2.1.33.1.4.4.1.2 |
| `environment.ups_output_current_a` | output current | A | phase | 1.3.6.1.2.1.33.1.4.4.1.3 |
| `environment.ups_output_load_percent` | output load | % | phase | 1.3.6.1.2.1.33.1.4.4.1.6 |
| `environment.temperature_c` | battery temperature | °C | BATTERY | 1.3.6.1.2.1.33.1.2.8.0 |
| `environment.ups_fan_status` | fan status | enum | — | 1.3.6.1.4.1.99999.4.1.0 |
| `environment.ups_charger_status` | charger status | enum | — | 1.3.6.1.4.1.99999.4.2.0 |
| `environment.ups_rectifier_status` | rectifier status | enum | — | 1.3.6.1.4.1.99999.4.3.0 |
| `environment.ups_phase_status` | phase status | enum | — | 1.3.6.1.4.1.99999.4.4.0 |
| `environment.ups_battery_status_ex` | extended battery status | enum | — | 1.3.6.1.4.1.99999.4.5.0 |

---

## 8. pdu / floor_pdu

Universal **+** (enterprise `99999.5.N.0`):

| metric_name | meaning | unit | tag | col |
|---|---|---|---|---|
| `pdu.load_percent` | load | % | — | 1 |
| `pdu.voltage_v` | phase voltage | V | — | 2 |
| `pdu.power_factor` | power factor | — | — | 3 |
| `pdu.phase_imbalance_percent` | phase imbalance | % | — | 4 |
| `pdu.outlet_status` | outlet status | enum | — | 5 |
| `pdu.breaker_status` | breaker status | enum | — | 6 |
| `pdu.outlet_failure` | outlet failure | enum | — | 7 |
| `pdu.smoke_detected` | smoke detected | enum | — | 8 |
| `pdu.current_a` | current | A | — | 9 |
| `pdu.ground_fault` | ground fault | enum | — | 10 |
| `pdu.real_power_w` | real power | W | — | 11 |
| `pdu.apparent_power_va` | apparent power | VA | — | 12 |
| `pdu.energy_kwh` | energy | kWh | — | 13 |
| `pdu.frequency_hz` | frequency | Hz | — | 14 |
| `environment.temperature_c` | ambient temp | °C | PDU | 15 |
| `environment.humidity_percent` | ambient humidity | % | — | 16 |
| `pdu.outlet_power_w` | per-outlet power | W | — | 17 |

---

## 9. generator

Universal **+** (enterprise `99999.7.N.0`):

| metric_name | meaning | unit | tag | col |
|---|---|---|---|---|
| `generator.fuel_percent` | fuel level | % | — | 1 |
| `generator.run_hours` | run hours | h | — | 2 |
| `generator.status` | status (standby/run/fault) | enum | — | 3 |
| `generator.load_percent` | load | % | — | 4 |
| `generator.output_kw` | output power | kW | — | 5 |
| `generator.output_voltage_v` | phase voltage | V | PhA / PhB / PhC | 6–8 |
| `generator.frequency_hz` | frequency | Hz | — | 9 |
| `generator.coolant_status` | coolant status | enum | — | 10 |
| `generator.oil_pressure_status` | oil pressure status | enum | — | 11 |
| `generator.battery_status` | battery status | enum | — | 12 |
| `generator.start_attempts` | start attempts | count | — | 13 |

---

## 10. energy_monitor (EV2 — BACnet, kind=energy)

**Panel**

| metric_name | meaning | unit | tag |
|---|---|---|---|
| `energy.active_power_kw` | active power | kW | — |
| `energy.energy_kwh` | energy | kWh | — |
| `energy.voltage_v` | phase voltage | V | PhA / PhB / PhC |
| `energy.current_a` | phase current | A | PhA / PhB / PhC |
| `energy.frequency_hz` | frequency | Hz | — |
| `energy.power_factor` | power factor | — | — |
| `energy.voltage_thd_percent` | voltage THD | % | — |
| `energy.current_thd_percent` | current THD | % | — |
| `energy.harmonic_current_percent` | harmonic current | % | H3 / H5 / H7 / H9 |
| `energy.alarm_overcurrent` | overcurrent alarm | enum | — |
| `energy.alarm_voltage_imbalance` | voltage imbalance alarm | enum | — |
| `energy.alarm_high_thd` | high THD alarm | enum | — |
| `energy.alarm_phase_loss` | phase loss alarm | enum | — |
| `energy.alarm_sensor_fault` | sensor fault alarm | enum | — |

**Per circuit** (tag = `CktNN`, NN = 01..42)

| metric_name | meaning | unit |
|---|---|---|
| `energy.circuit_current_a` | circuit current | A |
| `energy.circuit_power_kw` | circuit power | kW |
| `energy.circuit_energy_kwh` | circuit energy | kWh |
| `energy.circuit_power_factor` | circuit power factor | — |
| `energy.circuit_current_thd_percent` | circuit current THD | % |

---

## 11. chiller (BACnet, kind=energy, scope=cooling)

| metric_name | meaning | unit |
|---|---|---|
| `cooling.chw_supply_temp_c` | chilled water supply temp | °C |
| `cooling.chw_return_temp_c` | chilled water return temp | °C |
| `cooling.chw_setpoint_c` | CHW setpoint | °C |
| `cooling.chw_flow_lps` | CHW flow | L/s |
| `cooling.cond_supply_temp_c` | condenser supply temp | °C |
| `cooling.cond_return_temp_c` | condenser return temp | °C |
| `cooling.compressor_load_pct` | compressor load | % |
| `cooling.active_power_kw` | power | kW |
| `cooling.cooling_capacity_kw` | cooling capacity | kW |
| `cooling.cop` | coefficient of performance | — |
| `cooling.evap_pressure_kpa` | evaporator pressure | kPa |
| `cooling.cond_pressure_kpa` | condenser pressure | kPa |
| `cooling.run_hours_h` | run hours | h |
| `cooling.chiller_running` | running status | enum |
| `cooling.alarm_high_pressure` | high pressure alarm | enum |
| `cooling.alarm_low_evap_temp` | low evap temp alarm | enum |
| `cooling.alarm_flow_loss` | flow loss alarm | enum |

---

## 12. pump (BACnet, scope=cooling)

| metric_name | meaning | unit |
|---|---|---|
| `cooling.pump_speed_pct` | pump speed | % |
| `cooling.pump_flow_lps` | pump flow | L/s |
| `cooling.discharge_pressure_kpa` | discharge pressure | kPa |
| `cooling.suction_pressure_kpa` | suction pressure | kPa |
| `cooling.diff_pressure_kpa` | differential pressure | kPa |
| `cooling.motor_power_kw` | motor power | kW |
| `cooling.motor_temp_c` | motor temperature | °C |
| `cooling.vfd_frequency_hz` | VFD frequency | Hz |
| `cooling.run_hours_h` | run hours | h |
| `cooling.pump_running` | running status | enum |
| `cooling.alarm_fault` | fault alarm | enum |
| `cooling.alarm_low_flow` | low flow alarm | enum |

---

## 13. cooling_tower (BACnet, scope=cooling)

| metric_name | meaning | unit |
|---|---|---|
| `cooling.fan_speed_pct` | fan speed | % |
| `cooling.basin_temp_c` | basin temperature | °C |
| `cooling.cond_water_in_c` | condenser water in | °C |
| `cooling.cond_water_out_c` | condenser water out | °C |
| `cooling.fan_power_kw` | fan power | kW |
| `cooling.basin_level_pct` | basin level | % |
| `cooling.makeup_flow_lpm` | makeup flow | L/min |
| `cooling.vibration_mms` | vibration | mm/s |
| `cooling.run_hours_h` | run hours | h |
| `cooling.fan_running` | running status | enum |
| `cooling.alarm_high_vibration` | high vibration alarm | enum |
| `cooling.alarm_low_basin` | low basin level alarm | enum |

---

## 14. valve (BACnet, scope=cooling)

| metric_name | meaning | unit |
|---|---|---|
| `cooling.valve_position_pct` | valve position | % |
| `cooling.valve_commanded_position_pct` | commanded position | % |
| `cooling.actuator_temp_c` | actuator temperature | °C |
| `cooling.valve_modulating` | modulating status | enum |
| `cooling.alarm_actuator_fault` | actuator fault alarm | enum |

---

## 15. cdu (BACnet, scope=cooling)

| metric_name | meaning | unit |
|---|---|---|
| `cooling.tcs_supply_temp_c` | TCS supply temp | °C |
| `cooling.tcs_return_temp_c` | TCS return temp | °C |
| `cooling.tcs_setpoint_c` | TCS setpoint | °C |
| `cooling.tcs_flow_lps` | TCS flow | L/s |
| `cooling.facility_chw_valve_pct` | facility CHW valve | % |
| `cooling.facility_chw_flow_lps` | facility CHW flow | L/s |
| `cooling.tcs_loop_pressure_kpa` | TCS loop pressure | kPa |
| `cooling.heat_load_kw` | heat load | kW |
| `cooling.pump_power_kw` | pump power | kW |
| `cooling.pump_speed_pct` | pump speed | % |
| `cooling.approach_temp_c` | approach temperature | °C |
| `cooling.filter_dp_kpa` | filter differential pressure | kPa |
| `cooling.run_hours_h` | run hours | h |
| `cooling.cdu_running` | running status | enum |
| `cooling.alarm_leak` | leak alarm | enum |
| `cooling.alarm_high_supply_temp` | high supply temp alarm | enum |
| `cooling.alarm_pump_fault` | pump fault alarm | enum |
| `cooling.alarm_low_flow` | low flow alarm | enum |

---

## 16. crah (BACnet, scope=cooling)

| metric_name | meaning | unit |
|---|---|---|
| `cooling.supply_air_temp_c` | supply air temp | °C |
| `cooling.return_air_temp_c` | return air temp | °C |
| `cooling.crah_setpoint_c` | CRAH setpoint | °C |
| `cooling.fan_speed_pct` | fan speed | % |
| `cooling.chw_valve_pct` | CHW valve | % |
| `cooling.cooling_capacity_pct` | cooling capacity | % |
| `cooling.supply_humidity_pct` | supply humidity | % |
| `cooling.airflow_pct` | airflow | % |
| `cooling.fan_power_kw` | fan power | kW |
| `cooling.run_hours_h` | run hours | h |
| `cooling.crah_running` | running status | enum |
| `cooling.alarm_high_temp` | high temperature alarm | enum |
| `cooling.alarm_airflow_loss` | airflow loss alarm | enum |
| `cooling.alarm_filter_dirty` | filter dirty alarm | enum |

---

## 17. rpp

No telemetry — passive breaker panel, no agent. Appears only in the topology /
power graph.

---

## 18. Source files

| Protocol | Collector file |
|---|---|
| SNMP | `fwEDR/internal/snmp/metrics.go`, `internal/snmp/mibs.go` |
| gNMI | `fwEDR/internal/gnmi/client.go` |
| Redfish | `fwEDR/internal/redfish/manager.go` |
| BACnet (EV2) | `fwEDR/internal/bacnet/objects.go` |
| BACnet (plant) | `fwEDR/internal/bacnet/plant_objects.go` |
| Simulator OID/point catalog | `Datacenter_Network_Simulator/docs/*_ARCHITECTURE.md` |
