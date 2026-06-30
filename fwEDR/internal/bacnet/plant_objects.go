package bacnet

// plantObjects maps each chiller-plant BACnet device type to its objects,
// mirroring core/bacnet_plant_generator.py PLANT_SPEC in the simulator. Analog
// Inputs are instances 1..N and Binary Inputs 1..M, in spec order (object type
// disambiguates equal instance numbers). The name is the EDR/energy_metrics
// metric name: cooling.<point>_<unit> for analog, cooling.<status>/cooling.alarm_*
// for binary. Alarm BIs (name contains ".alarm_") additionally raise events.
//
// All plant readings are stored in energy_metrics with scope="cooling".
var plantObjects = map[string][]objMeta{
	"chiller": {
		{ObjAnalogInput, 1, "cooling.chw_supply_temp_c", "", 1},
		{ObjAnalogInput, 2, "cooling.chw_return_temp_c", "", 1},
		{ObjAnalogInput, 3, "cooling.chw_setpoint_c", "", 1},
		{ObjAnalogInput, 4, "cooling.chw_flow_lps", "", 1},
		{ObjAnalogInput, 5, "cooling.cond_supply_temp_c", "", 1},
		{ObjAnalogInput, 6, "cooling.cond_return_temp_c", "", 1},
		{ObjAnalogInput, 7, "cooling.compressor_load_pct", "", 1},
		{ObjAnalogInput, 8, "cooling.active_power_kw", "", 1},
		{ObjAnalogInput, 9, "cooling.cooling_capacity_kw", "", 1},
		{ObjAnalogInput, 10, "cooling.cop", "", 1},
		{ObjAnalogInput, 11, "cooling.evap_pressure_kpa", "", 1},
		{ObjAnalogInput, 12, "cooling.cond_pressure_kpa", "", 1},
		{ObjAnalogInput, 13, "cooling.run_hours_h", "", 1},
		{ObjBinaryInput, 1, "cooling.chiller_running", "", 1},
		{ObjBinaryInput, 2, "cooling.alarm_high_pressure", "", 1},
		{ObjBinaryInput, 3, "cooling.alarm_low_evap_temp", "", 1},
		{ObjBinaryInput, 4, "cooling.alarm_flow_loss", "", 1},
	},
	"pump": {
		{ObjAnalogInput, 1, "cooling.pump_speed_pct", "", 1},
		{ObjAnalogInput, 2, "cooling.pump_flow_lps", "", 1},
		{ObjAnalogInput, 3, "cooling.discharge_pressure_kpa", "", 1},
		{ObjAnalogInput, 4, "cooling.suction_pressure_kpa", "", 1},
		{ObjAnalogInput, 5, "cooling.diff_pressure_kpa", "", 1},
		{ObjAnalogInput, 6, "cooling.motor_power_kw", "", 1},
		{ObjAnalogInput, 7, "cooling.motor_temp_c", "", 1},
		{ObjAnalogInput, 8, "cooling.vfd_frequency_hz", "", 1},
		{ObjAnalogInput, 9, "cooling.run_hours_h", "", 1},
		{ObjBinaryInput, 1, "cooling.pump_running", "", 1},
		{ObjBinaryInput, 2, "cooling.alarm_fault", "", 1},
		{ObjBinaryInput, 3, "cooling.alarm_low_flow", "", 1},
	},
	"cooling_tower": {
		{ObjAnalogInput, 1, "cooling.fan_speed_pct", "", 1},
		{ObjAnalogInput, 2, "cooling.basin_temp_c", "", 1},
		{ObjAnalogInput, 3, "cooling.cond_water_in_c", "", 1},
		{ObjAnalogInput, 4, "cooling.cond_water_out_c", "", 1},
		{ObjAnalogInput, 5, "cooling.fan_power_kw", "", 1},
		{ObjAnalogInput, 6, "cooling.basin_level_pct", "", 1},
		{ObjAnalogInput, 7, "cooling.makeup_flow_lpm", "", 1},
		{ObjAnalogInput, 8, "cooling.vibration_mms", "", 1},
		{ObjAnalogInput, 9, "cooling.run_hours_h", "", 1},
		{ObjBinaryInput, 1, "cooling.fan_running", "", 1},
		{ObjBinaryInput, 2, "cooling.alarm_high_vibration", "", 1},
		{ObjBinaryInput, 3, "cooling.alarm_low_basin", "", 1},
	},
	"valve": {
		{ObjAnalogInput, 1, "cooling.valve_position_pct", "", 1},
		{ObjAnalogInput, 2, "cooling.valve_commanded_position_pct", "", 1},
		{ObjAnalogInput, 3, "cooling.actuator_temp_c", "", 1},
		{ObjBinaryInput, 1, "cooling.valve_modulating", "", 1},
		{ObjBinaryInput, 2, "cooling.alarm_actuator_fault", "", 1},
	},
	"cdu": {
		{ObjAnalogInput, 1, "cooling.tcs_supply_temp_c", "", 1},
		{ObjAnalogInput, 2, "cooling.tcs_return_temp_c", "", 1},
		{ObjAnalogInput, 3, "cooling.tcs_setpoint_c", "", 1},
		{ObjAnalogInput, 4, "cooling.tcs_flow_lps", "", 1},
		{ObjAnalogInput, 5, "cooling.facility_chw_valve_pct", "", 1},
		{ObjAnalogInput, 6, "cooling.facility_chw_flow_lps", "", 1},
		// Sim PLANT_SPEC inserts TCS_Loop_Pressure as AI:7 (bacnet_plant_generator.py),
		// so every CDU point from here on is +1 vs the old map. Without AI:7 mapped,
		// heat_load/pump_power/pump_speed/approach/filter_dp/run_hours each read the
		// neighbouring object's value, and loop pressure was never collected.
		{ObjAnalogInput, 7, "cooling.tcs_loop_pressure_kpa", "", 1},
		{ObjAnalogInput, 8, "cooling.heat_load_kw", "", 1},
		{ObjAnalogInput, 9, "cooling.pump_power_kw", "", 1},
		{ObjAnalogInput, 10, "cooling.pump_speed_pct", "", 1},
		{ObjAnalogInput, 11, "cooling.approach_temp_c", "", 1},
		{ObjAnalogInput, 12, "cooling.filter_dp_kpa", "", 1},
		{ObjAnalogInput, 13, "cooling.run_hours_h", "", 1},
		{ObjBinaryInput, 1, "cooling.cdu_running", "", 1},
		{ObjBinaryInput, 2, "cooling.alarm_leak", "", 1},
		{ObjBinaryInput, 3, "cooling.alarm_high_supply_temp", "", 1},
		{ObjBinaryInput, 4, "cooling.alarm_pump_fault", "", 1},
		{ObjBinaryInput, 5, "cooling.alarm_low_flow", "", 1},
	},
	"crah": {
		{ObjAnalogInput, 1, "cooling.supply_air_temp_c", "", 1},
		{ObjAnalogInput, 2, "cooling.return_air_temp_c", "", 1},
		{ObjAnalogInput, 3, "cooling.crah_setpoint_c", "", 1},
		{ObjAnalogInput, 4, "cooling.fan_speed_pct", "", 1},
		{ObjAnalogInput, 5, "cooling.chw_valve_pct", "", 1},
		{ObjAnalogInput, 6, "cooling.cooling_capacity_pct", "", 1},
		{ObjAnalogInput, 7, "cooling.supply_humidity_pct", "", 1},
		{ObjAnalogInput, 8, "cooling.airflow_pct", "", 1},
		{ObjAnalogInput, 9, "cooling.fan_power_kw", "", 1},
		{ObjAnalogInput, 10, "cooling.run_hours_h", "", 1},
		{ObjBinaryInput, 1, "cooling.crah_running", "", 1},
		{ObjBinaryInput, 2, "cooling.alarm_high_temp", "", 1},
		{ObjBinaryInput, 3, "cooling.alarm_airflow_loss", "", 1},
		{ObjBinaryInput, 4, "cooling.alarm_filter_dirty", "", 1},
	},
}

// isPlantType reports whether a device_type is a chiller-plant BACnet device.
func isPlantType(dt string) bool {
	_, ok := plantObjects[dt]
	return ok
}
