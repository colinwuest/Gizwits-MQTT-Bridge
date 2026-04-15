#!/usr/bin/env python3
"""
dhw_mqtt.py — Gizwits  Home Assistant MQTT bridge

Publishes heat pump readings via MQTT Discovery so HA auto-creates all
entities. Also subscribes to the setpoint command topic so HA can change
the target temperature from the UI, automations, or scripts.

Requirements:
    pip3 install paho-mqtt requests

Configuration:
    Edit the CONFIG section below, or override with environment variables:
        MQTT_HOST, MQTT_PORT, MQTT_USER, MQTT_PASS,
        GIZWITS_TOKEN, GIZWITS_APP_ID, GIZWITS_DID

Usage:
    python3 hot_water_mqtt.py
"""

import json
import logging
import os
import signal
import threading
import time
from typing import Optional

import paho.mqtt.client as mqtt
import requests

# ── Config ────────────────────────────────────────────────────────────────────
MQTT_HOST      = os.getenv("MQTT_HOST",      "localhost")
MQTT_PORT      = int(os.getenv("MQTT_PORT",  "1883"))
MQTT_USER      = os.getenv("MQTT_USER",      "")   # leave empty if no auth
MQTT_PASS      = os.getenv("MQTT_PASS",      "")
MQTT_CLIENT_ID = os.getenv("MQTT_CLIENT_ID", "hot_water_bridge")

# Path where baseline/solar temps are persisted (mount a Docker volume here)
DATA_DIR       = os.getenv("DATA_DIR", "/data")
STATE_FILE     = os.path.join(DATA_DIR, "state.json")

POLL_INTERVAL  = 120   # seconds between Gizwits polls

# Tank parameters for energy calculations
TANK_VOLUME    = int(os.getenv("TANK_VOLUME", "200"))   # litres
COLD_WATER_REF = float(os.getenv("COLD_WATER_TEMP", "10"))  # °C reference for stored energy
SPECIFIC_HEAT  = 4.186  # kJ/(kg·°C) for water

GIZWITS_BASE   = "https://euapi.gizwits.com"
DID            = os.getenv("GIZWITS_DID",    "")
GIZWITS_HEADERS = {
    "x-gizwits-application-id": os.getenv("GIZWITS_APP_ID",
                                            ""),
    "x-gizwits-user-token":     os.getenv("GIZWITS_TOKEN",
                                           ""),
    "Content-Type":             "application/json",
}

# MQTT topic prefixes
DISC   = "homeassistant"   # HA discovery prefix
PREFIX = "hot_water"       # state/command topic prefix

# ── Entity definitions ────────────────────────────────────────────────────────
# (Gizwits attribute key) -> (mqtt_id, friendly_name)
SENSORS = {
    "Data_AmbientTemp":  ("ambient_temp",    "Hot Water Ambient temperature"),
    "Data_UpTankTemp":   ("tank_upper_temp", "Hot Water Tank temperature (upper)"),
    "Data_DownTankTemp": ("tank_lower_temp", "Hot Water Tank temperature (lower)"),
    "Data_CoilTemp":     ("coil_temp",       "Hot Water Coil temperature"),
    "Data_SuctionTemp":  ("suction_temp",    "Hot Water Suction temperature"),
    "Data_ExhaustTemp":  ("exhaust_temp",    "Hot Water Exhaust temperature"),
    "Data_SunTemp":      ("solar_temp",      "Hot Water Solar temperature"),
    "Data_ReservedTemp": ("reserved_temp",   "Hot Water Reserved temperature"),
}

# (Gizwits key) -> (mqtt_id, friendly_name, unit, device_class, icon)
NUMERIC_SENSORS = {
    "Data_CompressorFreq":                ("compressor_freq",          "Hot Water Compressor frequency",     "Hz",  "frequency", None),
    "Data_CompressorCurrent":             ("compressor_current",       "Hot Water Compressor current",       "A",   "current",   None),
    "Data_CompressorVolt":                ("compressor_voltage",       "Hot Water Compressor voltage",       "V",   "voltage",   None),
    "Data_ElectricHeatingCurrent":        ("electric_heating_current", "Hot Water Electric heating current", "A",   "current",   None),
    "Data_ElectronicExpansionValve":      ("expansion_valve",          "Hot Water Expansion valve position", None,  None,        "mdi:valve"),
    "Data_CompressorAccumulativeRunTimeL": ("compressor_runtime",      "Hot Water Compressor runtime",       "min", None,        "mdi:timer"),
    "Data_RunState":                      ("run_state",                "Hot Water Run state",                None,  None,        "mdi:information-outline"),
}

# Calculated sensors (not from Gizwits — derived locally)
# Tuple: (mqtt_id, friendly_name, unit, device_class, icon, state_class)
CALCULATED_SENSORS = {
    "tank_energy":          ("tank_energy",          "Hot Water Tank energy stored",        "kWh", "energy", "mdi:water-boiler",  "measurement"),
    "heat_generated_today": ("heat_generated_today", "Hot Water Heat generated today",      "kWh", "energy", "mdi:heat-wave",     "measurement"),
    "heat_used_today":      ("heat_used_today",      "Hot Water Heat used today",           "kWh", "energy", "mdi:shower",        "measurement"),
    "total_heat_generated": ("total_heat_generated", "Hot Water Heat generated (lifetime)", "kWh", "energy", "mdi:heat-wave",     "total_increasing"),
    "total_heat_used":      ("total_heat_used",      "Hot Water Heat used (lifetime)",      "kWh", "energy", "mdi:shower-head",   "total_increasing"),
}

# (Gizwits key) -> (mqtt_id, friendly_name, device_class, icon)
BINARY_SENSORS = {
    "Out_Compressor":           ("compressor_running",   "Hot Water Compressor",       "running", "mdi:rotate-3d-variant"),
    "Out_ElectricHeating":      ("electric_heating",     "Hot Water Electric heating", "heat",    "mdi:lightning-bolt"),
    "Out_OutsideFan":           ("fan_running",          "Hot Water Fan",              "running", "mdi:fan"),
    "Out_WaterCirculatingPump": ("circulation_pump",     "Hot Water Circulation pump", "running", "mdi:pump"),
    "Out_SunWaterPump":         ("solar_pump",           "Hot Water Solar pump",       "running", "mdi:solar-power"),
    "State_Sterilization":      ("sterilisation",        "Hot Water Sterilisation",    "running", "mdi:bacteria"),
    "State_Antifreeze":         ("antifreeze",           "Hot Water Antifreeze",       "cold",    "mdi:snowflake"),
    "Cmd_Power":                ("power_state",          "Hot Water Power",            "power",   None),
}

SETPOINT_GIZWITS_KEY = "Para_SetTemp"
SETPOINT_ID          = "setpoint"
SETPOINT_NAME        = "Hot Water Target temperature"
SETPOINT_MIN         = 0
SETPOINT_MAX         = 75

# ── Solar mode entities ───────────────────────────────────────────────────────
SOLAR_SWITCH_ID       = "solar_mode"
SOLAR_SWITCH_NAME     = "Hot Water Excess solar mode"

BASELINE_TEMP_ID      = "baseline_temp"
BASELINE_TEMP_NAME    = "Hot Water Baseline temperature"
BASELINE_TEMP_DEFAULT = 50

SOLAR_TEMP_ID         = "solar_temp_setpoint"
SOLAR_TEMP_NAME       = "Hot Water Excess solar temperature"
SOLAR_TEMP_DEFAULT    = 65

# ── Heating mode ─────────────────────────────────────────────────────────────
# Para_Mode is a uint8 (0-7). The schema has no labels; these names match
# the standard Outes/JNOD air-source heat pump modes. Verify by changing
# modes in the eHomeMaster app and watching Para_Mode change.
MODE_MAP = {
    "Auto":      0,
    "Eco":       1,
    "Fast Heat": 2,
    "Sleep":     3,
    "Holiday":   4,
}
MODE_MAP_INV   = {v: k for k, v in MODE_MAP.items()}
MODE_ID        = "heating_mode"
MODE_NAME      = "Hot Water Heating mode"
MODE_GIZWITS   = "Para_Mode"

BASELINE_MODE_ID      = "baseline_mode"
BASELINE_MODE_NAME    = "Hot Water Baseline heating mode"
BASELINE_MODE_DEFAULT = "Eco"

SOLAR_MODE_ID         = "solar_heat_mode"
SOLAR_MODE_NAME       = "Hot Water Excess solar heating mode"
SOLAR_MODE_DEFAULT    = "Fast Heat"

# ── Runtime state (in-memory) ─────────────────────────────────────────────────
_state = {
    "solar_mode":    False,
    "baseline_temp": BASELINE_TEMP_DEFAULT,
    "solar_temp":    SOLAR_TEMP_DEFAULT,
    "baseline_mode": BASELINE_MODE_DEFAULT,
    "solar_mode_heat":      SOLAR_MODE_DEFAULT,
    "daily_heat_generated": 0.0,   # kWh added to tank today
    "daily_heat_used":      0.0,   # kWh removed from tank today
    "daily_stats_date":     "",    # YYYY-MM-DD of current counters
    "last_energy_kwh":      None,  # previous tank energy reading
    "total_heat_generated": 0.0,   # kWh added to tank all-time
    "total_heat_used":      0.0,   # kWh removed from tank all-time
}


def _load_state() -> None:
    """Load persisted baseline/solar temps from the data volume on startup."""
    try:
        with open(STATE_FILE) as f:
            saved = json.load(f)
        _state["baseline_temp"] = int(saved.get("baseline_temp", BASELINE_TEMP_DEFAULT))
        _state["solar_temp"]    = int(saved.get("solar_temp",    SOLAR_TEMP_DEFAULT))
        _state["baseline_mode"] = saved.get("baseline_mode", BASELINE_MODE_DEFAULT)
        _state["solar_mode_heat"]      = saved.get("solar_mode_heat", SOLAR_MODE_DEFAULT)
        _state["daily_heat_generated"] = float(saved.get("daily_heat_generated", 0.0))
        _state["daily_heat_used"]      = float(saved.get("daily_heat_used",      0.0))
        _state["daily_stats_date"]     = saved.get("daily_stats_date", "")
        _state["total_heat_generated"] = float(saved.get("total_heat_generated", 0.0))
        _state["total_heat_used"]      = float(saved.get("total_heat_used",      0.0))
        log.info("State loaded from %s: %s", STATE_FILE, saved)
    except FileNotFoundError:
        log.info("No state file found at %s — using defaults", STATE_FILE)
    except Exception as exc:
        log.warning("Could not load state file: %s", exc)


def _save_state() -> None:
    """Persist baseline/solar temps to the data volume."""
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(STATE_FILE, "w") as f:
            json.dump({
                "baseline_temp":        _state["baseline_temp"],
                "solar_temp":           _state["solar_temp"],
                "baseline_mode":        _state["baseline_mode"],
                "solar_mode_heat":      _state["solar_mode_heat"],
                "daily_heat_generated": _state["daily_heat_generated"],
                "daily_heat_used":      _state["daily_heat_used"],
                "daily_stats_date":     _state["daily_stats_date"],
                "total_heat_generated": _state["total_heat_generated"],
                "total_heat_used":      _state["total_heat_used"],
            }, f, indent=2)
    except Exception as exc:
        log.warning("Could not save state file: %s", exc)

DEVICE_INFO = {
    "identifiers":  ["hot_water_gizwits"],
    "name":         "Hot Water",
    "model":        "W-HTR-A8CF",
    "manufacturer": "Gizwits",
}

# ── Logging ───────────────────────────────────────────────────────────────────
log = logging.getLogger("hot_water")


# ── MQTT Discovery payloads ───────────────────────────────────────────────────
def _sensor_config(mqtt_id: str, friendly_name: str) -> dict:
    return {
        "name":                friendly_name,
        "unique_id":           f"hot_water_{mqtt_id}",
        "state_topic":         f"{PREFIX}/sensor/{mqtt_id}/state",
        "unit_of_measurement": "°C",
        "device_class":        "temperature",
        "state_class":         "measurement",
        "device":              DEVICE_INFO,
    }


def _number_config() -> dict:
    return {
        "name":                SETPOINT_NAME,
        "unique_id":           f"hot_water_{SETPOINT_ID}",
        "state_topic":         f"{PREFIX}/number/{SETPOINT_ID}/state",
        "command_topic":       f"{PREFIX}/number/{SETPOINT_ID}/set",
        "unit_of_measurement": "°C",
        "device_class":        "temperature",
        "min":                 SETPOINT_MIN,
        "max":                 SETPOINT_MAX,
        "step":                1,
        "mode":                "slider",
        "icon":                "mdi:thermometer-water",
        "device":              DEVICE_INFO,
    }


def _simple_number_config(entity_id: str, name: str, default: int) -> dict:
    return {
        "name":                name,
        "unique_id":           f"hot_water_{entity_id}",
        "state_topic":         f"{PREFIX}/number/{entity_id}/state",
        "command_topic":       f"{PREFIX}/number/{entity_id}/set",
        "unit_of_measurement": "°C",
        "device_class":        "temperature",
        "min":                 SETPOINT_MIN,
        "max":                 SETPOINT_MAX,
        "step":                1,
        "mode":                "slider",
        "icon":                "mdi:thermometer-water",
        "entity_category":     "config",
        "device":              DEVICE_INFO,
    }


def _switch_config() -> dict:
    return {
        "name":          SOLAR_SWITCH_NAME,
        "unique_id":     f"hot_water_{SOLAR_SWITCH_ID}",
        "state_topic":   f"{PREFIX}/switch/{SOLAR_SWITCH_ID}/state",
        "command_topic": f"{PREFIX}/switch/{SOLAR_SWITCH_ID}/set",
        "payload_on":    "ON",
        "payload_off":   "OFF",
        "icon":          "mdi:solar-power",
        "device":        DEVICE_INFO,
    }


def _select_config() -> dict:
    return {
        "name":          MODE_NAME,
        "unique_id":     f"hot_water_{MODE_ID}",
        "state_topic":   f"{PREFIX}/select/{MODE_ID}/state",
        "command_topic": f"{PREFIX}/select/{MODE_ID}/set",
        "options":       list(MODE_MAP.keys()),
        "icon":          "mdi:heat-wave",
        "device":        DEVICE_INFO,
    }


def _mode_select_config(entity_id: str, name: str) -> dict:
    return {
        "name":            name,
        "unique_id":       f"hot_water_{entity_id}",
        "state_topic":     f"{PREFIX}/select/{entity_id}/state",
        "command_topic":   f"{PREFIX}/select/{entity_id}/set",
        "options":         list(MODE_MAP.keys()),
        "icon":            "mdi:heat-wave",
        "entity_category": "config",
        "device":          DEVICE_INFO,
    }


def _numeric_sensor_config(mqtt_id: str, name: str, unit: Optional[str],
                           device_class: Optional[str], icon: Optional[str],
                           state_class: str = "measurement") -> dict:
    cfg = {
        "name":        name,
        "unique_id":   f"hot_water_{mqtt_id}",
        "state_topic": f"{PREFIX}/sensor/{mqtt_id}/state",
        "state_class": state_class,
        "device":      DEVICE_INFO,
    }
    if unit:
        cfg["unit_of_measurement"] = unit
    if device_class:
        cfg["device_class"] = device_class
    if icon:
        cfg["icon"] = icon
    return cfg


def _binary_sensor_config(mqtt_id: str, name: str, device_class: Optional[str],
                          icon: Optional[str]) -> dict:
    cfg = {
        "name":        name,
        "unique_id":   f"hot_water_{mqtt_id}",
        "state_topic": f"{PREFIX}/binary_sensor/{mqtt_id}/state",
        "payload_on":  "ON",
        "payload_off": "OFF",
        "device":      DEVICE_INFO,
    }
    if device_class:
        cfg["device_class"] = device_class
    if icon:
        cfg["icon"] = icon
    return cfg


def publish_discovery(client: mqtt.Client) -> None:
    """Publish retained MQTT Discovery configs — HA creates entities on receipt."""
    for _, (mqtt_id, friendly_name) in SENSORS.items():
        topic = f"{DISC}/sensor/hot_water_{mqtt_id}/config"
        client.publish(topic, json.dumps(_sensor_config(mqtt_id, friendly_name)),
                       retain=True)

    client.publish(f"{DISC}/number/hot_water_{SETPOINT_ID}/config",
                   json.dumps(_number_config()), retain=True)

    client.publish(f"{DISC}/number/hot_water_{BASELINE_TEMP_ID}/config",
                   json.dumps(_simple_number_config(BASELINE_TEMP_ID,
                              BASELINE_TEMP_NAME, BASELINE_TEMP_DEFAULT)),
                   retain=True)

    client.publish(f"{DISC}/number/hot_water_{SOLAR_TEMP_ID}/config",
                   json.dumps(_simple_number_config(SOLAR_TEMP_ID,
                              SOLAR_TEMP_NAME, SOLAR_TEMP_DEFAULT)),
                   retain=True)

    client.publish(f"{DISC}/switch/hot_water_{SOLAR_SWITCH_ID}/config",
                   json.dumps(_switch_config()), retain=True)

    client.publish(f"{DISC}/select/hot_water_{MODE_ID}/config",
                   json.dumps(_select_config()), retain=True)

    client.publish(f"{DISC}/select/hot_water_{BASELINE_MODE_ID}/config",
                   json.dumps(_mode_select_config(BASELINE_MODE_ID, BASELINE_MODE_NAME)),
                   retain=True)

    client.publish(f"{DISC}/select/hot_water_{SOLAR_MODE_ID}/config",
                   json.dumps(_mode_select_config(SOLAR_MODE_ID, SOLAR_MODE_NAME)),
                   retain=True)

    for _, (mqtt_id, name, unit, device_class, icon) in NUMERIC_SENSORS.items():
        topic = f"{DISC}/sensor/hot_water_{mqtt_id}/config"
        client.publish(topic,
                       json.dumps(_numeric_sensor_config(mqtt_id, name, unit, device_class, icon)),
                       retain=True)

    for _, (mqtt_id, name, device_class, icon) in BINARY_SENSORS.items():
        topic = f"{DISC}/binary_sensor/hot_water_{mqtt_id}/config"
        client.publish(topic,
                       json.dumps(_binary_sensor_config(mqtt_id, name, device_class, icon)),
                       retain=True)

    for _, (mqtt_id, name, unit, device_class, icon, state_class) in CALCULATED_SENSORS.items():
        topic = f"{DISC}/sensor/hot_water_{mqtt_id}/config"
        client.publish(topic,
                       json.dumps(_numeric_sensor_config(mqtt_id, name, unit, device_class, icon, state_class)),
                       retain=True)

    log.info("MQTT Discovery configs published")


def _publish_solar_state(client: mqtt.Client) -> None:
    """Publish current in-memory state of solar mode entities."""
    client.publish(f"{PREFIX}/switch/{SOLAR_SWITCH_ID}/state",
                   "ON" if _state["solar_mode"] else "OFF", retain=True)
    client.publish(f"{PREFIX}/number/{BASELINE_TEMP_ID}/state",
                   str(_state["baseline_temp"]), retain=True)
    client.publish(f"{PREFIX}/number/{SOLAR_TEMP_ID}/state",
                   str(_state["solar_temp"]), retain=True)
    client.publish(f"{PREFIX}/select/{BASELINE_MODE_ID}/state",
                   _state["baseline_mode"], retain=True)
    client.publish(f"{PREFIX}/select/{SOLAR_MODE_ID}/state",
                   _state["solar_mode_heat"], retain=True)


def _apply_solar_setpoint(client: mqtt.Client) -> None:
    """Send the correct setpoint AND heating mode to Gizwits based on solar mode state."""
    is_solar = _state["solar_mode"]
    target_temp = _state["solar_temp"] if is_solar else _state["baseline_temp"]
    target_mode = _state["solar_mode_heat"] if is_solar else _state["baseline_mode"]
    target_mode_int = MODE_MAP.get(target_mode)
    label = "ON" if is_solar else "OFF"
    try:
        set_setpoint(target_temp)
        client.publish(f"{PREFIX}/number/{SETPOINT_ID}/state",
                       str(target_temp), retain=True)
        log.info("Solar mode %s — setpoint applied: %d °C", label, target_temp)
    except requests.RequestException as exc:
        log.error("Failed to apply solar setpoint: %s", exc)
    if target_mode_int is not None:
        try:
            resp = requests.post(
                f"{GIZWITS_BASE}/app/control/{DID}",
                headers=GIZWITS_HEADERS,
                json={"attrs": {MODE_GIZWITS: target_mode_int}},
                timeout=10,
            )
            resp.raise_for_status()
            client.publish(f"{PREFIX}/select/{MODE_ID}/state", target_mode, retain=True)
            log.info("Solar mode %s — heating mode applied: %s", label, target_mode)
        except requests.RequestException as exc:
            log.error("Failed to apply solar heating mode: %s", exc)


# ── State publishing ──────────────────────────────────────────────────────────
def publish_states(client: mqtt.Client, attrs: dict) -> None:
    for gizwits_key, (mqtt_id, _) in SENSORS.items():
        if gizwits_key in attrs:
            client.publish(f"{PREFIX}/sensor/{mqtt_id}/state",
                           str(attrs[gizwits_key]), retain=True)

    if SETPOINT_GIZWITS_KEY in attrs:
        client.publish(f"{PREFIX}/number/{SETPOINT_ID}/state",
                       str(attrs[SETPOINT_GIZWITS_KEY]), retain=True)

    if MODE_GIZWITS in attrs:
        mode_int = attrs[MODE_GIZWITS]
        mode_name = MODE_MAP_INV.get(mode_int, f"Mode {mode_int}")
        client.publish(f"{PREFIX}/select/{MODE_ID}/state", mode_name, retain=True)

    for gizwits_key, (mqtt_id, *_) in NUMERIC_SENSORS.items():
        if gizwits_key in attrs:
            client.publish(f"{PREFIX}/sensor/{mqtt_id}/state",
                           str(attrs[gizwits_key]), retain=True)

    for gizwits_key, (mqtt_id, *_) in BINARY_SENSORS.items():
        if gizwits_key in attrs:
            client.publish(f"{PREFIX}/binary_sensor/{mqtt_id}/state",
                           "ON" if attrs[gizwits_key] else "OFF", retain=True)

    # ── Calculated sensors ────────────────────────────────────────────────
    upper = attrs.get("Data_UpTankTemp")
    lower = attrs.get("Data_DownTankTemp")
    if upper is not None and lower is not None:
        avg_temp = (upper + lower) / 2.0
        # Thermal energy stored in the tank relative to cold-water reference
        # Q = m × c × ΔT  (litres ≈ kg for water)
        energy_kj = TANK_VOLUME * SPECIFIC_HEAT * (avg_temp - COLD_WATER_REF)
        energy_kwh = round(energy_kj / 3600.0, 2)
        client.publish(f"{PREFIX}/sensor/tank_energy/state",
                       str(energy_kwh), retain=True)

        # ── Daily energy counters ─────────────────────────────────────────
        today = time.strftime("%Y-%m-%d")
        if _state["daily_stats_date"] != today:
            log.info("Daily heat counters reset for %s (was %s) — generated=%.2f kWh, used=%.2f kWh",
                     today, _state["daily_stats_date"] or "(none)",
                     _state["daily_heat_generated"], _state["daily_heat_used"])
            _state["daily_stats_date"]     = today
            _state["daily_heat_generated"] = 0.0
            _state["daily_heat_used"]      = 0.0
            _state["last_energy_kwh"]      = None

        if _state["last_energy_kwh"] is not None:
            delta = round(energy_kwh - _state["last_energy_kwh"], 3)
            if delta > 0:
                _state["daily_heat_generated"] = round(_state["daily_heat_generated"] + delta, 2)
                _state["total_heat_generated"] = round(_state["total_heat_generated"] + delta, 2)
                log.debug("Tank energy up %.3f kWh → generated today: %.2f kWh  lifetime: %.2f kWh",
                          delta, _state["daily_heat_generated"], _state["total_heat_generated"])
            elif delta < 0:
                _state["daily_heat_used"] = round(_state["daily_heat_used"] - delta, 2)
                _state["total_heat_used"] = round(_state["total_heat_used"] - delta, 2)
                log.debug("Tank energy down %.3f kWh → used today: %.2f kWh  lifetime: %.2f kWh",
                          abs(delta), _state["daily_heat_used"], _state["total_heat_used"])

        _state["last_energy_kwh"] = energy_kwh
        _save_state()

        client.publish(f"{PREFIX}/sensor/heat_generated_today/state",
                       str(round(_state["daily_heat_generated"], 2)), retain=True)
        client.publish(f"{PREFIX}/sensor/heat_used_today/state",
                       str(round(_state["daily_heat_used"], 2)), retain=True)
        client.publish(f"{PREFIX}/sensor/total_heat_generated/state",
                       str(round(_state["total_heat_generated"], 2)), retain=True)
        client.publish(f"{PREFIX}/sensor/total_heat_used/state",
                       str(round(_state["total_heat_used"], 2)), retain=True)


# ── Gizwits API ───────────────────────────────────────────────────────────────
def fetch_attrs() -> dict:
    resp = requests.get(
        f"{GIZWITS_BASE}/app/devdata/{DID}/latest",
        headers=GIZWITS_HEADERS,
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json().get("attr", {})


def set_setpoint(value: int) -> None:
    resp = requests.post(
        f"{GIZWITS_BASE}/app/control/{DID}",
        headers=GIZWITS_HEADERS,
        json={"attrs": {SETPOINT_GIZWITS_KEY: value}},
        timeout=10,
    )
    resp.raise_for_status()
    log.info("Setpoint sent to Gizwits: %d °C", value)


# ── MQTT callbacks ────────────────────────────────────────────────────────────
_connect_count = 0

def on_connect(client, userdata, flags, reason_code, properties=None):
    global _connect_count
    # reason_code is an object in paho 2.x; int in 1.x
    rc = reason_code if isinstance(reason_code, int) else reason_code.value
    _connect_count += 1
    if rc == 0:
        if _connect_count == 1:
            log.info("MQTT connected to %s:%d", MQTT_HOST, MQTT_PORT)
        else:
            log.warning("MQTT reconnected to %s:%d (connect #%d)", MQTT_HOST, MQTT_PORT, _connect_count)
        publish_discovery(client)
        topics = [
            f"{PREFIX}/number/{SETPOINT_ID}/set",
            f"{PREFIX}/number/{BASELINE_TEMP_ID}/set",
            f"{PREFIX}/number/{SOLAR_TEMP_ID}/set",
            f"{PREFIX}/switch/{SOLAR_SWITCH_ID}/set",
            f"{PREFIX}/select/{MODE_ID}/set",
            f"{PREFIX}/select/{BASELINE_MODE_ID}/set",
            f"{PREFIX}/select/{SOLAR_MODE_ID}/set",
        ]
        for t in topics:
            client.subscribe(t)
            log.info("Subscribed to %s", t)
        # Restore retained solar state to HA
        _publish_solar_state(client)
    else:
        log.error("MQTT connection refused (rc=%d)", rc)


def _parse_temp(payload: str) -> Optional[int]:
    try:
        value = int(float(payload))
    except ValueError:
        return None
    if not (SETPOINT_MIN <= value <= SETPOINT_MAX):
        log.error("Temperature %d°C out of range (%d–%d)",
                  value, SETPOINT_MIN, SETPOINT_MAX)
        return None
    return value


def on_message(client, userdata, msg):
    payload = msg.payload.decode().strip()
    log.debug("MQTT ← %s : %s", msg.topic, payload)

    # ── Direct setpoint override ──────────────────────────────────────────────
    if msg.topic == f"{PREFIX}/number/{SETPOINT_ID}/set":
        log.info("Received setpoint command: %s", payload)
        value = _parse_temp(payload)
        if value is None:
            return
        try:
            set_setpoint(value)
            client.publish(f"{PREFIX}/number/{SETPOINT_ID}/state",
                           str(value), retain=True)
        except requests.RequestException as exc:
            log.error("Failed to set setpoint: %s", exc)

    # ── Baseline temperature slider ───────────────────────────────────────────
    elif msg.topic == f"{PREFIX}/number/{BASELINE_TEMP_ID}/set":
        log.info("Received baseline temp command: %s (current: %d)", payload, _state["baseline_temp"])
        value = _parse_temp(payload)
        if value is None:
            return
        if value == _state["baseline_temp"]:
            log.debug("Baseline temp unchanged at %d — skipping", value)
            return
        _state["baseline_temp"] = value
        client.publish(f"{PREFIX}/number/{BASELINE_TEMP_ID}/state",
                       str(value), retain=True)
        log.info("Baseline temperature updated: %d °C", value)
        _save_state()
        if not _state["solar_mode"]:
            _apply_solar_setpoint(client)

    # ── Solar temperature slider ──────────────────────────────────────────────
    elif msg.topic == f"{PREFIX}/number/{SOLAR_TEMP_ID}/set":
        log.info("Received solar temp command: %s (current: %d)", payload, _state["solar_temp"])
        value = _parse_temp(payload)
        if value is None:
            return
        if value == _state["solar_temp"]:
            log.debug("Solar temp unchanged at %d — skipping", value)
            return
        _state["solar_temp"] = value
        client.publish(f"{PREFIX}/number/{SOLAR_TEMP_ID}/state",
                       str(value), retain=True)
        log.info("Solar temperature updated: %d °C", value)
        _save_state()
        if _state["solar_mode"]:
            _apply_solar_setpoint(client)

    # ── Solar mode switch ─────────────────────────────────────────────────────
    elif msg.topic == f"{PREFIX}/switch/{SOLAR_SWITCH_ID}/set":
        new_mode = payload.upper() == "ON"
        log.info("Received solar switch command: %s (current: %s)", payload, _state["solar_mode"])
        if new_mode == _state["solar_mode"]:
            log.debug("Solar mode unchanged at %s — skipping", new_mode)
            return
        _state["solar_mode"] = new_mode
        client.publish(f"{PREFIX}/switch/{SOLAR_SWITCH_ID}/state",
                       "ON" if new_mode else "OFF", retain=True)
        log.info("Solar mode switched %s", "ON" if new_mode else "OFF")
        _apply_solar_setpoint(client)


    # ── Heating mode select ──────────────────────────────────────────────
    elif msg.topic == f"{PREFIX}/select/{MODE_ID}/set":
        mode_int = MODE_MAP.get(payload)
        if mode_int is None:
            log.error("Unknown mode: %r. Valid options: %s", payload, list(MODE_MAP))
            return
        try:
            resp = requests.post(
                f"{GIZWITS_BASE}/app/control/{DID}",
                headers=GIZWITS_HEADERS,
                json={"attrs": {MODE_GIZWITS: mode_int}},
                timeout=10,
            )
            resp.raise_for_status()
            client.publish(f"{PREFIX}/select/{MODE_ID}/state", payload, retain=True)
            log.info("Heating mode set to %s (%d)", payload, mode_int)
        except requests.RequestException as exc:
            log.error("Failed to set heating mode: %s", exc)

    # ── Baseline heating mode select ──────────────────────────────────────────
    elif msg.topic == f"{PREFIX}/select/{BASELINE_MODE_ID}/set":
        log.info("Received baseline mode command: %s (current: %s)", payload, _state["baseline_mode"])
        if payload not in MODE_MAP:
            log.error("Unknown mode: %r. Valid options: %s", payload, list(MODE_MAP))
            return
        if payload == _state["baseline_mode"]:
            log.debug("Baseline mode unchanged at %s — skipping", payload)
            return
        _state["baseline_mode"] = payload
        client.publish(f"{PREFIX}/select/{BASELINE_MODE_ID}/state", payload, retain=True)
        log.info("Baseline heating mode updated: %s", payload)
        _save_state()
        if not _state["solar_mode"]:
            _apply_solar_setpoint(client)

    # ── Solar heating mode select ─────────────────────────────────────────────
    elif msg.topic == f"{PREFIX}/select/{SOLAR_MODE_ID}/set":
        log.info("Received solar heat mode command: %s (current: %s)", payload, _state["solar_mode_heat"])
        if payload not in MODE_MAP:
            log.error("Unknown mode: %r. Valid options: %s", payload, list(MODE_MAP))
            return
        if payload == _state["solar_mode_heat"]:
            log.debug("Solar heat mode unchanged at %s — skipping", payload)
            return
        _state["solar_mode_heat"] = payload
        client.publish(f"{PREFIX}/select/{SOLAR_MODE_ID}/state", payload, retain=True)
        log.info("Solar heating mode updated: %s", payload)
        _save_state()
        if _state["solar_mode"]:
            _apply_solar_setpoint(client)


def on_disconnect(client, userdata, reason_code, properties=None):
    rc = reason_code if isinstance(reason_code, int) else reason_code.value
    if rc == 0:
        log.info("MQTT disconnected cleanly")
    else:
        log.warning("MQTT disconnected unexpectedly (rc=%d) — will reconnect", rc)


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    _load_state()

    # Support both paho-mqtt 1.x and 2.x
    try:
        client = mqtt.Client(
            client_id=MQTT_CLIENT_ID,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )
    except AttributeError:
        client = mqtt.Client(client_id=MQTT_CLIENT_ID)

    client.on_connect    = on_connect
    client.on_message    = on_message
    client.on_disconnect = on_disconnect

    if MQTT_USER:
        client.username_pw_set(MQTT_USER, MQTT_PASS)

    client.connect(MQTT_HOST, MQTT_PORT, keepalive=300)
    client.loop_start()   # handles reconnects in background thread

    # Graceful shutdown
    stop = threading.Event()
    def _shutdown(sig, frame):
        log.info("Shutdown signal received")
        stop.set()
    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    log.info("Service running — polling Gizwits every %ds", POLL_INTERVAL)
    while not stop.is_set():
        try:
            attrs = fetch_attrs()
            publish_states(client, attrs)
            log.info(
                "Published — tank=%.1f°C  ambient=%.1f°C  setpoint=%s°C",
                attrs.get("Data_UpTankTemp", 0),
                attrs.get("Data_AmbientTemp", 0),
                attrs.get(SETPOINT_GIZWITS_KEY, "?"),
            )
        except requests.RequestException as exc:
            log.error("Gizwits fetch failed: %s", exc)

        stop.wait(POLL_INTERVAL)

    client.loop_stop()
    client.disconnect()
    log.info("Service stopped")


if __name__ == "__main__":
    main()
