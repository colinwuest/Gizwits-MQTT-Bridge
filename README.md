# Gizwits-MQTT-Bridge

Integrates a **Gizwits-based air-source heat pump water heater** (Outes/JNOD `W-HTR-A8CF`, sold under various brand names) with **Home Assistant** via MQTT Discovery. Tested against Outes Aab21R1/200E. 

The service polls the Gizwits cloud API every 120 seconds and publishes all sensor readings, binary states, and diagnostic data as MQTT Discovery messages so Home Assistant auto-creates every entity. It also subscribes to command topics so you can control the unit directly from HA.

---

## Features

- **Temperature sensors** — ambient, upper/lower tank, coil, suction, exhaust, solar probe
- **Target temperature control** — number entity with slider (0–75 °C)
- **Heating mode select** — Auto, Eco, Fast Heat, Sleep, Holiday
- **Excess solar mode** — one switch flips the heat pump to a higher target temperature and aggressive heating mode when solar power is available, then reverts when switched off
- **Diagnostic sensors** — compressor frequency, current & voltage, expansion valve position, runtime, available hot water level
- **Run state** — decoded to `Off` / `Standby` / `Heating` / `Fault`
- **Fault reporting** — a `problem` binary sensor plus a text sensor listing every active fault (21 fault flags decoded: high/low pressure, IPM, PFC, phase loss, fan, communication, etc.)
- **Energy sensors** — tank energy stored (kWh), heat generated today, heat used today, and lifetime totals for both
- **Binary sensors** — compressor, electric element, fan, circulation pump, solar pump, four-way valve, electronic anode, boiler output, sterilisation, antifreeze, defrost, holiday mode
- **Configuration entities** — baseline and solar temperature setpoints, baseline and solar heating mode profiles (shown in HA's device Configuration section)
- **State persistence** — solar mode profile settings and energy counters survive container restarts via a Docker named volume

---

## Prerequisites

- Home Assistant with the **Mosquitto (MQTT) integration** configured
- An MQTT broker reachable from both HA and the Docker host
- Docker + Docker Compose on the host running this service
- The Gizwits **App ID**, **user token**, and **device ID** for your heat pump (see below)

---

## Getting Your Gizwits Credentials

The vendor app communicates with `euapi.gizwits.com`. You need three values:

| Value | Where it appears |
|---|---|
| `App-ID` | `X-Gizwits-Application-Id` request header |
| `User token` | `X-Gizwits-User-token` request header |
| `Device ID` | URL path: `/app/devdata/<DID>/latest` |

### Using Proxyman on iOS

[Proxyman](https://proxyman.io) is the easiest way to capture these on an iPhone or iPad without a Mac.

1. **Install Proxyman** from the App Store and open it.
2. Tap **Certificate** → follow the on-screen steps to install and trust the Proxyman root CA in iOS Settings (`Settings → General → VPN & Device Management → Proxyman CA`). This is required to decrypt HTTPS traffic.
3. In Proxyman, tap the **filter icon** and add `euapi.gizwits.com` as a host filter so only relevant traffic is shown.
4. Open your heat pump's vendor app (e.g. *eHomeMaster*) and navigate to the device screen so it refreshes live data.
5. Back in Proxyman, you will see requests to `euapi.gizwits.com`. Tap any request to `/app/devdata/…/latest`.
6. In the **Request** tab, note:
   - `X-Gizwits-Application-Id` → this is your **App ID**
   - `X-Gizwits-User-token` → this is your **user token**
7. The **Device ID** is the path segment between `/app/devdata/` and `/latest`, e.g.:
   ```
   https://euapi.gizwits.com/app/devdata/<DID>/latest
                                                   ^^^^^^^^^^^^^^^^^^^^^^
   ```

> **Token expiry** — Gizwits user tokens can expire. If the service stops receiving data, re-capture the token from the app and update it in `docker-compose.yml`. An expired token makes the API return **HTTP 400**; the bridge logs an explicit hint when it sees a 400/401/403.

### Device schema reference

The app also calls `GET /app/datapoint?product_key=<PRODUCT_KEY>`, which returns the full
device schema — every attribute with its data type, scaling (`ratio`/`addition`) and value
semantics. This is what the run-state and fault mappings in this bridge are derived from:

```bash
curl -H "X-Gizwits-Application-Id: $APP_ID" \
     -H "X-Gizwits-User-token: $TOKEN" \
     "https://euapi.gizwits.com/app/datapoint?product_key=$PRODUCT_KEY"
```

The product key defaults to the `W-HTR-A8CF` value and can be overridden with the
`GIZWITS_PRODUCT_KEY` environment variable.

---

## Quick Start

The image is published to Docker Hub at [`cwuest/gizwits-mqtt-bridge`](https://hub.docker.com/r/cwuest/gizwits-mqtt-bridge) and supports **linux/amd64**, **linux/arm64**, and **linux/arm/v7** (Raspberry Pi).

### 1. Create a `docker-compose.yml`

```yaml
services:
  hot_water:
    image: cwuest/gizwits-mqtt-bridge:latest
    container_name: hot_water
    restart: unless-stopped
    environment:
      MQTT_HOST: "192.168.1.10"   # your MQTT broker
      MQTT_PORT: "1883"
      MQTT_USER: "myuser"
      MQTT_PASS: "mypassword"
      GIZWITS_TOKEN:  "your_user_token_here"
      GIZWITS_APP_ID: "your_app_id_here"
      GIZWITS_DID:    "your_device_id_here"
      TANK_VOLUME: "200"           # tank capacity in litres (200 or 300)
      DATA_DIR: "/data"
    volumes:
      - hot_water_data:/data
    logging:
      driver: "json-file"
      options:
        max-size: "5m"
        max-file: "3"

volumes:
  hot_water_data:
```

### 2. Run

```bash
docker compose up -d
```

### 3. Check logs

```bash
docker compose logs -f
```

You should see lines like:
```
2026-04-07T12:00:00  INFO      MQTT connected to 192.168.1.10:1883
2026-04-07T12:00:00  INFO      MQTT Discovery configs published
2026-04-07T12:00:01  INFO      Published — tank=61.0°C  ambient=23.3°C  setpoint=50°C
```

Home Assistant will auto-discover the device under **Settings → Devices & Services → MQTT**.

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `MQTT_HOST` | `localhost` | MQTT broker hostname or IP |
| `MQTT_PORT` | `1883` | MQTT broker port |
| `MQTT_USER` | *(empty)* | MQTT username (leave empty if no auth) |
| `MQTT_PASS` | *(empty)* | MQTT password |
| `MQTT_CLIENT_ID` | `hot_water_bridge` | MQTT client identifier |
| `GIZWITS_TOKEN` | *(required)* | Gizwits user token |
| `GIZWITS_APP_ID` | *(required)* | Gizwits application ID |
| `GIZWITS_DID` | *(required)* | Gizwits device ID |
| `DATA_DIR` | `/data` | Path for state persistence (mount a volume here) |
| `TANK_VOLUME` | `200` | Tank capacity in litres — used for energy calculations |
| `COLD_WATER_TEMP` | `10` | Cold water reference temperature in °C for stored energy baseline |

---

## Home Assistant Entities

### Controls
| Entity | Type | Description |
|---|---|---|
| Hot Water Target temperature | Number | Setpoint sent to the device (0–75 °C) |
| Hot Water Heating mode | Select | Auto / Eco / Fast Heat / Sleep / Holiday |
| Hot Water Excess solar mode | Switch | Activates solar profile (temp + mode) |

### Configuration (shown in HA device Configuration section)
| Entity | Type | Description |
|---|---|---|
| Hot Water Baseline temperature | Number | Setpoint used when solar mode is OFF |
| Hot Water Excess solar temperature | Number | Setpoint used when solar mode is ON |
| Hot Water Baseline heating mode | Select | Heating mode used when solar mode is OFF |
| Hot Water Excess solar heating mode | Select | Heating mode used when solar mode is ON |

### Temperature Sensors
Ambient, upper tank, lower tank, coil, suction, exhaust, solar probe, reserved

### Diagnostic Sensors
Compressor frequency (Hz), compressor current (A), compressor voltage (V), electric heating current (A), expansion valve position, compressor runtime (min), run state

### Energy Sensors
| Entity | Description |
|---|---|
| Hot Water Tank energy stored | Thermal energy in the tank relative to cold-water reference (kWh) |
| Hot Water Heat generated today | Energy added to the tank since midnight — heat pump, element, solar (kWh) |
| Hot Water Heat used today | Energy drawn from the tank since midnight — showers, taps, losses (kWh) |
| Hot Water Heat generated (lifetime) | Cumulative energy added to the tank since first run (kWh, `total_increasing`) |
| Hot Water Heat used (lifetime) | Cumulative energy drawn from the tank since first run (kWh, `total_increasing`) |

Daily counters reset at midnight. All counters survive container restarts via the Docker volume. Lifetime totals never reset and can be added to HA's Energy Dashboard.

### Binary Sensors
Compressor running, electric heating active, fan running, circulation pump running, solar pump running, sterilisation active, antifreeze active, power state

---

## Excess Solar Mode

The solar mode switch is designed for use with a home energy management system or HA automation. When excess solar power is available:

1. Trigger the **Hot Water Excess solar mode** switch to ON.
2. The bridge immediately pushes the **solar temperature setpoint** and **solar heating mode** to the device, maximising heat pump use of free solar energy.
3. When the switch is turned OFF, the bridge reverts the device to the **baseline** setpoint and heating mode.

The four profile values (baseline/solar temp, baseline/solar mode) persist across container restarts in the Docker named volume `hot_water_data`.

---

## Running Without Docker

```bash
pip3 install paho-mqtt requests

export MQTT_HOST=192.168.1.10
export GIZWITS_TOKEN=your_token
export GIZWITS_APP_ID=your_app_id
export GIZWITS_DID=your_device_id

python3 dhw_mqtt.py
```

---

## Compatibility

Tested against devices using the **Gizwits EU API** (`euapi.gizwits.com`). If your device uses a different regional endpoint (e.g. `usapi.gizwits.com`), update `GIZWITS_BASE` at the top of `dhw_mqtt.py`. The same approach should work for any Gizwits-based heat pump water heater sold under the Outes, JNOD, or OEM brand names that uses the *eHomeMaster* app.

---

## License

MIT
