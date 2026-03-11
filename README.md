# Aranet MQTT Collector

A robust Python tool to collect environmental data from **Aranet sensors via MQTT**, store them in a **SQLite database**, and provide tools for **data export and visualization**.

The collector subscribes to MQTT topics published by an **Aranet base station** and automatically records all measurements in a structured database.

The design is **future-proof**: new detector types or new measurement variables require **no modification to the code**.

---

# Features

* MQTT ingestion from Aranet base stations
* Automatic sensor discovery
* SQLite database storage
* Long-format time series storage
* CSV export (long and wide formats)
* Built-in plotting utilities
* Robust handling of unknown future variables
* Metadata management (base station and sensors)

---

# Supported MQTT Topics

The collector supports the following topic structure:

```
Aranet/<base_id>/name
Aranet/<base_id>/sensors/<sensor_id>/name
Aranet/<base_id>/sensors/<sensor_id>/productNumber
Aranet/<base_id>/sensors/<sensor_id>/json/measurements
```

Example measurement payload:

```json
{
  "radon": "16",
  "atmosphericpressure": "300",
  "battery": "0.91",
  "temperature": "22.1",
  "humidity": "43.2",
  "rssi": "-99",
  "time": "1773219886"
}
```

All variables except `time` are automatically recorded.

Alarm messages are ignored:

```
Aranet/<base_id>/sensors/<sensor_id>/json/alarms
```

---

# Architecture

```
Aranet Sensors
      │
      ▼
Aranet Base Station
      │
      ▼
MQTT Broker (HiveMQ Cloud)
      │
      ▼
Aranet Collector
      │
      ▼
SQLite Database
      │
      ├── CSV Export
      └── Plotting
```

---

# Installation

## Requirements

* Python 3.10+
* pip

Install dependencies:

```
pip install -r requirements.txt
```

---

# Configuration

The collector uses **environment variables** for configuration.

Example configuration:

## Linux / macOS

```
export MQTT_HOST="your_cluster.s1.eu.hivemq.cloud"
export MQTT_PORT="8883"
export MQTT_USER="username"
export MQTT_PASSWORD="password"
export MQTT_TOPIC="Aranet/#"
export ARANET_DB="$HOME/aranet_data.sqlite"
```

## Windows PowerShell

```
$env:MQTT_HOST="your_cluster.s1.eu.hivemq.cloud"
$env:MQTT_PORT="8883"
$env:MQTT_USER="username"
$env:MQTT_PASSWORD="password"
$env:MQTT_TOPIC="Aranet/#"
$env:ARANET_DB="C:\data\aranet_data.sqlite"
```

---

# Running the Collector

Start the collector:

```
python aranet_collector.py run
```

The program will:

* connect to the MQTT broker
* subscribe to `Aranet/#`
* store incoming data in the SQLite database

Example output:

```
Connected to HiveMQ Cloud
Subscribed to Aranet/#
Measurements stored | sensor_ref=352406009362/40D246 | variables=6
```

---

# Listing Sensors

```
python aranet_collector.py list-sensors
```

Example output:

```
sensor_ref           base_id      sensor_id
352406009362/40D246 352406009362 40D246
```

---

# Listing Available Variables

```
python aranet_collector.py list-variables
```

For a specific sensor:

```
python aranet_collector.py list-variables --sensor-ref "352406009362/40D246"
```

---

# Exporting Data

## Long format

```
python aranet_collector.py export-long --output measurements_long.csv
```

Example:

| time       | sensor              | variable    | value |
| ---------- | ------------------- | ----------- | ----- |
| 2026-03-11 | 352406009362/40D246 | radon       | 16    |
| 2026-03-11 | 352406009362/40D246 | temperature | 22.1  |

---

## Wide format

```
python aranet_collector.py export-wide --output measurements_wide.csv
```

Example:

| time       | sensor              | radon | temperature | humidity |
| ---------- | ------------------- | ----- | ----------- | -------- |
| 2026-03-11 | 352406009362/40D246 | 16    | 22.1        | 43       |

---

# Plotting Data

Generate a plot for a variable:

```
python aranet_collector.py plot --sensor-ref "352406009362/40D246" --variable "radon"
```

Save the plot to a file:

```
python aranet_collector.py plot --sensor-ref "352406009362/40D246" --variable "radon" --output radon.png
```

---

# Database

All data are stored in a SQLite database.

Tables:

* `bases`
* `sensors`
* `messages`
* `measurements`

The **measurements table** stores data in long format:

| column           | description               |
| ---------------- | ------------------------- |
| sensor_ref       | unique sensor identifier  |
| variable         | measurement variable      |
| value_num        | numeric value             |
| payload_time_utc | timestamp from sensor     |
| received_at_utc  | time message was received |

---

# Design Principles

The collector is designed to:

* support unknown future sensor types
* store measurements without fixed schema
* ensure long-term robustness
* maintain reproducible scientific data

---

# Typical Workflow

Start acquisition:

```
python aranet_collector.py run
```

Inspect sensors:

```
python aranet_collector.py list-sensors
```

Export dataset:

```
python aranet_collector.py export-wide --output dataset.csv
```

Plot results:

```
python aranet_collector.py plot --sensor-ref "352406009362/40D246" --variable "radon"
```

---

# License

MIT License
