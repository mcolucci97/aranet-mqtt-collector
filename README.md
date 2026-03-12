# Aranet Environmental Monitoring Platform

### CEA / LNHB -- RadonNET Testbed

A Python-based platform to collect, store, and visualize environmental
data from *Aranet wireless sensors*.

The system ingests measurements via *MQTT*, stores them in a *Supabase
PostgreSQL database*, and provides an interactive *Streamlit dashboard*
for real-time monitoring and historical analysis.

The platform is currently deployed as a *testbed in the CEA/LNHB
building* within the framework of the *RadonNET project*, supporting the
development of distributed environmental monitoring networks.

Monitored quantities include:

-   Radon concentration
-   Temperature
-   Humidity
-   Atmospheric pressure
-   Particulate matter (PM1, PM2.5, PM10)
-   Battery level
-   Signal strength (RSSI)

------------------------------------------------------------------------

# System Architecture

    Aranet Sensors
          │
          ▼
    Aranet Base Station
          │
          ▼
    MQTT Broker (HiveMQ Cloud)
          │
          ▼
    MQTT Collector (Python)
          │
          ▼
    Supabase PostgreSQL Database
          │
          ▼
    Streamlit Dashboard

The system supports *real-time acquisition*, *historical data
exploration*, and *data export*.

------------------------------------------------------------------------

# Repository Structure

    .
    ├── app_cloud.py
    ├── aranet_collector.py
    ├── cloud_aranet_collector.py
    ├── cea_logo.png
    ├── radonnet_logo.png
    ├── requirements.txt
    ├── README.md
    ├── LICENSE
    └── .gitignore

## Main Components

  -------------------------------------------------------------------------------
  File                        Description
  --------------------------- ---------------------------------------------------
  app_cloud.py                Streamlit dashboard connected to Supabase

  aranet_collector.py         MQTT collector designed for *local execution*

  cloud_aranet_collector.py   MQTT collector designed for *cloud deployment*

  requirements.txt            Python dependencies

  cea_logo.png                CEA logo used in dashboard

  radonnet_logo.png           RadonNET project logo
  -------------------------------------------------------------------------------

------------------------------------------------------------------------

# Dashboard

The *Streamlit dashboard* provides:

-   real-time sensor visualization
-   comparison between multiple sensors
-   historical time-series plots
-   CSV export of filtered data
-   automatic unit handling
-   responsive layout

The dashboard is designed for *environmental monitoring in laboratory
environments* and is used as part of the *RadonNET testbed
infrastructure at CEA/LNHB*.

## Run the dashboard locally

    streamlit run app_cloud.py

The dashboard reads data directly from *Supabase*.

Required credentials must be stored in:

    .streamlit/secrets.toml

Example configuration:

    SUPABASE_URL = "https://your-project.supabase.co"
    SUPABASE_KEY = "your-anon-key"

------------------------------------------------------------------------

# MQTT Data Collector

The repository contains *two collector implementations*.

Both subscribe to Aranet MQTT topics and store measurements in the
database.

------------------------------------------------------------------------

# Local Collector

File:

    aranet_collector.py

This version is intended for *local laboratory deployment*.

Features:

-   MQTT ingestion
-   automatic sensor discovery
-   metadata management
-   structured measurement storage
-   CSV export
-   plotting utilities

This version can operate with a *local SQLite database*.

## Start the collector

    python aranet_collector.py run

------------------------------------------------------------------------

# Cloud Collector

File:

    cloud_aranet_collector.py

This version is designed for *cloud infrastructure*.

It stores all measurements in *Supabase PostgreSQL*.

Typical deployment environments:

-   cloud servers
-   Docker containers
-   background workers
-   hosted Python services

------------------------------------------------------------------------

# Supported MQTT Topics

The collectors support the following topic structure.

    Aranet/<base_id>/name
    Aranet/<base_id>/sensors/<sensor_id>/name
    Aranet/<base_id>/sensors/<sensor_id>/productNumber
    Aranet/<base_id>/sensors/<sensor_id>/json/measurements

Example measurement payload:

    {
      "radon": "16",
      "atmosphericpressure": "300",
      "battery": "0.91",
      "temperature": "22.1",
      "humidity": "43.2",
      "rssi": "-99",
      "time": "1773219886"
    }

All variables except *time* are automatically recorded.

Alarm messages are ignored:

    Aranet/<base_id>/sensors/<sensor_id>/json/alarms

------------------------------------------------------------------------

# Database Structure

Measurements are stored in *Supabase PostgreSQL*.

## bases

Stores base station metadata.

    base_id
    base_name
    updated_at

## sensors

Stores sensor metadata.

    sensor_ref
    base_id
    sensor_id
    sensor_name
    product_number
    updated_at

## measurements

Stores measurements in *long format*.

    sensor_ref
    variable
    value_num
    value_text
    payload_time_utc
    received_at_utc
    unit
    raw_json

Indexes optimize queries on:

    sensor_ref
    variable
    payload_time_utc

------------------------------------------------------------------------

# Installation

## Install dependencies

    pip install -r requirements.txt

Recommended Python version:

    Python 3.12

------------------------------------------------------------------------

# MQTT Configuration

Collectors require the following environment variables.

Linux / macOS

    export MQTT_HOST="your_cluster.s1.eu.hivemq.cloud"
    export MQTT_PORT="8883"
    export MQTT_USER="username"
    export MQTT_PASSWORD="password"
    export MQTT_TOPIC="Aranet/#"

Windows PowerShell

    $env:MQTT_HOST="your_cluster.s1.eu.hivemq.cloud"
    $env:MQTT_PORT="8883"
    $env:MQTT_USER="username"
    $env:MQTT_PASSWORD="password"
    $env:MQTT_TOPIC="Aranet/#"

------------------------------------------------------------------------

# Supabase Configuration

For the cloud collector and dashboard:

    SUPABASE_URL=https://your-project.supabase.co
    SUPABASE_KEY=your-anon-key
    SUPABASE_SERVICE_ROLE_KEY=your-service-role-key

------------------------------------------------------------------------

# Features

-   real-time MQTT ingestion
-   automatic sensor discovery
-   flexible measurement storage
-   cloud database integration
-   interactive Streamlit dashboard
-   multi-sensor comparison
-   CSV data export
-   scientific formatting of measurements
-   robust handling of new measurement variables

------------------------------------------------------------------------

# Design Principles

The platform is designed to:

-   support unknown future sensor types
-   store measurements without fixed schema
-   ensure long-term robustness
-   maintain reproducible scientific datasets
-   support distributed environmental monitoring networks

------------------------------------------------------------------------

# Project Context

This platform supports activities related to:

*CEA -- Laboratoire National Henri Becquerel (LNHB)*

Environmental monitoring and radon measurement research.

The system is deployed as a *testbed for distributed environmental
monitoring instrumentation* within the *RadonNET project*, enabling the
evaluation of sensor networks for:

-   radon monitoring
-   aerosol measurements
-   indoor environmental characterization
-   instrumentation research and development

------------------------------------------------------------------------

# License

MIT License.
