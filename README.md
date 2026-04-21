# MQTT-STORM

A Java-based Apache Storm topology for consuming MQTT sensor data and storing processed readings in PostgreSQL.

## Overview

`MQTT-STORM` is a streaming data project that connects MQTT-based sensor ingestion with Apache Storm processing and PostgreSQL storage. It subscribes to incoming sensor messages, parses JSON payloads, extracts selected sensor values, writes a CSV log, and inserts structured readings into a PostgreSQL table.

This project is designed to demonstrate:
- MQTT-based sensor data ingestion
- stream processing with Apache Storm
- JSON message parsing and mapping
- PostgreSQL persistence for sensor data

## Features

- MQTT subscription using Apache Storm
- Custom message and tuple mappers
- JSON sensor payload parsing
- CSV export of selected sensor values
- PostgreSQL insertion for structured records
- Maven-based Java project setup

## Tech Stack

- Java
- Apache Storm
- MQTT
- PostgreSQL
- Maven
- Gson
- OpenCSV

## Project Structure

```text
src/main/java/
├── Bolts/
│   └── MqttBolt1.java
├── Connection/
│   ├── InsertDataPostgre.java
│   └── PostgreConnection.java
├── Mapper/
│   ├── CustomMessageMapper.java
│   └── CustomTupleMapper.java
└── Topology/
    └── MqttTopology.java

pom.xml
README.md
```

## How It Works

The topology:
1. subscribes to the `sensordata` MQTT topic
2. maps MQTT payloads into Storm tuples
3. parses JSON sensor messages inside the bolt
4. extracts selected sensor readings
5. writes CSV output for logging
6. inserts normalized sensor records into PostgreSQL

## Notes

This repository is an early streaming systems project and contains hardcoded connection placeholders and environment-specific settings that should be updated before production use.

Examples include:
- MQTT credentials
- SSH tunnel configuration
- PostgreSQL connection settings
- file output paths

## License

MIT
