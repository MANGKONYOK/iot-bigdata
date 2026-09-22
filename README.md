# CPE371 Big Data Engineering - IoT & Spark Stream Processing Lab

MQTT telemetry from a sensor datalog, landed as JSON, aggregated by PySpark Structured
Streaming into 10-second windows, forwarded to the cloud and fed back to smart-home actuators.

## Overview

| # | section | short detail | status |
| - | ------- | ------------ | ------ |
| 1 | [Team](#1-team) | three members, one pipeline stage each | done |
| 2 | [Pipeline](#2-pipeline) | four stages from CSV to actuator command | done |
| 3 | [Requirements](#3-requirements) | verified runtime versions, not guesses | done |
| 4 | [Setup](#4-setup) | JDK, virtual environment, dependencies | done |
| 5 | [Running the pipeline](#5-running-the-pipeline) | start the four stages in order | done |
| 6 | [Modules](#6-modules) | what each source file does and its flags | done |
| 7 | [Thresholds and rules](#7-thresholds-and-rules) | every tunable constant in one place | done |
| 8 | [Measured results](#8-measured-results) | numbers from a real end-to-end run | done |
| 9 | [Issue status](#9-issue-status) | eight issues, current state | current |
| 10 | [Fixes applied](#10-fixes-applied) | six defects found by running the code | done |
| 11 | [Layout](#11-layout) | directory tree | done |

---

## 1. Team

| member | ID | area |
| ------ | -- | ---- |
| Sorawit Chaithong | 67070503442 | Spark Structured Streaming and analytics |
| Kittiphat Noikate | 67070503459 | Data ingestion and MQTT pipeline |
| Piti Srisongkram | 67070503467 | Cloud integration and actuators |

## 2. Pipeline

| # | stage | module | in | out |
| - | ----- | ------ | -- | --- |
| 1 | Publish | `mqtt_publisher.py` | `data/CPE371_datalog.csv` | MQTT topics on `broker.mqttdashboard.com` |
| 2 | Land | `mqtt_bridge.py` | MQTT subscription | one JSON file per event in `iot_landing/` |
| 3 | Aggregate | `spark_streaming.py` | `iot_landing/` | 10-second windowed metrics + alert status |
| 4 | Act | `cloud_forwarder.py`, `actuator_logic.py` | landed events | dweet.cc, ThingSpeak, LED commands |

The dataset holds 1088 rows. Each row produces one indoor and one outdoor event, so a full
replay emits 2176 events.

Stage 3 answers the lab activity directly — four windowed averages:

| metric | source |
| ------ | ------ |
| Indoor temperature | `avg(temperature)` where `spatial_zone = Indoor` |
| Indoor humidity | `avg(humidity)` where `spatial_zone = Indoor` |
| Outdoor temperature | `avg(temperature)` where `spatial_zone = Outdoor` |
| Outdoor humidity | `avg(humidity)` where `spatial_zone = Outdoor` |

## 3. Requirements

Versions below were run, not assumed.

| component | version | note |
| --------- | ------- | ---- |
| Python | 3.14.6 | PySpark 4.2 lists 3.10–3.14 as supported |
| Java | Temurin 17.0.20.1 | Spark 4.x requires JDK 17 or 21 |
| PySpark | 4.2.0 | pulls its own Spark distribution |
| paho-mqtt | 2.1.0 | 2.x needs an explicit `CallbackAPIVersion` |
| requests | 2.34.2 | HTTP to dweet.cc and ThingSpeak |

Java is the only hard prerequisite that pip cannot install. A JDK tarball extracted into
your home directory is enough — no system install, no admin rights.

## 4. Setup

1. Install a JDK 17 and point `JAVA_HOME` at it.

   ```bash
   export JAVA_HOME=~/jdk/jdk-17.0.20.1+1/Contents/Home   # macOS tarball layout
   export PATH="$JAVA_HOME/bin:$PATH"
   java -version                                          # expect 17.x
   ```

2. Create the virtual environment and install dependencies.

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate          # Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. Set the ThingSpeak write key, only if you have a channel.

   ```bash
   export THINGSPEAK_WRITE_KEY=your_write_key
   ```

   Unset, stage 4 still forwards to dweet.cc and skips ThingSpeak with a warning. The key is
   read from the environment and never stored in the repository.

## 5. Running the pipeline

Four stages, four terminals, started in this order. Stages 1 and 2 are long-running.

| # | terminal | command | wait for |
| - | -------- | ------- | -------- |
| 1 | bridge | `python src/mqtt_bridge.py` | `Subscribed to topic hierarchy` |
| 2 | spark | `python src/spark_streaming.py` | `Streaming query active` |
| 3 | publisher | `python src/mqtt_publisher.py --interval 0.5 --max-records 40 --mode both` | publisher exits |
| 4 | cloud | `python src/cloud_forwarder.py --count 1 --actuate` | forwarder exits |

Notes:

1. Start the bridge before the publisher, or the first events are published to nobody.
2. Spark needs roughly 15 seconds to start its session before it reads anything.
3. Delete `checkpoints/` after changing the aggregation, since a checkpoint stores the previous
   query's state schema.
4. Add `--publish-commands` to stage 4 to send actuator decisions back over MQTT.

## 6. Modules

| module | lines | does | key flags |
| ------ | ----- | ---- | --------- |
| `mqtt_publisher.py` | 262 | replays the CSV as indoor/outdoor JSON events over MQTT | `--interval --max-records --mode --room --broker` |
| `mqtt_bridge.py` | 180 | subscribes, enriches, writes each event atomically to the landing zone | `--topic --landing-dir --max-messages` |
| `spark_streaming.py` | 335 | windowed aggregation, watermarking, alert status, console sink | `--window --watermark --trigger --output-mode --by-room` |
| `cloud_forwarder.py` | 285 | derives the four metrics, posts to dweet.cc and ThingSpeak, runs actuators | `--thing-name --count --interval --actuate --publish-commands` |
| `actuator_logic.py` | 158 | evaluates the four control rules and emits LED commands | library, `python src/actuator_logic.py` for a demo |

The bridge writes `.{event_id}.tmp` then renames to `{event_id}.json`. The leading dot keeps
Spark's file source from reading a half-written file.

Actuator commands use the topic scheme the assignment specifies:

```
/CPE_HOUSE/<ROOM>/temperature
/CPE_HOUSE/<ROOM>/humidity
/CPE_HOUSE/<ROOM>/LED/status      reported state
/CPE_HOUSE/<ROOM>/LED/set         commanded state
```

## 7. Thresholds and rules

Stream alerts, in `spark_streaming.py`:

| constant | value | effect |
| -------- | ----- | ------ |
| `DEFAULT_TEMP_ALERT_THRESHOLD` | 35.0 °C | window status becomes `ALERT` above this |
| `DEFAULT_HUM_ALERT_THRESHOLD` | 70.0 % | window status becomes `ALERT` above this |
| window / slide | 10 s | tumbling windows |
| watermark | 30 s | evicts closed-window state, drops later arrivals |

Actuator rules, in `actuator_logic.py`:

| rule | condition | action |
| ---- | --------- | ------ |
| Air conditioner | `room == living` and temperature > 28.0 °C | `POWER_ON` |
| Wine cellar | temperature outside 12.0–18.0 °C | `COOL` or `HEAT` |
| Air purifier | AQI > 100 | `FAN_HIGH` |
| Alert LED | window status is `ALERT` | `BLINK` |

ThingSpeak's free tier refuses updates less than 15 seconds apart and answers `0` when it does,
so `cloud_forwarder.py` enforces that floor.

## 8. Measured results

One run, 40 records at 0.5 s, all four stages concurrent.

| metric | value | metric | value |
| ------ | ----- | ------ | ----- |
| Events published | 80 | Indoor average temperature | 26.29 °C |
| Events landed | 80 | Indoor average humidity | 34.14 % |
| Delivery rate | 100.0 % | Outdoor average temperature | 46.05 °C |
| Spark micro-batches | 8 | Outdoor average humidity | 28.29 % |
| Latency median | 1.06 s | Latency p95 | 1.44 s |

Windowed state held at 2–4 rows per batch across the run, so the watermark is evicting closed
windows rather than accumulating them. No stage log contained an error or traceback.

## 9. Issue status

| # | issue | owner | status |
| - | ----- | ----- | ------ |
| 1 | Setup & workspace | Kittiphat | done |
| 2 | MQTT publisher | Kittiphat | done |
| 3 | MQTT ingestion bridge | Kittiphat | done |
| 4 | PySpark structured streaming, 10 s windows | Sorawit | done |
| 5 | Watermarking & alert thresholds | Sorawit | done |
| 6 | Cloud integration, dweet.cc & ThingSpeak | Piti | done, ThingSpeak dashboard pending |
| 7 | Floorplan & actuator logic | Piti | done, placement diagram pending |
| 8 | End-to-end verification | all | done |

Outstanding work:

1. Create the ThingSpeak channel, populate fields 1–8 and capture the dashboard screenshot.
2. Draw the sensor-to-actuator placement diagram for issue 7.
3. Fill sections 4 and 5 of `docs/Lab_Report.md`; the table in section 8 above covers section 4.

## 10. Fixes applied

Six defects, all found by running code that was already marked complete.

| # | issue | defect | effect if unfixed | status |
| - | ----- | ------ | ----------------- | ------ |
| 1 | 4 | JSON reader missing `multiLine` | 850 rows parsed, every field `NULL` | fixed |
| 2 | 4 | aggregation grouped by room | the four required metrics never produced | fixed |
| 3 | 4 | `bedroom` absent from the indoor room list | those events classified `Unspecified` | fixed |
| 4 | 5 | `to_timestamp` under Spark 4 ANSI mode | one malformed event aborted the whole query | fixed |
| 5 | 6 | dweet.cc posted as JSON instead of form data | every post failed while reporting success | fixed |
| 6 | 7 | placeholder thresholds, no room awareness | every control rule fired incorrectly | fixed |

Defect 4 is the subtle one. Spark 4 enables `spark.sql.ansi.enabled` by default, where
`to_timestamp` raises instead of returning `NULL`. That killed the stream on a single bad
message and left the `coalesce` fallback to `_received_at` permanently unreachable.
`try_to_timestamp` restores the intended behaviour.

## 11. Layout

```text
Lab5
├── README.md                     this file
├── requirements.txt              dependency manifest
├── data/
│   └── CPE371_datalog.csv        1088 rows of sensor telemetry
├── iot_landing/                  runtime landing zone, one JSON file per event
├── notebooks/
│   ├── play_dweet.ipynb          dweet.cc HTTP example
│   ├── play_mqtt_publisher.ipynb MQTT publish example
│   └── play_mqtt_subscriber.ipynb MQTT subscribe example
├── src/
│   ├── mqtt_publisher.py         stage 1, CSV to MQTT
│   ├── mqtt_bridge.py            stage 2, MQTT to landing zone
│   ├── spark_streaming.py        stage 3, windowed aggregation and alerts
│   ├── cloud_forwarder.py        stage 4, dweet.cc and ThingSpeak
│   └── actuator_logic.py         stage 4, control rules
└── docs/
    ├── architecture.png          system architecture diagram
    └── Lab_Report.md             lab report
```
