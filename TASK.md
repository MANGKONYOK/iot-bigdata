# CPE371 Big Data IoT & Spark Streaming - Team Task Board

## Current Status & Context
- **Issue #1 (Setup & Workspace)**: Initialized repository structure, environment configs, `.gitignore`, and dataset. (Merged to `dev`)
- **Issue #2 (MQTT Publisher)**: Developed `src/mqtt_publisher.py` streaming real-time JSON events to `broker.mqttdashboard.com`. (Merged to `dev`)
- **Issue #3 (MQTT Ingestion Bridge)**: Developed `src/mqtt_bridge.py` buffering messages with atomic file renaming into `iot_landing/`. (Merged to `dev`)

---

## Task Assignments

### 1. Sorawit: Issues #4 & #5 (Spark Stream Analytics Lead)
* **Branch:** `feat/spark-streaming`
* **Target File:** `src/spark_streaming.py`

#### Issue #4: PySpark Structured Streaming (10s Windowed Aggregations)
- [ ] Read continuous JSON micro-batches from `iot_landing/` using `spark.readStream`.
- [ ] Define explicit `StructType` matching publisher payload schema:
  - `event_id` (StringType), `device_id` (StringType), `room` (StringType), `location_type` (StringType)
  - `event_time` (StringType -> parse to TimestampType)
  - `temperature` (DoubleType), `humidity` (DoubleType), `aqi` (IntegerType), `ac_status` (IntegerType)
  - `latitude` (DoubleType), `longitude` (DoubleType)
  - `_topic` (StringType), `_received_at` (StringType), `_landed_at` (DoubleType)
- [ ] Perform spatial categorization:
  - **Indoor:** `living`, `kitchen`, `laundry`, `wine_cellar`, `gym`, `guest_bedroom`, `dining`, `bath`, `guest_bath`, `hall`
  - **Outdoor:** `terrace`, `patio`, `garage`
- [ ] Compute tumbling/sliding **10-second windowed averages** for:
  - Indoor Temperature & Humidity
  - Outdoor Temperature & Humidity
- [ ] Output streaming updates to console sink using `outputMode("update")` or `outputMode("complete")` with `trigger(processingTime="5 seconds")`.

#### Issue #5: Watermarking & Anomaly/Alert Threshold Logic
- [ ] Apply `.withWatermark("timestamp", "30 seconds")` (or 2 minutes) on event time to evict expired state from memory.
- [ ] Add calculated conditional column `status`:
  - `ALERT` when `avg_temperature > 35.0` (or `avg_humidity > 70.0`)
  - `OK` otherwise
- [ ] Ensure checkpoint directory is configured (e.g. `./checkpoints/spark_iot_metrics`).

---

### 2. Piti: Issues #6 & #7 (Cloud Forwarder & Actuator Control Lead)
* **Branch:** `feat/cloud-actuators`
* **Target Files:** `src/cloud_forwarder.py`, `src/actuator_logic.py`

#### Issue #6: Cloud Integration (dweet.cc & ThingSpeak - Assignment 1)
- [ ] **dweet.cc Forwarder:**
  - Send latest room/window metrics via HTTP POST to `https://dweet.cc/dweet/for/<thing_name>`.
  - Validate response via HTTP GET `https://dweet.cc/get/latest/dweet/for/<thing_name>`.
- [ ] **ThingSpeak Forwarder:**
  - Create a ThingSpeak IoT Channel with 4 main fields:
    - `Field 1`: Indoor Temperature
    - `Field 2`: Indoor Humidity
    - `Field 3`: Outdoor Temperature
    - `Field 4`: Outdoor Humidity
  - Send metrics via HTTP GET/POST to `https://api.thingspeak.com/update?api_key=<WRITE_KEY>&field1=...`.
  - Capture real-time chart dashboard screenshots for the report.

#### Issue #7: Smart Home Floorplan & Actuator Logic (Assignment 2)
- [ ] Implement feedback control rules in `src/actuator_logic.py` based on `CPE325HOUSE` floorplan:
  - **Air Conditioner / Cooler Control:** If `room == 'living'` and `temperature > 28°C` -> Turn AC `ON`.
  - **Wine Cellar Climate Control:** Keep `wine_cellar` temperature between `12°C - 18°C`.
  - **Air Purifier Control:** If `aqi > 100` (Unhealthy) -> Turn Purifier `ON / HIGH`.
  - **LED Alert Indicator:** If `status == 'ALERT'` -> Blink Alert LED.
- [ ] Draft an architecture placement diagram linking floorplan sensors to relays/actuators.

---

### 3. All: Issue #8 (Integration Testing & Team Lead)
* **Branch:** `feat/integration-e2e`

#### Issue #8: End-to-End Pipeline Verification
- [ ] Execute the full 4-stage streaming pipeline concurrently:
  1. Start Bridge: `python src/mqtt_bridge.py`
  2. Start Spark Streaming: `python src/spark_streaming.py`
  3. Start Publisher: `python src/mqtt_publisher.py --interval 0.5`
  4. Run Cloud Forwarder & Actuators: `python src/cloud_forwarder.py`
- [ ] Validate end-to-end latency, state memory stability, and error logs.
- [ ] Capture all terminal output screenshots and metric tables.

---

### 4. Joint Deliverable: Lab Report (`docs/Lab_Report.md`)
- [ ] **All Members**: Review and populate your corresponding sections in `docs/Lab_Report.md`.
- [ ] Add execution screenshots, system discussion, and conclusions.
- [ ] Export final PDF for submission.