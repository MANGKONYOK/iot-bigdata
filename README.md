# CPE371 Big Data Engineering - IoT & Spark Stream Processing Lab

## Team Members
- **Member 1**: Sorawit Chaithong (ID: 67070503442) - Spark Structured Streaming & Analytics
- **Member 2**: Kittiphat Noikate (ID: 67070503459) - Data Ingestion & MQTT Pipeline
- **Member 3**: Piti Srisongkram (ID: 67070503467) - Cloud Integration, Actuators

---

## Project Overview

This project implements an e2e Big Data IoT telemetry and stream processing pipeline:
1. **Data Ingestion & Simulation**: Telemetry from `data/CPE371_datalog.csv` is published over MQTT.
2. **MQTT Bridge**: Telemetry messages are subscribed to and landed in `iot_landing/` as micro-batch JSON chunks.
3. **Stream Processing**: PySpark Structured Streaming ingests landed data, performing windowed aggregations, anomaly detection, and schema validation.
4. **Cloud & Actuation**: Processed insights are forwarded to cloud platforms (dweet.cc / ThingSpeak) and trigger smart home actuators (such as HVAC / air conditioner controls).

---

## Environment Setup

### 1. Prerequisites
- **Python**: Version 3.9 - 3.11 (or 3.12 with compatible wheel builds)
- **Java**: OpenJDK 11 or 17 LTS (e.g., Eclipse Temurin or Amazon Corretto 17)
  > [!WARNING]
  > **Java 21+ / Java 25 Compatibility Issue**: PySpark runs on the JVM and often fails with `IllegalAccessError` or `UnsupportedOperationException` on Java 21+ (and Java 25) due to Java Module System encapsulation (JEP 403). **Java 11 or 17 is strongly recommended.**

  - Verify your active Java version:
    ```bash
    java -version
    ```

  - Configure `JAVA_HOME` if multiple Java versions are installed:
    - **Windows (Command Prompt)**:
      ```cmd
      set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-17...
      set PATH=%JAVA_HOME%\bin;%PATH%
      ```
    - **Windows (PowerShell)**:
      ```powershell
      $env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17..."
      $env:Path = "$env:JAVA_HOME\bin;$env:Path"
      ```
    - **macOS / Linux**:
      ```bash
      export JAVA_HOME=/path/to/jdk-17
      export PATH=$JAVA_HOME/bin:$PATH
      ```

### 2. Virtual Environment Setup
```bash
# Clone the repository
git clone https://github.com/MANGKONYOK/iot-bigdata.git
cd iot-bigdata

# Create and activate virtual environment
python -m venv .venv

# On macOS/Linux:
source .venv/bin/activate

# On Windows (Command Prompt):
.venv\Scripts\activate.bat

# On Windows (PowerShell):
.venv\Scripts\Activate.ps1

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt
```

### 3. Gitignore Sanity Check
Verify that `.gitignore` is operating properly and runtime landing events are ignored:
```bash
# On macOS / Linux / Git Bash:
touch iot_landing/test_event.json
git status

# On Windows PowerShell:
New-Item -ItemType File -Path iot_landing/test_event.json
git status

# Expected output: test_event.json MUST NOT appear under "Untracked files"

# Clean up test file:
# Bash: rm iot_landing/test_event.json
# PowerShell: Remove-Item iot_landing/test_event.json
```

---

## Repository Directory Structure

```text
IoT
├── attachments/                       # Original lab assignments and instructional materials
├── .gitignore                         # Git exclusion rules for artifacts, environments, and logs
├── README.md                          # Project documentation and setup instructions
├── requirements.txt                   # Project dependency manifest
├── data/
│   └── CPE371_datalog.csv             # Raw sensor datalog dataset
├── iot_landing/
│   └── .gitkeep                       # Landing directory for streaming micro-batches
├── notebooks/
│   ├── play_dweet.ipynb               # Cloud forwarding and dweet exploration
│   ├── play_mqtt_publisher.ipynb      # Interactive MQTT publisher prototyping
│   └── play_mqtt_subscriber.ipynb     # Interactive MQTT subscriber prototyping
├── src/
│   ├── __init__.py                    # Source package initializer
│   ├── mqtt_publisher.py              # Sensor simulation & MQTT publish service
│   ├── mqtt_bridge.py                 # MQTT consumer & landing zone writer
│   ├── spark_streaming.py             # PySpark Structured Streaming pipeline
│   ├── cloud_forwarder.py             # Cloud integration (dweet.cc / ThingSpeak)
│   └── actuator_logic.py              # Actuator decision & control logic
└── docs/
    ├── architecture.png               # System architecture diagram
    └── Lab_Report.md                  # Comprehensive lab report and results
```