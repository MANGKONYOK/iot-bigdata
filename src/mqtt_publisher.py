"""
MQTT Publisher Module.
Simulates IoT sensors by reading telemetry data from CSV and publishing to an MQTT broker.
"""

import csv
import json
import os
import sys
import time
import uuid
from typing import Dict, Any, Generator, Optional

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

# Use the reliable public broker from the lab notebooks
DEFAULT_BROKER = "broker.mqttdashboard.com"
DEFAULT_PORT = 1883
DEFAULT_TOPIC = "CPE_DEMO_HOUSE/room1"
DEFAULT_DATASET_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "CPE371_datalog.csv"
)


def load_telemetry_records(filepath: str = DEFAULT_DATASET_PATH) -> Generator[Dict[str, Any], None, None]:
    """Reads telemetry records from the CSV dataset file (with UTF-8 BOM handling)."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Dataset not found at {filepath}")

    with open(filepath, mode="r", encoding="utf-8-sig") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            yield {
                "room_temperature": float(row.get("room_temperature", 0.0)),
                "room_humidity": float(row.get("room_humidity", 0.0)),
                "outdoor_temperature": float(row.get("outdoor_temperature", 0.0)),
                "outdoor_humidity": float(row.get("outdoor_humidity", 0.0)),
                "location_lattitude": float(row.get("location_lattitude", 0.0)),
                "location_longitude": float(row.get("location_longitude", 0.0)),
                "AQI": float(row.get("AQI", 0.0)),
                "AirCondition_Status": int(row.get("AirCondition_Status", 0)),
                "timestamp": time.time(),
            }


class MQTTPublisher:
    """Manages MQTT connection and streaming publication."""

    def __init__(self, broker: str = DEFAULT_BROKER, port: int = DEFAULT_PORT):
        self.broker = broker
        self.port = port
        self.connected = False
        client_id = f"cpe371_pub_{uuid.uuid4().hex[:8]}"

        try:
            self.client = mqtt.Client(CallbackAPIVersion.VERSION2, client_id=client_id)
        except AttributeError:
            self.client = mqtt.Client(client_id=client_id)

        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            self.connected = True
            print(f"[*] Connected to MQTT broker ({self.broker}:{self.port}) successfully.")
        else:
            print(f"[!] Failed to connect to broker. Return code: {reason_code}")

    def _on_disconnect(self, client, userdata, flags, reason_code=None, properties=None):
        self.connected = False
        print("[*] Disconnected from MQTT broker.")

    def connect(self, timeout: float = 10.0) -> bool:
        """Connects to the MQTT broker and waits for acknowledgment."""
        print(f"[*] Connecting to broker {self.broker}:{self.port}...")
        self.client.connect(self.broker, self.port, keepalive=60)
        self.client.loop_start()

        start = time.time()
        while not self.connected and (time.time() - start) < timeout:
            time.sleep(0.1)

        if not self.connected:
            print(f"[!] Connection timeout after {timeout} seconds.")
            return False
        return True

    def publish_stream(
        self,
        topic: str = DEFAULT_TOPIC,
        filepath: str = DEFAULT_DATASET_PATH,
        interval_seconds: float = 1.0,
        max_records: Optional[int] = None,
    ) -> int:
        """Streams records from dataset file to MQTT topic."""
        if not self.connected and not self.connect():
            raise ConnectionError(f"Could not connect to MQTT broker at {self.broker}:{self.port}")

        count = 0
        try:
            print(f"[*] Starting telemetry stream to topic '{topic}' (interval: {interval_seconds}s)...")
            for record in load_telemetry_records(filepath):
                # Update timestamp to real-time epoch
                record["timestamp"] = time.time()
                payload = json.dumps(record)

                msg_info = self.client.publish(topic, payload, qos=0)
                msg_info.wait_for_publish(timeout=2.0)

                count += 1
                print(f"[{count}] Published: Temp={record['room_temperature']}C, AQI={record['AQI']}")

                if max_records and count >= max_records:
                    print(f"[*] Reached max records limit ({max_records}).")
                    break

                time.sleep(interval_seconds)
        except KeyboardInterrupt:
            print("\n[!] Publication stopped by user.")
        finally:
            self.close()

        return count

    def close(self):
        """Cleanly stops network loop and disconnects."""
        self.client.loop_stop()
        self.client.disconnect()


def publish_telemetry_stream(
    broker: str = DEFAULT_BROKER,
    port: int = DEFAULT_PORT,
    topic: str = DEFAULT_TOPIC,
    interval_seconds: float = 1.0,
    max_records: Optional[int] = None,
) -> None:
    """Convenience function to publish telemetry."""
    publisher = MQTTPublisher(broker=broker, port=port)
    publisher.publish_stream(
        topic=topic,
        interval_seconds=interval_seconds,
        max_records=max_records,
    )


if __name__ == "__main__":
    # By default, publish 5 records for testing, or set max_records=None to stream continuously
    publish_telemetry_stream(max_records=5)