"""
MQTT Telemetry Publisher Module.
Simulates IoT sensor nodes by parsing historical records from data/CPE371_datalog.csv
and streaming standardized JSON telemetry events to an external MQTT broker.
"""

import argparse
import csv
from datetime import datetime
import json
import os
import sys
import time
from typing import Dict, Any, Generator, Optional
import uuid

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

# Default MQTT broker settings
DEFAULT_BROKER = "broker.mqttdashboard.com"
DEFAULT_PORT = 1883
DEFAULT_KEEPALIVE = 60
DEFAULT_QOS = 0

# Topic hierarchy constants
TOPIC_INDOOR_DEFAULT = "CPE371/house/indoor/sensor"
TOPIC_OUTDOOR_DEFAULT = "CPE371/house/outdoor/sensor"
DEFAULT_ROOM = "living"

DEFAULT_DATASET_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "CPE371_datalog.csv"
)

# House floorplan rooms mapping
INDOOR_ROOMS = ["living", "kitchen", "dining", "bedroom", "laundry"]


def format_indoor_event(
    row: Dict[str, Any],
    room: str = DEFAULT_ROOM,
    device_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Formats a CSV row into a standardized Indoor JSON telemetry event."""
    dev_id = device_id or ("sensor-indoor-01" if room == "living" else f"sensor-{room}-01")
    return {
        "event_id": str(uuid.uuid4()),
        "device_id": dev_id,
        "room": room,
        "location_type": "Indoor",
        "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "temperature": round(float(row.get("room_temperature", 0.0)), 2),
        "humidity": round(float(row.get("room_humidity", 0.0)), 2),
        "aqi": int(round(float(row.get("AQI", 0)))),
        "ac_status": int(row.get("AirCondition_Status", 0)),
        "latitude": round(float(row.get("location_lattitude", 0.0)), 6),
        "longitude": round(float(row.get("location_longitude", 0.0)), 6),
    }


def format_outdoor_event(
    row: Dict[str, Any],
    room: str = "terrace",
    device_id: str = "sensor-outdoor-01",
) -> Dict[str, Any]:
    """Formats a CSV row into a standardized Outdoor JSON telemetry event."""
    return {
        "event_id": str(uuid.uuid4()),
        "device_id": device_id,
        "room": room,
        "location_type": "Outdoor",
        "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "temperature": round(float(row.get("outdoor_temperature", 0.0)), 2),
        "humidity": round(float(row.get("outdoor_humidity", 0.0)), 2),
        "aqi": int(round(float(row.get("AQI", 0)))),
        "ac_status": 0,
        "latitude": round(float(row.get("location_lattitude", 0.0)), 6),
        "longitude": round(float(row.get("location_longitude", 0.0)), 6),
    }


def load_raw_dataset(filepath: str = DEFAULT_DATASET_PATH) -> Generator[Dict[str, Any], None, None]:
    """Reads raw telemetry records from CSV, handling UTF-8 BOM encoding."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Dataset file not found at: {filepath}")

    with open(filepath, mode="r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


class MQTTPublisher:
    """Manages connection lifecycle and streams sensor events to MQTT Broker."""

    def __init__(
        self,
        broker: str = DEFAULT_BROKER,
        port: int = DEFAULT_PORT,
        keepalive: int = DEFAULT_KEEPALIVE,
        qos: int = DEFAULT_QOS,
    ):
        self.broker = broker
        self.port = port
        self.keepalive = keepalive
        self.qos = qos
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
            print(f"[*] Connected successfully to MQTT broker ({self.broker}:{self.port}).")
        else:
            print(f"[!] Failed to connect to broker. Reason code: {reason_code}")

    def _on_disconnect(self, client, userdata, flags, reason_code=None, properties=None):
        self.connected = False
        print("[*] Disconnected from MQTT broker.")

    def connect(self, timeout: float = 10.0) -> bool:
        """Establishes connection to the broker and blocks until handshake completes."""
        print(f"[*] Connecting to MQTT broker at {self.broker}:{self.port} (keepalive={self.keepalive}s)...")
        self.client.connect(self.broker, self.port, keepalive=self.keepalive)
        self.client.loop_start()

        start_time = time.time()
        while not self.connected and (time.time() - start_time) < timeout:
            time.sleep(0.1)

        if not self.connected:
            print(f"[!] Connection timed out after {timeout} seconds.")
            return False
        return True

    def publish_event(self, topic: str, payload: Dict[str, Any]) -> bool:
        """Publishes a single JSON telemetry event to the designated topic."""
        message_str = json.dumps(payload, ensure_ascii=False)
        msg_info = self.client.publish(topic, message_str, qos=self.qos)
        msg_info.wait_for_publish(timeout=2.0)
        return msg_info.is_published()

    def stream_dataset(
        self,
        filepath: str = DEFAULT_DATASET_PATH,
        mode: str = "both",
        room: str = DEFAULT_ROOM,
        interval_seconds: float = 1.0,
        max_records: Optional[int] = None,
    ) -> int:
        """Streams dataset rows as telemetry events at configurable intervals."""
        if not self.connected and not self.connect():
            raise ConnectionError(f"Cannot connect to broker {self.broker}:{self.port}")

        count = 0
        print(f"[*] Streaming dataset from: {filepath}")
        print(f"[*] Mode: {mode.upper()} | Interval: {interval_seconds}s | QoS: {self.qos}")

        try:
            for row in load_raw_dataset(filepath):
                count += 1

                # 1. Publish Indoor Telemetry
                if mode in ("indoor", "both"):
                    indoor_payload = format_indoor_event(row, room=room)
                    indoor_topic = f"CPE371/house/{room}/sensor" if room != "indoor" else TOPIC_INDOOR_DEFAULT
                    self.publish_event(indoor_topic, indoor_payload)
                    print(
                        f"[{count}] Indoor -> {indoor_topic} | "
                        f"Temp={indoor_payload['temperature']}C, Hum={indoor_payload['humidity']}%, "
                        f"AQI={indoor_payload['aqi']}, AC={indoor_payload['ac_status']}"
                    )

                # 2. Publish Outdoor Telemetry
                if mode in ("outdoor", "both"):
                    outdoor_payload = format_outdoor_event(row, room="terrace")
                    self.publish_event(TOPIC_OUTDOOR_DEFAULT, outdoor_payload)
                    print(
                        f"[{count}] Outdoor -> {TOPIC_OUTDOOR_DEFAULT} | "
                        f"Temp={outdoor_payload['temperature']}C, Hum={outdoor_payload['humidity']}%"
                    )

                if max_records and count >= max_records:
                    print(f"[*] Reached specified maximum record limit ({max_records}).")
                    break

                time.sleep(interval_seconds)

        except KeyboardInterrupt:
            print("\n[!] Streaming interrupted by user (KeyboardInterrupt).")
        finally:
            self.close()

        print(f"[*] Total dataset rows streamed: {count}")
        return count

    def close(self):
        """Gracefully terminates network loop and disconnects client."""
        try:
            self.client.loop_stop()
            self.client.disconnect()
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser(
        description="CPE371 IoT Telemetry Publisher - Simulates sensor telemetry over MQTT from CSV dataset"
    )
    parser.add_argument("--broker", default=DEFAULT_BROKER, help=f"MQTT broker hostname (default: {DEFAULT_BROKER})")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"MQTT broker port (default: {DEFAULT_PORT})")
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Streaming interval between records in seconds (default: 1.0)",
    )
    parser.add_argument(
        "--mode",
        choices=["indoor", "outdoor", "both"],
        default="both",
        help="Telemetry mode to stream: indoor, outdoor, or both (default: both)",
    )
    parser.add_argument(
        "--room",
        default="living",
        choices=INDOOR_ROOMS + ["indoor"],
        help="Indoor room name for mapping (default: living)",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Maximum rows to stream (default: None - stream entire file)",
    )
    parser.add_argument(
        "--dataset",
        default=DEFAULT_DATASET_PATH,
        help=f"Path to datalog CSV file (default: {DEFAULT_DATASET_PATH})",
    )

    args = parser.parse_args()

    publisher = MQTTPublisher(broker=args.broker, port=args.port)
    publisher.stream_dataset(
        filepath=args.dataset,
        mode=args.mode,
        room=args.room,
        interval_seconds=args.interval,
        max_records=args.max_records,
    )


if __name__ == "__main__":
    main()