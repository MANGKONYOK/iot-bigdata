"""
MQTT Ingestion Bridge Module.
Subscribes to live IoT telemetry streams from an MQTT broker and persists each incoming
message as an atomic discrete JSON event file into the local landing directory (iot_landing/)
for ingestion by Apache Spark Structured Streaming.
"""

import argparse
from datetime import datetime
import json
import os
import sys
import time
from typing import Dict, Any, Optional
import uuid

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

# Default MQTT broker configuration
DEFAULT_BROKER = "broker.mqttdashboard.com"
DEFAULT_PORT = 1883
DEFAULT_KEEPALIVE = 60
DEFAULT_TOPIC = "CPE371/house/#"

DEFAULT_LANDING_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "iot_landing"
)


class MQTTBridge:
    """Subscribes to MQTT topics and buffers events atomically into iot_landing/."""

    def __init__(
        self,
        broker: str = DEFAULT_BROKER,
        port: int = DEFAULT_PORT,
        topic: str = DEFAULT_TOPIC,
        landing_dir: str = DEFAULT_LANDING_DIR,
        max_messages: Optional[int] = None,
    ):
        self.broker = broker
        self.port = port
        self.topic = topic
        self.landing_dir = os.path.abspath(landing_dir)
        self.max_messages = max_messages
        self.message_count = 0
        self.running = False

        os.makedirs(self.landing_dir, exist_ok=True)

        client_id = f"cpe371_bridge_{uuid.uuid4().hex[:8]}"
        try:
            self.client = mqtt.Client(CallbackAPIVersion.VERSION2, client_id=client_id)
        except AttributeError:
            self.client = mqtt.Client(client_id=client_id)

        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code == 0:
            print(f"[*] Connected successfully to MQTT Broker ({self.broker}:{self.port}).")
            client.subscribe(self.topic, qos=0)
            print(f"[*] Subscribed to topic hierarchy: '{self.topic}'")
        else:
            print(f"[!] Connection failed with return code: {reason_code}")

    def _on_disconnect(self, client, userdata, flags, reason_code=None, properties=None):
        print("[*] Disconnected from MQTT broker.")

    def _on_message(self, client, userdata, msg):
        """Processes and atomically writes each incoming message."""
        payload_str = msg.payload.decode("utf-8", errors="replace")

        # 1. Validate JSON payload structure
        try:
            data = json.loads(payload_str)
            if not isinstance(data, dict):
                print(f"[!] Warning: Payload from '{msg.topic}' is not a JSON object. Skipping.")
                return
        except json.JSONDecodeError as err:
            print(f"[!] Warning: Malformed JSON from '{msg.topic}': {err}. Skipping to avoid landing corrupt files.")
            return

        # 2. Extract event_id and attach ingestion metadata
        event_id = str(data.get("event_id") or uuid.uuid4())
        data["_topic"] = msg.topic
        data["_qos"] = msg.qos
        data["_received_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        data["_landed_at"] = time.time()

        # 3. Atomic File Renaming: write to .{event_id}.tmp then rename to {event_id}.json
        tmp_path = os.path.join(self.landing_dir, f".{event_id}.tmp")
        final_path = os.path.join(self.landing_dir, f"{event_id}.json")

        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, final_path)

            self.message_count += 1
            device = data.get("device_id", "unknown")
            temp = data.get("temperature", "N/A")
            print(f"[{self.message_count}] Landed -> {event_id}.json ({msg.topic} | Dev={device} | Temp={temp}C)")

        except Exception as exc:
            print(f"[!] Error writing atomic file for event {event_id}: {exc}")
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
            return

        # 4. Check if max messages reached
        if self.max_messages and self.message_count >= self.max_messages:
            print(f"[*] Reached max messages limit ({self.max_messages}). Stopping bridge...")
            self.stop()

    def start(self):
        """Starts the MQTT bridge network listener."""
        self.running = True
        print(f"[*] Starting MQTT Ingestion Bridge...")
        print(f"[*] Broker: {self.broker}:{self.port} | Topic: {self.topic}")
        print(f"[*] Landing Directory: {self.landing_dir}")

        self.client.connect(self.broker, self.port, keepalive=DEFAULT_KEEPALIVE)

        try:
            self.client.loop_forever()
        except KeyboardInterrupt:
            print("\n[!] Bridge interrupted by user (KeyboardInterrupt).")
        finally:
            self.stop()

    def stop(self):
        """Cleanly terminates listener loop and disconnects."""
        if self.running:
            self.running = False
            try:
                self.client.disconnect()
            except Exception:
                pass
            print(f"[*] Bridge stopped. Total messages landed: {self.message_count}")


def main():
    parser = argparse.ArgumentParser(
        description="CPE371 MQTT Ingestion Bridge - Buffers MQTT streams into landing zone as atomic JSON files"
    )
    parser.add_argument("--broker", default=DEFAULT_BROKER, help=f"MQTT broker hostname (default: {DEFAULT_BROKER})")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"MQTT broker port (default: {DEFAULT_PORT})")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help=f"MQTT topic to subscribe to (default: {DEFAULT_TOPIC})")
    parser.add_argument(
        "--landing-dir",
        default=DEFAULT_LANDING_DIR,
        help=f"Target landing directory (default: {DEFAULT_LANDING_DIR})",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Maximum messages to land before exiting (default: None - continuous)",
    )

    args = parser.parse_args()

    bridge = MQTTBridge(
        broker=args.broker,
        port=args.port,
        topic=args.topic,
        landing_dir=args.landing_dir,
        max_messages=args.max_messages,
    )
    bridge.start()


if __name__ == "__main__":
    main()