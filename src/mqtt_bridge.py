"""
MQTT Bridge Module.
Subscribes to MQTT telemetry topics and lands data in iot_landing/ for Spark streaming ingestion.
"""

import json
import os
import sys
import time
import uuid
from typing import Any

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

# Use the reliable public broker from the lab notebooks
DEFAULT_BROKER = "broker.mqttdashboard.com"
DEFAULT_PORT = 1883
DEFAULT_TOPIC = "CPE_DEMO_HOUSE/#"
DEFAULT_LANDING_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "iot_landing"
)


class MQTTBridge:
    """Subscribes to MQTT topics and persists incoming events into the landing zone."""

    def __init__(
        self,
        broker: str = DEFAULT_BROKER,
        port: int = DEFAULT_PORT,
        topic: str = DEFAULT_TOPIC,
        landing_dir: str = DEFAULT_LANDING_DIR,
    ):
        self.broker = broker
        self.port = port
        self.topic = topic
        self.landing_dir = landing_dir
        os.makedirs(self.landing_dir, exist_ok=True)
        self.message_count = 0

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
            print(f"[*] Connected to MQTT Broker ({self.broker}:{self.port}) successfully.")
            client.subscribe(self.topic)
            print(f"[*] Subscribed to topic hierarchy: {self.topic}")
        else:
            print(f"[!] Connection failed with return code: {reason_code}")

    def _on_disconnect(self, client, userdata, flags, reason_code=None, properties=None):
        print("[*] Disconnected from MQTT broker.")

    def _on_message(self, client, userdata, msg):
        try:
            payload_str = msg.payload.decode("utf-8", errors="replace")

            # Support both structured JSON payloads and plain text messages
            try:
                data = json.loads(payload_str)
                if not isinstance(data, dict):
                    data = {"payload": data}
            except json.JSONDecodeError:
                data = {"raw_payload": payload_str}

            data["_topic"] = msg.topic
            data["_landed_at"] = time.time()

            self.message_count += 1

            # Atomic write: write to .tmp then rename to .json so Spark does not read partial files
            file_id = f"event_{int(time.time() * 1000)}_{self.message_count}"
            tmp_path = os.path.join(self.landing_dir, f"{file_id}.tmp")
            final_path = os.path.join(self.landing_dir, f"{file_id}.json")

            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f)
            os.replace(tmp_path, final_path)

            print(f"[{self.message_count}] Landed event from '{msg.topic}' -> {file_id}.json")
        except Exception as err:
            print(f"[!] Error processing message on topic '{msg.topic}': {err}")

    def start(self):
        """Starts the MQTT bridge listener loop."""
        print(f"[*] Connecting MQTT bridge to {self.broker}:{self.port}...")
        self.client.connect(self.broker, self.port, keepalive=60)

        try:
            self.client.loop_forever()
        except KeyboardInterrupt:
            print("\n[!] Bridge interrupted by user.")
        finally:
            self.client.disconnect()
            print(f"[*] Bridge stopped. Total messages landed: {self.message_count}")


if __name__ == "__main__":
    bridge = MQTTBridge()
    bridge.start()