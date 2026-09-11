"""
Cloud Forwarder Module
Forwards aggregated stream metrics to cloud platforms (dweet.cc and ThingSpeak).
"""

import json
import time
from typing import Dict, Any

try:
    import requests
except ImportError:
    requests = None

DWEET_BASE_URL = "https://dweet.cc/dweet/for"
THINGSPEAK_BASE_URL = "https://api.thingspeak.com/update"


def send_to_dweet(thing_name: str, payload: Dict[str, Any], timeout: float = 5.0) -> bool:
    """Posts telemetry data to dweet.cc."""
    if requests is None:
        raise RuntimeError("requests is not installed. Run `pip install -r requirements.txt`")

    url = f"{DWEET_BASE_URL}/{thing_name}"
    try:
        response = requests.post(url, json=payload, timeout=timeout)
        if response.status_code == 200:
            print(f"[+] Dweet posted to '{thing_name}': {payload}")
            return True
        else:
            print(f"[!] Dweet failed with status {response.status_code}: {response.text}")
            return False
    except Exception as exc:
        print(f"[!] Dweet request error: {exc}")
        return False


def send_to_thingspeak(api_key: str, fields: Dict[str, Any], timeout: float = 5.0) -> bool:
    """Posts telemetry fields (field1..field8) to ThingSpeak channel."""
    if requests is None:
        raise RuntimeError("requests is not installed. Run `pip install -r requirements.txt`")

    params = {"api_key": api_key, **fields}
    try:
        response = requests.get(THINGSPEAK_BASE_URL, params=params, timeout=timeout)
        if response.status_code == 200 and response.text != "0":
            print(f"[+] ThingSpeak updated successfully (entry id: {response.text})")
            return True
        else:
            print(f"[!] ThingSpeak update failed: status {response.status_code}, response: {response.text}")
            return False
    except Exception as exc:
        print(f"[!] ThingSpeak request error: {exc}")
        return False


if __name__ == "__main__":
    demo_thing = "cpe371-demo-device"
    demo_data = {
        "room_temperature": 25.5,
        "room_humidity": 50.0,
        "AQI": 42.0,
        "timestamp": time.time(),
    }
    print(f"[*] Testing cloud forwarding to dweet.cc for thing '{demo_thing}'...")
    send_to_dweet(demo_thing, demo_data)