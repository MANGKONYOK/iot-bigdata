"""
Cloud Forwarder Module (Issue #6 - Assignment 1)

Forwards aggregated IoT stream metrics to cloud platforms:
  - dweet.cc    : JSON storage endpoint (POST) with read-back validation (GET)
  - ThingSpeak  : channel fields for real-time dashboard visualisation

Reads the landing zone written by mqtt_bridge.py so it runs independently of
Spark; point read_latest_metrics() at the Spark sink to consume aggregates.
"""

import argparse
import glob
import json
import os
import time
from typing import Any, Dict, List, Optional

from actuator_logic import process_telemetry_actuation, publish_decisions

try:
    import requests
except ImportError:
    requests = None

DWEET_POST_URL = "https://dweet.cc/dweet/for"
DWEET_GET_URL = "https://dweet.cc/get/latest/dweet/for"
THINGSPEAK_BASE_URL = "https://api.thingspeak.com/update"

# ThingSpeak free tier rejects updates issued less than 15 s apart.
THINGSPEAK_MIN_INTERVAL = 15.0

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_LANDING_DIR = os.path.join(ROOT_DIR, "iot_landing")
DEFAULT_THING_NAME = "cpe371-67070503467"


def _require_requests() -> None:
    if requests is None:
        raise RuntimeError("requests is not installed. Run `pip install -r requirements.txt`")


def send_to_dweet(thing_name: str, payload: Dict[str, Any], timeout: float = 10.0) -> bool:
    """
    Posts telemetry to dweet.cc.

    Must use form encoding (data=), not json=. dweet.cc answers a JSON body with
    HTTP 200 even when it rejects the request, so the body's "this" field - not
    the status code - is what decides success.
    """
    _require_requests()
    url = f"{DWEET_POST_URL}/{thing_name}"
    try:
        response = requests.post(url, data=payload, timeout=timeout)
        body = response.json()
    except Exception as exc:
        print(f"[!] Dweet request error: {exc}")
        return False

    if response.status_code == 200 and body.get("this") == "succeeded":
        print(f"[+] Dweet posted to '{thing_name}': {len(payload)} fields")
        return True

    print(f"[!] Dweet failed: {body.get('this')} - {body.get('because', response.text[:120])}")
    return False


def verify_dweet(thing_name: str, timeout: float = 10.0) -> Optional[Dict[str, Any]]:
    """Reads back the most recent dweet and returns its content (Issue #6 validation step)."""
    _require_requests()
    url = f"{DWEET_GET_URL}/{thing_name}"
    try:
        response = requests.get(url, timeout=timeout)
        body = response.json()
    except Exception as exc:
        print(f"[!] Dweet read-back error: {exc}")
        return None

    if body.get("this") != "succeeded":
        print(f"[!] Dweet read-back failed: {body.get('because', 'unknown reason')}")
        return None

    content = body["with"][0]["content"]
    print(f"[+] Dweet read-back OK ({len(content)} fields): {content}")
    return content


def send_to_thingspeak(api_key: str, fields: Dict[str, Any], timeout: float = 10.0) -> bool:
    """
    Posts channel fields to ThingSpeak.

    A response body of "0" means the update was refused - almost always because
    the free tier's 15 s rate limit was breached.
    """
    _require_requests()
    if not api_key:
        print("[!] ThingSpeak skipped: THINGSPEAK_WRITE_KEY is not set")
        return False

    params = {"api_key": api_key, **fields}
    try:
        response = requests.get(THINGSPEAK_BASE_URL, params=params, timeout=timeout)
    except Exception as exc:
        print(f"[!] ThingSpeak request error: {exc}")
        return False

    if response.status_code == 200 and response.text.strip() != "0":
        print(f"[+] ThingSpeak updated (entry id: {response.text.strip()})")
        return True

    print(f"[!] ThingSpeak refused the update (response '{response.text.strip()}') - "
          f"check the write key and the 15 s rate limit")
    return False


# Two payload shapes reach dweet.cc: raw datalog rows posted by the Assignment 1
# notebook, and aggregated metrics posted by forward_once(). Accept either.
DWEET_FIELD_ALIASES = {
    "field1": ("room_temperature", "indoor_temperature"),
    "field2": ("room_humidity", "indoor_humidity"),
    "field3": ("outdoor_temperature",),
    "field4": ("outdoor_humidity",),
    "field5": ("AQI", "aqi"),
    "field6": ("AirCondition_Status", "ac_status"),
    "field7": ("location_lattitude", "latitude"),
    "field8": ("location_longitude", "longitude"),
}


def dweet_content_to_fields(content: Dict[str, Any]) -> Dict[str, Any]:
    """
    Maps a dweet.cc content payload onto ThingSpeak fields.

    dweet returns every value as a string, so values pass through as-is and
    ThingSpeak parses them.
    """
    fields = {}
    for field, aliases in DWEET_FIELD_ALIASES.items():
        for alias in aliases:
            if content.get(alias) is not None:
                fields[field] = content[alias]
                break
    return fields


def forward_dweet_to_thingspeak(thing_name: str, api_key: str) -> bool:
    """
    Assignment 1 step 3: read what is stored at dweet.cc and chart it in ThingSpeak.

    The dashboard is therefore fed from the dweet store, not from a second pass over
    the CSV.
    """
    content = verify_dweet(thing_name)
    if not content:
        return False

    fields = dweet_content_to_fields(content)
    if not fields:
        print(f"[!] Latest dweet has no recognised datalog fields: {sorted(content)}")
        return False

    print(f"[*] Forwarding {len(fields)} fields from dweet.cc -> ThingSpeak")
    return send_to_thingspeak(api_key, fields)


def read_latest_metrics(landing_dir: str = DEFAULT_LANDING_DIR, sample: int = 40) -> Dict[str, Any]:
    """
    Derives the four headline metrics from the most recent landed events.

    Events carry location_type ("Indoor"/"Outdoor") set by mqtt_publisher, so the
    split needs no room lookup table.
    """
    paths = sorted(glob.glob(os.path.join(landing_dir, "*.json")), key=os.path.getmtime)[-sample:]
    buckets: Dict[str, Dict[str, List[float]]] = {
        "Indoor": {"t": [], "h": []},
        "Outdoor": {"t": [], "h": []},
    }
    aqi: List[float] = []
    ac_status: Optional[int] = None

    for path in paths:
        try:
            with open(path, encoding="utf-8") as handle:
                event = json.load(handle)
        except (OSError, json.JSONDecodeError):
            continue  # partially written or malformed file - skip it

        zone = event.get("location_type")
        if zone in buckets:
            if event.get("temperature") is not None:
                buckets[zone]["t"].append(float(event["temperature"]))
            if event.get("humidity") is not None:
                buckets[zone]["h"].append(float(event["humidity"]))
        if event.get("aqi") is not None:
            aqi.append(float(event["aqi"]))
        if event.get("ac_status") is not None:
            ac_status = int(event["ac_status"])

    def mean(values: List[float]) -> Optional[float]:
        return round(sum(values) / len(values), 2) if values else None

    return {
        "indoor_temperature": mean(buckets["Indoor"]["t"]),
        "indoor_humidity": mean(buckets["Indoor"]["h"]),
        "outdoor_temperature": mean(buckets["Outdoor"]["t"]),
        "outdoor_humidity": mean(buckets["Outdoor"]["h"]),
        "aqi": mean(aqi),
        "ac_status": ac_status,
        "sample_size": len(paths),
    }


def build_thingspeak_fields(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Maps metrics onto ThingSpeak channel fields.

    field1-field4 are the four required by Issue #6; field5-field6 satisfy the
    assignment's "all fields is preferred". Unset metrics are omitted so
    ThingSpeak keeps the previous value rather than recording a blank.
    """
    mapping = {
        "field1": metrics.get("indoor_temperature"),
        "field2": metrics.get("indoor_humidity"),
        "field3": metrics.get("outdoor_temperature"),
        "field4": metrics.get("outdoor_humidity"),
        "field5": metrics.get("aqi"),
        "field6": metrics.get("ac_status"),
    }
    return {key: value for key, value in mapping.items() if value is not None}


def run_actuators(landing_dir: str, sample: int = 10, mqtt_client: Any = None) -> List[Dict[str, Any]]:
    """
    Applies the Issue #7 actuator rules to the most recent landed events and,
    when an MQTT client is supplied, publishes each command to its LED/set topic.

    This is the seam Issue #8 calls "Cloud Forwarder & Actuators": it joins the
    cloud module to the rule module over the same landing zone.
    """
    paths = sorted(glob.glob(os.path.join(landing_dir, "*.json")), key=os.path.getmtime)[-sample:]
    decisions: List[Dict[str, Any]] = []

    for path in paths:
        try:
            with open(path, encoding="utf-8") as handle:
                event = json.load(handle)
        except (OSError, json.JSONDecodeError):
            continue
        decisions.extend(process_telemetry_actuation(event))

    if decisions:
        unique = {(d["device"], d["action"]) for d in decisions}
        print(f"[*] Actuators: {len(decisions)} decisions, {len(unique)} distinct commands")
        for device, action in sorted(unique):
            print(f"      {device:20s} -> {action}")
        if mqtt_client is not None:
            print(f"[+] Published {publish_decisions(decisions, mqtt_client)} commands to /CPE_HOUSE/*/LED/set")

    return decisions


def forward_once(thing_name: str, landing_dir: str, api_key: str,
                 actuate: bool = False, mqtt_client: Any = None) -> Dict[str, Any]:
    """Reads current metrics and pushes them to both cloud platforms."""
    metrics = read_latest_metrics(landing_dir)
    if metrics["sample_size"] == 0:
        print(f"[!] No events found in {landing_dir} - run mqtt_bridge.py and mqtt_publisher.py first")
        return metrics

    print(f"[*] Metrics over last {metrics['sample_size']} events: "
          f"indoor {metrics['indoor_temperature']}C/{metrics['indoor_humidity']}% | "
          f"outdoor {metrics['outdoor_temperature']}C/{metrics['outdoor_humidity']}%")

    payload = {key: value for key, value in metrics.items()
               if key != "sample_size" and value is not None}
    send_to_dweet(thing_name, payload)
    verify_dweet(thing_name)
    send_to_thingspeak(api_key, build_thingspeak_fields(metrics))

    if actuate:
        run_actuators(landing_dir, mqtt_client=mqtt_client)

    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(description="Forward IoT metrics to dweet.cc and ThingSpeak")
    parser.add_argument("--thing-name", default=DEFAULT_THING_NAME, help="dweet.cc thing name")
    parser.add_argument("--landing-dir", default=DEFAULT_LANDING_DIR, help="Landing zone to read")
    parser.add_argument("--interval", type=float, default=THINGSPEAK_MIN_INTERVAL,
                        help=f"Seconds between forwards (min {THINGSPEAK_MIN_INTERVAL} for ThingSpeak)")
    parser.add_argument("--count", type=int, default=1, help="Number of forwards (0 = run forever)")
    parser.add_argument("--from-dweet", action="store_true",
                        help="Feed ThingSpeak from the latest dweet.cc record (Assignment 1 step 3)")
    parser.add_argument("--actuate", action="store_true",
                        help="Also evaluate the Issue #7 actuator rules on recent events")
    parser.add_argument("--publish-commands", action="store_true",
                        help="With --actuate, publish commands to /CPE_HOUSE/<ROOM>/LED/set over MQTT")
    parser.add_argument("--broker", default="broker.mqttdashboard.com", help="MQTT broker for commands")
    args = parser.parse_args()

    api_key = os.environ.get("THINGSPEAK_WRITE_KEY", "")
    if not api_key:
        print("[!] THINGSPEAK_WRITE_KEY not set - dweet.cc only. "
              "Set it with: export THINGSPEAK_WRITE_KEY=your_key")

    mqtt_client = None
    if args.publish_commands:
        import paho.mqtt.client as mqtt
        try:
            mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        except AttributeError:
            mqtt_client = mqtt.Client()
        mqtt_client.connect(args.broker, 1883, 60)
        mqtt_client.loop_start()
        print(f"[*] Actuator commands will be published to {args.broker}")

    interval = max(args.interval, THINGSPEAK_MIN_INTERVAL) if api_key else args.interval
    sent = 0
    try:
        while args.count == 0 or sent < args.count:
            print(f"\n--- forward #{sent + 1} ---")
            if args.from_dweet:
                forward_dweet_to_thingspeak(args.thing_name, api_key)
            else:
                forward_once(args.thing_name, args.landing_dir, api_key,
                             actuate=args.actuate, mqtt_client=mqtt_client)
            sent += 1
            if args.count == 0 or sent < args.count:
                time.sleep(interval)
    except KeyboardInterrupt:
        print("\n[!] Stopped by user.")
    finally:
        if mqtt_client is not None:
            mqtt_client.loop_stop()
            mqtt_client.disconnect()


if __name__ == "__main__":
    main()
