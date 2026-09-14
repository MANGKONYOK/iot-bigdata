"""
Actuator Logic Module (Issue #7 - Assignment 2)

Evaluates stream telemetry against the CPE_HOUSE feedback-control rules and emits
actuator commands on the Assignment 2 topic hierarchy:

    /CPE_HOUSE/<ROOM>/temperature
    /CPE_HOUSE/<ROOM>/humidity
    /CPE_HOUSE/<ROOM>/LED/status     - reported LED state
    /CPE_HOUSE/<ROOM>/LED/set        - commanded LED state

Consumes the normalized event schema produced by mqtt_publisher.py and aggregated
by spark_streaming.py (room / temperature / humidity / aqi / status), NOT raw CSV
column names.
"""

from typing import Any, Dict, List, Optional

TOPIC_ROOT = "CPE_HOUSE"

# Node placement read off the CPE_HOUSE floorplan (attachments/Assignment/assignment2.png).
# "sensor" = blue Temperature/Humidity node, "led" = yellow LED controller node.
ROOM_NODES: Dict[str, Dict[str, bool]] = {
    "laundry":       {"sensor": True,  "led": True},
    "living":        {"sensor": True,  "led": True},
    "kitchen":       {"sensor": True,  "led": True},
    "terrace":       {"sensor": True,  "led": True},
    "gym":           {"sensor": True,  "led": True},
    "dining":        {"sensor": False, "led": True},
    "garage":        {"sensor": False, "led": True},
    "bath":          {"sensor": False, "led": True},
    "terrace_pool":  {"sensor": False, "led": True},
    "patio":         {"sensor": False, "led": True},
    # Issue #7 requires a wine-cellar climate rule, but the floorplan shows no sensor
    # node there. Declared so the rule has somewhere to act once one is fitted.
    "wine_cellar":   {"sensor": False, "led": False},
}

# Rule thresholds (Issue #7)
LIVING_AC_THRESHOLD = 28.0
WINE_CELLAR_MIN = 12.0
WINE_CELLAR_MAX = 18.0
AQI_UNHEALTHY = 100.0


def topic_for(room: str, leaf: str) -> str:
    """Builds a CPE_HOUSE topic, e.g. topic_for('living', 'LED/set')."""
    return f"/{TOPIC_ROOT}/{room.upper()}/{leaf}"


def _decision(device: str, action: str, reason: str, room: Optional[str] = None) -> Dict[str, Any]:
    decision = {"device": device, "action": action, "reason": reason}
    if room:
        decision["room"] = room
        decision["topic"] = topic_for(room, "LED/set")
    return decision


def evaluate_air_conditioner(room: str, temperature: float,
                             threshold: float = LIVING_AC_THRESHOLD) -> Optional[Dict[str, Any]]:
    """Living-room cooling: AC ON above the threshold (Issue #7)."""
    if room != "living":
        return None
    if temperature > threshold:
        return _decision("AirConditioner", "POWER_ON",
                         f"Living room {temperature:.1f}C exceeds {threshold:.1f}C", room)
    return _decision("AirConditioner", "POWER_OFF",
                     f"Living room {temperature:.1f}C at or below {threshold:.1f}C", room)


def evaluate_wine_cellar(room: str, temperature: float,
                         low: float = WINE_CELLAR_MIN,
                         high: float = WINE_CELLAR_MAX) -> Optional[Dict[str, Any]]:
    """Wine cellar climate control: hold between 12C and 18C (Issue #7)."""
    if room != "wine_cellar":
        return None
    if temperature > high:
        return _decision("WineCellarChiller", "COOL",
                         f"Cellar {temperature:.1f}C above {high:.1f}C", room)
    if temperature < low:
        return _decision("WineCellarHeater", "HEAT",
                         f"Cellar {temperature:.1f}C below {low:.1f}C", room)
    return _decision("WineCellarClimate", "MAINTAIN",
                     f"Cellar {temperature:.1f}C within {low:.1f}-{high:.1f}C", room)


def evaluate_air_purifier(aqi: float, threshold: float = AQI_UNHEALTHY) -> Dict[str, Any]:
    """Air purifier: HIGH when AQI is Unhealthy (> 100) (Issue #7)."""
    if aqi > threshold:
        return _decision("AirPurifier", "FAN_HIGH",
                         f"AQI {aqi:.0f} is Unhealthy (> {threshold:.0f})")
    return _decision("AirPurifier", "FAN_AUTO",
                     f"AQI {aqi:.0f} within acceptable range")


def evaluate_alert_led(status: str, room: Optional[str] = None) -> Dict[str, Any]:
    """Alert LED: blink while the Spark window status is ALERT (Issue #7)."""
    if str(status).upper() == "ALERT":
        return _decision("AlertLED", "BLINK", "Window status is ALERT", room)
    return _decision("AlertLED", "OFF", f"Window status is {status}", room)


def process_telemetry_actuation(telemetry: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Evaluates every applicable rule for one telemetry record.

    Rules are room-scoped, so a record only triggers the rules relevant to its room.
    """
    room = str(telemetry.get("room", "")).lower()
    actions: List[Dict[str, Any]] = []

    temperature = telemetry.get("temperature")
    if temperature is not None:
        for rule in (evaluate_air_conditioner, evaluate_wine_cellar):
            decision = rule(room, float(temperature))
            if decision:
                actions.append(decision)

    if telemetry.get("aqi") is not None:
        actions.append(evaluate_air_purifier(float(telemetry["aqi"])))

    if telemetry.get("status") is not None:
        actions.append(evaluate_alert_led(telemetry["status"], room or None))

    return actions


def publish_decisions(decisions: List[Dict[str, Any]], client: Any, qos: int = 0) -> int:
    """
    Publishes each decision to its room's /LED/set topic, closing the feedback loop
    between Spark analytics and the Assignment 2 actuator topics.
    """
    published = 0
    for decision in decisions:
        topic = decision.get("topic")
        if not topic:
            continue
        payload = "ON" if decision["action"] in ("POWER_ON", "FAN_HIGH", "BLINK", "HEAT", "COOL") else "OFF"
        client.publish(topic, payload, qos=qos)
        published += 1
    return published


if __name__ == "__main__":
    samples = [
        {"room": "living", "temperature": 29.5, "humidity": 60.0, "aqi": 120, "status": "ALERT"},
        {"room": "living", "temperature": 26.0, "humidity": 55.0, "aqi": 80, "status": "OK"},
        {"room": "wine_cellar", "temperature": 19.5, "status": "OK"},
        {"room": "wine_cellar", "temperature": 11.0, "status": "OK"},
        {"room": "wine_cellar", "temperature": 15.0, "status": "OK"},
    ]
    for sample in samples:
        print(f"\n--- {sample['room']} @ {sample.get('temperature')}C "
              f"aqi={sample.get('aqi')} status={sample.get('status')} ---")
        for decision in process_telemetry_actuation(sample):
            target = decision.get("topic", "(no LED topic)")
            print(f"  [{decision['device']:18s}] {decision['action']:10s} -> {target}")
            print(f"     {decision['reason']}")
