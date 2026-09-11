"""
Actuator Logic Module
Evaluates stream telemetry against operational rules and triggers smart actuator commands.
"""

from typing import Dict, Any


def evaluate_air_conditioner(room_temp: float, target_temp: float = 25.0, hysteresis: float = 1.0) -> Dict[str, Any]:
    """
    Determines whether the Air Conditioner should be turned ON, OFF, or remain in state.
    """
    if room_temp > target_temp + hysteresis:
        return {
            "device": "AirConditioner",
            "action": "POWER_ON",
            "reason": f"Room temperature ({room_temp:.1f} C) exceeds upper threshold ({target_temp + hysteresis:.1f} C)",
        }
    elif room_temp < target_temp - hysteresis:
        return {
            "device": "AirConditioner",
            "action": "POWER_OFF",
            "reason": f"Room temperature ({room_temp:.1f} C) dropped below lower threshold ({target_temp - hysteresis:.1f} C)",
        }
    else:
        return {
            "device": "AirConditioner",
            "action": "MAINTAIN",
            "reason": f"Room temperature ({room_temp:.1f} C) is within comfortable range",
        }


def evaluate_air_purifier(aqi: float, threshold: float = 50.0) -> Dict[str, Any]:
    """
    Determines whether the Air Purifier should be engaged based on AQI.
    """
    if aqi > threshold:
        return {
            "device": "AirPurifier",
            "action": "FAN_HIGH",
            "reason": f"AQI ({aqi:.1f}) exceeds acceptable threshold ({threshold})",
        }
    return {
        "device": "AirPurifier",
        "action": "FAN_AUTO",
        "reason": f"AQI ({aqi:.1f}) is within normal parameters",
    }


def process_telemetry_actuation(telemetry: Dict[str, Any]) -> list:
    """Evaluates all actuator rules for a given telemetry record."""
    actions = []
    if "room_temperature" in telemetry:
        actions.append(evaluate_air_conditioner(float(telemetry["room_temperature"])))
    if "AQI" in telemetry:
        actions.append(evaluate_air_purifier(float(telemetry["AQI"])))
    return actions


if __name__ == "__main__":
    sample_event = {
        "room_temperature": 27.5,
        "room_humidity": 65.0,
        "AQI": 75.0,
    }
    decisions = process_telemetry_actuation(sample_event)
    print("Actuator decisions for sample telemetry:")
    for decision in decisions:
        print(f" - [{decision['device']}] -> {decision['action']}: {decision['reason']}")