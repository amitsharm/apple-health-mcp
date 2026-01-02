"""
Health data ingestion endpoint.
Receives data from iOS Shortcuts and stores in Redis.
"""
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, unquote
from upstash_redis import Redis
from datetime import datetime, timezone, timedelta
import json
import os

API_KEY = os.environ.get("API_KEY", "")

redis = Redis(
    url=os.environ.get("UPSTASH_REDIS_REST_URL"),
    token=os.environ.get("UPSTASH_REDIS_REST_TOKEN")
)


def get_pacific_now():
    """Get current time in Pacific timezone (PST/PDT)."""
    pacific_tz = timezone(timedelta(hours=-8))  # PST
    return datetime.now(pacific_tz)


def check_auth(headers) -> bool:
    if not API_KEY:
        return True
    auth = headers.get("Authorization", "")
    return auth == f"Bearer {API_KEY}"


def parse_values(raw: str) -> list:
    """Parse newline-separated values from iOS Shortcuts."""
    decoded = unquote(raw).replace("\r\n", "\n").replace("\r", "\n")
    values = []
    for v in decoded.split("\n"):
        v = v.strip()
        if v:
            try:
                values.append(float(v))
            except ValueError:
                values.append(v)
    return values


def compute_hr_zones(values: list) -> dict:
    """
    Calculate time spent in each heart rate zone.
    Zones based on typical training thresholds.
    """
    nums = [v for v in values if isinstance(v, (int, float))]
    if not nums:
        return {}

    zones = {
        "rest": 0,      # < 100 bpm
        "light": 0,     # 100-120 bpm (yoga, walking)
        "moderate": 0,  # 120-140 bpm (strength, easy cardio)
        "hard": 0,      # 140-160 bpm (tempo, harder cardio)
        "max": 0        # 160+ bpm (intervals, sprints)
    }

    for hr in nums:
        if hr < 100:
            zones["rest"] += 1
        elif hr < 120:
            zones["light"] += 1
        elif hr < 140:
            zones["moderate"] += 1
        elif hr < 160:
            zones["hard"] += 1
        else:
            zones["max"] += 1

    total = len(nums)
    return {
        "zones": zones,
        "zone_pct": {k: round(v / total * 100) for k, v in zones.items()},
        "training_load": zones["moderate"] + zones["hard"] + zones["max"],
        "high_intensity": zones["hard"] + zones["max"]
    }


def compute_sleep_stats(values: list) -> dict:
    """Analyze sleep stage distribution."""
    stages = {"REM": 0, "Core": 0, "Deep": 0, "Awake": 0}
    for v in values:
        if isinstance(v, str):
            if "REM" in v:
                stages["REM"] += 1
            elif "Core" in v or "Light" in v:
                stages["Core"] += 1
            elif "Deep" in v:
                stages["Deep"] += 1
            elif "Awake" in v or "Wake" in v:
                stages["Awake"] += 1

    total = sum(stages.values())
    if total == 0:
        return {"values": values}

    fragmentation = round(stages["Awake"] / total * 100, 1)
    quality = "good" if fragmentation < 20 and stages["REM"] > 0 and stages["Deep"] > 0 else \
              "fair" if fragmentation < 35 else "poor"

    return {
        "stages": stages,
        "fragmentation_pct": fragmentation,
        "quality": quality,
        "has_rem": stages["REM"] > 0,
        "has_deep": stages["Deep"] > 0
    }


def compute_blood_pressure_stats(systolic_values: list, diastolic_values: list) -> dict:
    """
    Compute blood pressure statistics from paired systolic/diastolic readings.
    Threshold: systolic ≥140 OR diastolic ≥90 mmHg (traditional hypertension).
    """
    systolic_nums = [v for v in systolic_values if isinstance(v, (int, float))]
    diastolic_nums = [v for v in diastolic_values if isinstance(v, (int, float))]

    if not systolic_nums or not diastolic_nums:
        return {"count": 0}

    # Ensure equal number of readings (pair them)
    count = min(len(systolic_nums), len(diastolic_nums))
    systolic_nums = systolic_nums[:count]
    diastolic_nums = diastolic_nums[:count]

    # Count elevated readings
    elevated = sum(1 for s, d in zip(systolic_nums, diastolic_nums) if s >= 140 or d >= 90)

    return {
        "systolic_avg": round(sum(systolic_nums) / count, 1),
        "systolic_min": round(min(systolic_nums), 1),
        "systolic_max": round(max(systolic_nums), 1),
        "diastolic_avg": round(sum(diastolic_nums) / count, 1),
        "diastolic_min": round(min(diastolic_nums), 1),
        "diastolic_max": round(max(diastolic_nums), 1),
        "count": count,
        "elevated_readings": elevated
    }


def compute_blood_glucose_stats(values: list) -> dict:
    """
    Compute blood glucose statistics.
    Target range: 70-180 mg/dL (standard diabetes management).
    """
    nums = [v for v in values if isinstance(v, (int, float))]
    if not nums:
        return {"count": 0}

    count = len(nums)
    avg = sum(nums) / count

    # Calculate standard deviation
    variance = sum((x - avg) ** 2 for x in nums) / count
    std_dev = variance ** 0.5

    # Calculate percentage in target range (70-180 mg/dL)
    in_range = sum(1 for v in nums if 70 <= v <= 180)
    in_range_pct = round(in_range / count * 100, 1)

    return {
        "avg": round(avg, 1),
        "min": round(min(nums), 1),
        "max": round(max(nums), 1),
        "std_dev": round(std_dev, 1),
        "count": count,
        "in_range_pct": in_range_pct
    }


def compute_stats(values: list, key: str = "") -> dict:
    """Compute statistics for health samples."""
    key_lower = key.lower().strip()

    if key_lower == "sleep":
        return compute_sleep_stats(values)

    if key_lower == "bloodglucose":
        return compute_blood_glucose_stats(values)

    nums = [v for v in values if isinstance(v, (int, float))]
    if not nums:
        return {"count": len(values)}

    result = {
        "avg": round(sum(nums) / len(nums), 2),
        "min": round(min(nums), 2),
        "max": round(max(nums), 2),
        "count": len(nums)
    }

    # Add HR zones for heart rate data
    if key_lower == "heartrate":
        result["hr_zones"] = compute_hr_zones(nums)

    return result


class handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if not check_auth(self.headers):
            self.send_response(401)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "unauthorized"}).encode())
            return

        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length).decode("utf-8")

        form_data = parse_qs(body)
        date_key = get_pacific_now().strftime("%Y-%m-%d")
        redis_key = f"health:{date_key}"

        existing = redis.get(redis_key)
        health_data = json.loads(existing) if existing else {}

        # Check for paired blood pressure fields
        bp_systolic_key = None
        bp_diastolic_key = None
        for key in form_data.keys():
            key_lower = key.lower()
            if "bloodpressuresystolic" in key_lower:
                bp_systolic_key = key
            elif "bloodpressurediastolic" in key_lower:
                bp_diastolic_key = key

        # Process blood pressure as a paired metric
        if bp_systolic_key and bp_diastolic_key:
            systolic_raw = form_data[bp_systolic_key][0] if form_data[bp_systolic_key] else ""
            diastolic_raw = form_data[bp_diastolic_key][0] if form_data[bp_diastolic_key] else ""
            systolic_values = parse_values(systolic_raw)
            diastolic_values = parse_values(diastolic_raw)
            health_data["bloodpressure"] = compute_blood_pressure_stats(systolic_values, diastolic_values)

        # Process all other metrics normally
        for key, values in form_data.items():
            key_lower = key.lower()
            # Skip blood pressure fields since they're handled separately
            if "bloodpressuresystolic" in key_lower or "bloodpressurediastolic" in key_lower:
                continue

            raw = values[0] if values else ""
            parsed = parse_values(raw)
            health_data[key] = compute_stats(parsed, key)

        health_data["_updated"] = get_pacific_now().isoformat()
        redis.set(redis_key, json.dumps(health_data))

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({
            "ok": True,
            "date": date_key,
            "keys": list(form_data.keys())
        }).encode())

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps({
            "endpoint": "ingest",
            "method": "POST",
            "description": "Receives health data from iOS Shortcuts"
        }).encode())
