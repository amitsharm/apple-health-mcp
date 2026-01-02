"""
MCP Server for health data.
Exposes health metrics to Claude via Model Context Protocol.
"""
from http.server import BaseHTTPRequestHandler
from upstash_redis import Redis
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, parse_qs
import json
import os

MCP_SECRET = os.environ.get("MCP_SECRET", "")

# Optional: User's typical weekly exercise routine (context for Claude, not math)
# Format: "strength:4,yoga:7,meditation:7,cardio:2" (days per week)
# If not set, Claude relies purely on HR zone data
EXERCISE_DAYS_PER_WEEK = os.environ.get("EXERCISE_DAYS_PER_WEEK", "")

redis = Redis(
    url=os.environ.get("UPSTASH_REDIS_REST_URL"),
    token=os.environ.get("UPSTASH_REDIS_REST_TOKEN")
)


def get_pacific_now():
    """Get current time in Pacific timezone (PST/PDT)."""
    pacific_tz = timezone(timedelta(hours=-8))  # PST
    return datetime.now(pacific_tz)


def check_secret(path: str) -> bool:
    if not MCP_SECRET:
        return True
    query = parse_qs(urlparse(path).query)
    return query.get("key", [""])[0] == MCP_SECRET


def parse_exercise_routine() -> dict:
    """Parse exercise routine from env var."""
    if not EXERCISE_DAYS_PER_WEEK:
        return {}
    routine = {}
    for item in EXERCISE_DAYS_PER_WEEK.split(","):
        if ":" in item:
            k, v = item.split(":", 1)
            try:
                routine[k.strip()] = int(v.strip())
            except ValueError:
                pass
    return routine


def get_health_data(date_key: str) -> dict:
    data = redis.get(f"health:{date_key}")
    return json.loads(data) if data else {}


def get_hrv_baseline(days: int = 14) -> dict:
    """Calculate HRV baseline from recent history."""
    hrv_values = []
    for i in range(1, days + 1):
        date = (get_pacific_now() - timedelta(days=i)).strftime("%Y-%m-%d")
        data = get_health_data(date)
        if data and "hrv" in data and data["hrv"].get("avg"):
            hrv_values.append(data["hrv"]["avg"])
    if not hrv_values:
        return {"baseline": None, "days": 0}
    return {
        "baseline": round(sum(hrv_values) / len(hrv_values), 1),
        "days": len(hrv_values)
    }


# MCP Tools

def tool_get_today() -> str:
    """Get all raw health metrics for today."""
    date_key = get_pacific_now().strftime("%Y-%m-%d")
    data = get_health_data(date_key)
    if not data:
        return json.dumps({"error": "No data synced today. Run iOS shortcuts."})
    return json.dumps(data, indent=2)


def tool_get_trends(days: int = 7) -> str:
    """Get raw health trends over multiple days."""
    results = {}
    for i in range(days):
        date = (get_pacific_now() - timedelta(days=i)).strftime("%Y-%m-%d")
        data = get_health_data(date)
        if data:
            day_data = {
                "hrv": data.get("hrv", {}).get("avg"),
                "resting_hr": data.get("heartRate", {}).get("min"),
                "exercise_min": data.get("exercise", {}).get("count", 0),
                "steps": data.get("steps", {}).get("count", 0)
            }
            # Include HR zones if available
            if "heartRate" in data and "hr_zones" in data["heartRate"]:
                day_data["hr_zones"] = data["heartRate"]["hr_zones"].get("zone_pct")
            # Include sleep data
            if "sleep" in data:
                day_data["sleep"] = {
                    "fragmentation_pct": data["sleep"].get("fragmentation_pct"),
                    "has_deep": data["sleep"].get("has_deep"),
                    "has_rem": data["sleep"].get("has_rem")
                }
            # Include blood pressure if available
            if "bloodpressure" in data:
                day_data["blood_pressure"] = {
                    "systolic_avg": data["bloodpressure"].get("systolic_avg"),
                    "diastolic_avg": data["bloodpressure"].get("diastolic_avg"),
                    "elevated_count": data["bloodpressure"].get("elevated_readings", 0)
                }
            # Include blood glucose if available
            if "bloodglucose" in data:
                day_data["blood_glucose"] = {
                    "avg": data["bloodglucose"].get("avg"),
                    "in_range_pct": data["bloodglucose"].get("in_range_pct")
                }
            results[date] = day_data
    if not results:
        return json.dumps({"error": f"No data for last {days} days."})
    return json.dumps(results, indent=2)


def get_day_summary(data: dict) -> dict:
    """Extract minimal summary for a day's data."""
    if not data:
        return None
    summary = {}
    if "hrv" in data and data["hrv"].get("avg"):
        summary["hrv"] = round(data["hrv"]["avg"], 1)
    if "heartRate" in data and "hr_zones" in data["heartRate"]:
        summary["hr_zones"] = data["heartRate"]["hr_zones"].get("zone_pct")
    if "exercise" in data:
        summary["exercise_min"] = data["exercise"].get("count", 0)
    return summary if summary else None


def tool_get_recovery_status() -> str:
    """Return raw health data for LLM reasoning. No pre-computed recommendations."""
    date_key = get_pacific_now().strftime("%Y-%m-%d")
    data = get_health_data(date_key)
    baseline = get_hrv_baseline()

    status = {
        "date": date_key,
        "weekly_routine": parse_exercise_routine() or None
    }

    # HRV - raw numbers only
    if "hrv" in data and data["hrv"].get("avg"):
        hrv = data["hrv"]["avg"]
        status["hrv"] = {
            "today": round(hrv, 1),
            "baseline": baseline.get("baseline"),
            "baseline_days": baseline.get("days", 0),
            "vs_baseline_pct": round(((hrv - baseline["baseline"]) / baseline["baseline"]) * 100) if baseline.get("baseline") else None
        }

    # Resting HR
    if "heartRate" in data:
        hr = data["heartRate"]
        status["resting_hr"] = hr.get("min")
        if "hr_zones" in hr:
            status["hr_zones"] = hr["hr_zones"].get("zone_pct")

    # Sleep - raw data
    if "sleep" in data:
        status["sleep"] = {
            "stages": data["sleep"].get("stages"),
            "fragmentation_pct": data["sleep"].get("fragmentation_pct"),
            "has_deep": data["sleep"].get("has_deep"),
            "has_rem": data["sleep"].get("has_rem")
        }

    # Exercise minutes
    if "exercise" in data:
        status["exercise_min"] = data["exercise"].get("count", 0)

    # Respiratory rate
    if "respRate" in data:
        status["respiratory_rate"] = data["respRate"].get("avg")

    # Blood pressure
    if "bloodpressure" in data:
        status["blood_pressure"] = {
            "systolic_avg": data["bloodpressure"].get("systolic_avg"),
            "systolic_min": data["bloodpressure"].get("systolic_min"),
            "systolic_max": data["bloodpressure"].get("systolic_max"),
            "diastolic_avg": data["bloodpressure"].get("diastolic_avg"),
            "diastolic_min": data["bloodpressure"].get("diastolic_min"),
            "diastolic_max": data["bloodpressure"].get("diastolic_max"),
            "elevated_readings": data["bloodpressure"].get("elevated_readings", 0)
        }

    # Blood glucose
    if "bloodglucose" in data:
        status["blood_glucose"] = {
            "avg": data["bloodglucose"].get("avg"),
            "min": data["bloodglucose"].get("min"),
            "max": data["bloodglucose"].get("max"),
            "std_dev": data["bloodglucose"].get("std_dev"),
            "in_range_pct": data["bloodglucose"].get("in_range_pct")
        }

    # Steps
    if "steps" in data:
        status["steps"] = data["steps"].get("count", 0)

    # Last 3 days for training pattern context
    recent_days = {}
    for i in range(1, 4):
        day_key = (get_pacific_now() - timedelta(days=i)).strftime("%Y-%m-%d")
        day_data = get_health_data(day_key)
        summary = get_day_summary(day_data)
        if summary:
            recent_days[f"day_minus_{i}"] = summary
    if recent_days:
        status["recent_days"] = recent_days

    return json.dumps(status, indent=2)


# Tool definitions for MCP

TOOLS = [
    {
        "name": "get_today",
        "description": "Get all raw health data for today: HRV, heart rate (with HR zones), sleep stages, blood pressure, blood glucose, steps, exercise minutes, respiratory rate.",
        "inputSchema": {"type": "object", "properties": {}, "required": []}
    },
    {
        "name": "get_trends",
        "description": "Get raw health data over multiple days: HRV, resting HR, exercise minutes, steps, HR zones, sleep data, blood pressure, blood glucose.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "description": "Number of days (default 7)"}
            },
            "required": []
        }
    },
    {
        "name": "get_recovery_status",
        "description": "Get recovery data: HRV (today vs 14-day baseline), resting HR, sleep, blood pressure, blood glucose, exercise minutes, HR zones, plus last 3 days for training pattern context. Includes user's weekly routine.",
        "inputSchema": {"type": "object", "properties": {}, "required": []}
    }
]


def handle_tool_call(name: str, args: dict) -> str:
    if name == "get_today":
        return tool_get_today()
    elif name == "get_trends":
        return tool_get_trends(args.get("days", 7))
    elif name == "get_recovery_status":
        return tool_get_recovery_status()
    return json.dumps({"error": f"Unknown tool: {name}"})


class handler(BaseHTTPRequestHandler):
    def send_json(self, data: dict, status: int = 200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def do_GET(self):
        if not check_secret(self.path):
            self.send_json({"error": "unauthorized"}, 401)
            return
        self.send_json({
            "name": "health",
            "version": "1.0.0",
            "description": "Personal health data from Apple Watch via iOS Shortcuts",
            "tools": TOOLS
        })

    def do_POST(self):
        if not check_secret(self.path):
            self.send_json({"error": "unauthorized"}, 401)
            return

        content_length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(content_length).decode("utf-8"))

        method = body.get("method", "")
        req_id = body.get("id")

        if method == "initialize":
            self.send_json({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "health", "version": "1.0.0"}
                }
            })
        elif method == "tools/list":
            self.send_json({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"tools": TOOLS}
            })
        elif method == "tools/call":
            params = body.get("params", {})
            result = handle_tool_call(params.get("name", ""), params.get("arguments", {}))
            self.send_json({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": result}]}
            })
        else:
            self.send_json({
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {"code": -32601, "message": f"Unknown method: {method}"}
            })
