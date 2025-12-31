"""
Health data retrieval endpoint.
Simple read access to stored health data.
"""
from http.server import BaseHTTPRequestHandler
from upstash_redis import Redis
from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlparse
import json
import os

API_KEY = os.environ.get("API_KEY", "")

redis = Redis(
    url=os.environ.get("UPSTASH_REDIS_REST_URL"),
    token=os.environ.get("UPSTASH_REDIS_REST_TOKEN")
)


def check_auth(headers) -> bool:
    if not API_KEY:
        return True
    auth = headers.get("Authorization", "")
    return auth == f"Bearer {API_KEY}"


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if not check_auth(self.headers):
            self.send_response(401)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "unauthorized"}).encode())
            return

        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        days = int(query.get("days", [7])[0])

        results = {}
        for i in range(days):
            date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            data = redis.get(f"health:{date}")
            if data:
                results[date] = json.loads(data)

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(results, indent=2).encode())
