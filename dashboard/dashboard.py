
import json
import os
import sys

import requests
from flask import Flask, jsonify, render_template

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

app = Flask(__name__)
LOG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), config.CHAOS_LOG_PATH)


def get_status(node_id):
    try:
        r = requests.get(f"{config.node_url(node_id)}/status", timeout=1)
        if r.status_code == 200:
            return r.json()
    except requests.exceptions.RequestException:
        pass
    return {"node_id": node_id, "role": "UNREACHABLE", "term": None,
            "leader_id": None, "partitioned": None, "key_count": None,
            "uptime_s": None, "last_heartbeat_age_s": None}


@app.route("/api/cluster-state")
def cluster_state():
    nodes = [get_status(nid) for nid in config.NODES]

    events = []
    if os.path.exists(LOG_PATH):
        with open(LOG_PATH) as f:
            lines = f.readlines()[-40:]
        for line in lines:
            line = line.strip()
            if line:
                events.append(json.loads(line))
        events.reverse()

    return jsonify({"nodes": nodes, "events": events})


@app.route("/")
def index():
    return render_template("index.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=config.DASHBOARD_PORT, debug=False)
