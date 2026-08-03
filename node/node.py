

import argparse
import random
import threading
import time

import requests
from flask import Flask, jsonify, request

from storage import Storage
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

FOLLOWER, CANDIDATE, LEADER = "FOLLOWER", "CANDIDATE", "LEADER"


class NodeState:
    def __init__(self, node_id: int):
        self.node_id = node_id
        self.role = FOLLOWER
        self.current_term = 0
        self.voted_for = None
        self.leader_id = None
        self.last_heartbeat = time.time()
        self.lock = threading.RLock()

        self.partitioned = False

        self.storage = Storage(f"node{node_id}.db")

        self.last_replicated_at = None
        self.started_at = time.time()


def make_app(node_id: int) -> Flask:
    state = NodeState(node_id)
    app = Flask(__name__)
    peers = [nid for nid in config.NODES if nid != node_id]


    def reset_election_timer():
        state.last_heartbeat = time.time()

    def election_deadline():
        lo, hi = config.ELECTION_TIMEOUT_RANGE
        return random.uniform(lo, hi)

    def call_peer(peer_id, path, payload=None, method="POST", timeout=None):
        if state.partitioned:
            return None
        timeout = timeout or config.REQUEST_TIMEOUT
        url = f"{config.node_url(peer_id)}{path}"
        try:
            if method == "POST":
                r = requests.post(url, json=payload, timeout=timeout)
            else:
                r = requests.get(url, timeout=timeout)
            if r.status_code == 200:
                return r.json()
        except requests.exceptions.RequestException:
            return None
        return None


    def heartbeat_loop():
        while True:
            time.sleep(config.HEARTBEAT_INTERVAL)
            with state.lock:
                if state.role != LEADER or state.partitioned:
                    continue
                term, nid = state.current_term, state.node_id
            for peer in peers:
                call_peer(peer, "/heartbeat", {"term": term, "leader_id": nid})

    def election_loop():
        while True:
            time.sleep(0.3)
            with state.lock:
                if state.role == LEADER or state.partitioned:
                    continue
                timed_out = (time.time() - state.last_heartbeat) > election_deadline()
            if timed_out:
                start_election()

    def start_election():
        with state.lock:
            state.role = CANDIDATE
            state.current_term += 1
            term = state.current_term
            state.voted_for = (term, state.node_id)
            state.leader_id = None
        reset_election_timer()

        votes = 1
        for peer in peers:
            resp = call_peer(peer, "/request_vote",
                              {"term": term, "candidate_id": state.node_id})
            if resp and resp.get("vote_granted"):
                votes += 1

        majority = (len(config.NODES) // 2) + 1
        with state.lock:
            if state.role == CANDIDATE and state.current_term == term and votes >= majority:
                state.role = LEADER
                state.leader_id = state.node_id
                print(f"[node {state.node_id}] elected LEADER for term {term} "
                      f"with {votes}/{len(config.NODES)} votes")
            else:
                state.role = FOLLOWER

    def catch_up_on_startup():
        for peer in peers:
            resp = call_peer(peer, "/snapshot", method="GET")
            if resp is not None:
                state.storage.apply_snapshot(resp)
                print(f"[node {state.node_id}] caught up from peer {peer}: "
                      f"{len(resp)} keys merged")
                return

    @app.route("/put", methods=["POST"])
    def put():
        body = request.get_json(force=True)
        key, value = body.get("key"), body.get("value")
        if key is None:
            return jsonify({"error": "key is required"}), 400

        with state.lock:
            if state.role != LEADER:
                return jsonify({
                    "error": "not the leader",
                    "leader_id": state.leader_id,
                    "leader_url": config.node_url(state.leader_id) if state.leader_id is not None else None,
                }), 421
            term = state.current_term

        ts = time.time()
        state.storage.put(key, value, updated_at=ts)

        acks = 1
        deadline = time.time() + config.REPLICATION_QUORUM_TIMEOUT
        for peer in peers:
            if time.time() > deadline:
                break
            resp = call_peer(peer, "/replicate", {
                "op": "put", "key": key, "value": value,
                "updated_at": ts, "term": term,
            })
            if resp and resp.get("ok"):
                acks += 1

        majority = (len(config.NODES) // 2) + 1
        state.last_replicated_at = time.time()
        if acks >= majority:
            return jsonify({"ok": True, "acks": acks, "term": term})
        return jsonify({"ok": False, "error": "failed to reach write quorum",
                         "acks": acks}), 503

    @app.route("/delete/<key>", methods=["DELETE"])
    def delete(key):
        with state.lock:
            if state.role != LEADER:
                return jsonify({
                    "error": "not the leader",
                    "leader_id": state.leader_id,
                    "leader_url": config.node_url(state.leader_id) if state.leader_id is not None else None,
                }), 421
            term = state.current_term

        ts = time.time()
        state.storage.delete(key, updated_at=ts)

        acks = 1
        for peer in peers:
            resp = call_peer(peer, "/replicate", {
                "op": "delete", "key": key, "updated_at": ts, "term": term,
            })
            if resp and resp.get("ok"):
                acks += 1

        majority = (len(config.NODES) // 2) + 1
        if acks >= majority:
            return jsonify({"ok": True, "acks": acks})
        return jsonify({"ok": False, "error": "failed to reach write quorum",
                         "acks": acks}), 503

    @app.route("/get/<key>", methods=["GET"])
    def get(key):
        value = state.storage.get(key)
        if value is None:
            return jsonify({"error": "not found"}), 404
        return jsonify({"key": key, "value": value})

    @app.route("/heartbeat", methods=["POST"])
    def heartbeat():
        body = request.get_json(force=True)
        term, leader_id = body["term"], body["leader_id"]
        if state.partitioned:
            return jsonify({"ok": False}), 200
        with state.lock:
            if term >= state.current_term:
                state.current_term = term
                state.role = FOLLOWER
                state.leader_id = leader_id
        reset_election_timer()
        return jsonify({"ok": True})

    @app.route("/request_vote", methods=["POST"])
    def request_vote():
        body = request.get_json(force=True)
        term, candidate_id = body["term"], body["candidate_id"]
        if state.partitioned:
            return jsonify({"vote_granted": False}), 200
        with state.lock:
            already_voted_this_term = (
                state.voted_for is not None and state.voted_for[0] == term
            )
            if term > state.current_term and not already_voted_this_term:
                state.current_term = term
                state.voted_for = (term, candidate_id)
                state.role = FOLLOWER
                reset_election_timer()
                return jsonify({"vote_granted": True})
        return jsonify({"vote_granted": False})

    @app.route("/replicate", methods=["POST"])
    def replicate():
        body = request.get_json(force=True)
        if state.partitioned:
            return jsonify({"ok": False}), 200
        op, key, ts, term = body["op"], body["key"], body["updated_at"], body["term"]
        with state.lock:
            if term < state.current_term:
                return jsonify({"ok": False, "error": "stale term"}), 200
        if op == "put":
            state.storage.put(key, body["value"], updated_at=ts)
        elif op == "delete":
            state.storage.delete(key, updated_at=ts)
        return jsonify({"ok": True})

    @app.route("/snapshot", methods=["GET"])
    def snapshot():
        return jsonify(state.storage.get_all_raw())

    @app.route("/status", methods=["GET"])
    def status():
        with state.lock:
            return jsonify({
                "node_id": state.node_id,
                "role": state.role,
                "term": state.current_term,
                "leader_id": state.leader_id,
                "partitioned": state.partitioned,
                "key_count": state.storage.count(),
                "uptime_s": round(time.time() - state.started_at, 1),
                "last_heartbeat_age_s": round(time.time() - state.last_heartbeat, 2),
            })

    @app.route("/admin/partition", methods=["POST"])
    def admin_partition():
        state.partitioned = True
        return jsonify({"ok": True, "partitioned": True})

    @app.route("/admin/heal", methods=["POST"])
    def admin_heal():
        state.partitioned = False
        reset_election_timer()
        return jsonify({"ok": True, "partitioned": False})

    catch_up_on_startup()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    threading.Thread(target=election_loop, daemon=True).start()

    return app


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, required=True, help="node id (must be a key in config.NODES)")
    args = parser.parse_args()

    node_id = args.id
    app = make_app(node_id)
    port = config.NODES[node_id]["port"]
    print(f"[node {node_id}] starting on port {port}")
    app.run(host="0.0.0.0", port=port, threaded=True)
