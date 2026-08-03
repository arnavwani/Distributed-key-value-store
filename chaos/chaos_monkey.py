
import json
import os
import random
import subprocess
import sys
import time

import requests

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

NODE_SCRIPT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "node", "node.py")
LOG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), config.CHAOS_LOG_PATH)


def log_event(event: dict):
    event["timestamp"] = time.time()
    with open(LOG_PATH, "a") as f:
        f.write(json.dumps(event) + "\n")
    print(f"[chaos] {event}")


class Cluster:
    def __init__(self):
        self.procs = {}

    def start_node(self, node_id):
        db_file = f"node{node_id}.db"
        if self.procs.get(node_id) is None and os.path.exists(db_file) and node_id not in self.procs:
            os.remove(db_file)
        p = subprocess.Popen(
            [sys.executable, NODE_SCRIPT, "--id", str(node_id)],
            cwd=os.path.dirname(NODE_SCRIPT),
        )
        self.procs[node_id] = p

    def start_all(self):
        for node_id in config.NODES:
            self.start_node(node_id)
        time.sleep(2)

    def kill(self, node_id):
        p = self.procs.get(node_id)
        if p is not None:
            p.kill()
            p.wait()
            self.procs[node_id] = None

    def restart(self, node_id):
        self.start_node(node_id)

    def is_alive(self, node_id):
        p = self.procs.get(node_id)
        return p is not None and p.poll() is None


def get_status(node_id):
    try:
        r = requests.get(f"{config.node_url(node_id)}/status", timeout=1)
        return r.json() if r.status_code == 200 else None
    except requests.exceptions.RequestException:
        return None


def get_leader(cluster):
    for node_id in config.NODES:
        if not cluster.is_alive(node_id):
            continue
        s = get_status(node_id)
        if s and s["role"] == "LEADER":
            return node_id
    return None


def put(node_id, key, value):
    try:
        r = requests.post(f"{config.node_url(node_id)}/put",
                           json={"key": key, "value": value}, timeout=2)
        return r.status_code == 200
    except requests.exceptions.RequestException:
        return False


def check_consistency(cluster):
    snapshots = {}
    for node_id in config.NODES:
        if not cluster.is_alive(node_id):
            continue
        s = get_status(node_id)
        if s is None or s.get("partitioned"):
            continue
        try:
            r = requests.get(f"{config.node_url(node_id)}/snapshot", timeout=1)
            if r.status_code == 200:
                raw = r.json()
                live = {k: v["value"] for k, v in raw.items() if not v["deleted"]}
                snapshots[node_id] = live
        except requests.exceptions.RequestException:
            pass

    if len(snapshots) < 2:
        return True, {"reachable_nodes": list(snapshots.keys()), "note": "fewer than 2 nodes reachable, nothing to compare"}

    values = list(snapshots.values())
    consistent = all(v == values[0] for v in values)
    return consistent, {"reachable_nodes": list(snapshots.keys()), "snapshots": snapshots}


ACTIONS = ["kill_leader", "kill_follower", "partition_node", "delay_node"]


def run_chaos_round(cluster, round_num):
    action = random.choice(ACTIONS)
    log_event({"round": round_num, "action": action, "phase": "start"})

    if action == "kill_leader":
        leader = get_leader(cluster)
        if leader is None:
            log_event({"round": round_num, "action": action, "phase": "skip", "reason": "no leader found"})
            return
        cluster.kill(leader)
        log_event({"round": round_num, "action": action, "target": leader, "phase": "killed"})
        time.sleep(4)
        cluster.restart(leader)
        log_event({"round": round_num, "action": action, "target": leader, "phase": "restarted"})

    elif action == "kill_follower":
        leader = get_leader(cluster)
        candidates = [n for n in config.NODES if n != leader and cluster.is_alive(n)]
        if not candidates:
            log_event({"round": round_num, "action": action, "phase": "skip", "reason": "no follower available"})
            return
        target = random.choice(candidates)
        cluster.kill(target)
        log_event({"round": round_num, "action": action, "target": target, "phase": "killed"})
        time.sleep(2)
        cluster.restart(target)
        log_event({"round": round_num, "action": action, "target": target, "phase": "restarted"})

    elif action == "partition_node":
        alive = [n for n in config.NODES if cluster.is_alive(n)]
        if not alive:
            return
        target = random.choice(alive)
        try:
            requests.post(f"{config.node_url(target)}/admin/partition", timeout=1)
        except requests.exceptions.RequestException:
            pass
        duration = random.uniform(3, 6)
        log_event({"round": round_num, "action": action, "target": target,
                   "phase": "partitioned", "duration_s": round(duration, 1)})
        time.sleep(duration)
        try:
            requests.post(f"{config.node_url(target)}/admin/heal", timeout=1)
        except requests.exceptions.RequestException:
            pass
        log_event({"round": round_num, "action": action, "target": target, "phase": "healed"})

    elif action == "delay_node":

        alive = [n for n in config.NODES if cluster.is_alive(n)]
        if not alive:
            return
        target = random.choice(alive)
        try:
            requests.post(f"{config.node_url(target)}/admin/partition", timeout=1)
        except requests.exceptions.RequestException:
            pass
        time.sleep(1.5)
        try:
            requests.post(f"{config.node_url(target)}/admin/heal", timeout=1)
        except requests.exceptions.RequestException:
            pass
        log_event({"round": round_num, "action": action, "target": target, "phase": "delayed_1.5s"})

    time.sleep(3)
    consistent, details = check_consistency(cluster)
    log_event({"round": round_num, "action": action, "phase": "consistency_check",
               "consistent": consistent, **details})


def main():
    if os.path.exists(LOG_PATH):
        os.remove(LOG_PATH)
    for node_id in config.NODES:
        db_file = os.path.join(os.path.dirname(NODE_SCRIPT), f"node{node_id}.db")
        if os.path.exists(db_file):
            os.remove(db_file)

    cluster = Cluster()
    cluster.start_all()

    leader = None
    for _ in range(20):
        leader = get_leader(cluster)
        if leader is not None:
            break
        time.sleep(0.5)

    log_event({"phase": "cluster_ready", "leader": leader})

    if leader is not None:
        for i in range(5):
            put(leader, f"seed-key-{i}", f"value-{i}")
    else:
        log_event({"phase": "warning", "message": "no leader elected before seeding - chaos will run against an empty store"})

    round_num = 0
    try:
        while True:
            round_num += 1
            run_chaos_round(cluster, round_num)
            time.sleep(2)
    except KeyboardInterrupt:
        log_event({"phase": "shutdown", "reason": "keyboard_interrupt"})
        for node_id in list(cluster.procs):
            cluster.kill(node_id)


if __name__ == "__main__":
    main()
