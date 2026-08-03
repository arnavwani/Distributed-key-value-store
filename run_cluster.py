
import os
import subprocess
import sys
import time

import config

ROOT = os.path.dirname(os.path.abspath(__file__))
NODE_SCRIPT = os.path.join(ROOT, "node", "node.py")
DASHBOARD_SCRIPT = os.path.join(ROOT, "dashboard", "dashboard.py")

procs = []

def main():
    for node_id in config.NODES:
        db_file = os.path.join(ROOT, "node", f"node{node_id}.db")
        if os.path.exists(db_file):
            os.remove(db_file)
        p = subprocess.Popen([sys.executable, NODE_SCRIPT, "--id", str(node_id)],
                              cwd=os.path.join(ROOT, "node"))
        procs.append(p)

    time.sleep(1)
    dash = subprocess.Popen([sys.executable, DASHBOARD_SCRIPT], cwd=os.path.join(ROOT, "dashboard"))
    procs.append(dash)

    print("\nCluster running. Dashboard: http://localhost:%d" % config.DASHBOARD_PORT)
    print("Press Ctrl+C to stop everything.\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("stopping...")
        for p in procs:
            p.kill()

if __name__ == "__main__":
    main()
