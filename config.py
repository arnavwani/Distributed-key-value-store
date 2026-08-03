

NODES = {
    0: {"host": "127.0.0.1", "port": 5001},
    1: {"host": "127.0.0.1", "port": 5002},
    2: {"host": "127.0.0.1", "port": 5003},
}

def node_url(node_id: int) -> str:
    n = NODES[node_id]
    return f"http://{n['host']}:{n['port']}"


HEARTBEAT_INTERVAL = 1.0
ELECTION_TIMEOUT_RANGE = (3.0, 5.0) 
                                    
REQUEST_TIMEOUT = 1.0             
REPLICATION_QUORUM_TIMEOUT = 2.0  
                                   

DASHBOARD_PORT = 5000
DASHBOARD_POLL_INTERVAL = 2.0

CHAOS_LOG_PATH = "chaos_log.jsonl"
