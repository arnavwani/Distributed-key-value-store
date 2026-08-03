# Distributed-kv-store

A distributed key-value store built from scratch in Python, with leader election, log replication, automatic failover, and a chaos-testing harness that deliberately breaks the cluster to verify it stays consistent.

Inspired by the core ideas behind Raft consensus — implemented as a simplified, from-scratch version to make the mechanics of distributed consensus (leader election, replication, failure recovery) directly readable and testable.

## Features

- **Leader-follower replication** — writes go through an elected leader and are replicated to followers before being acknowledged
- **Automatic leader election** — randomized election timeouts + majority voting; a new leader is elected within seconds of the old one failing
- **Crash recovery** — a node that restarts (fresh or after a crash) automatically catches up via a full snapshot from a peer, merged with last-write-wins conflict resolution
- **Chaos testing harness** — a separate script that randomly kills nodes, partitions them from the network, or delays them, then verifies the surviving nodes still agree on the data
- **Live dashboard** — real-time view of cluster state (leader, term, per-node key counts) and a running log of chaos events with pass/fail consistency checks

## Demo

Cluster surviving 17 rounds of random node kills and network partitions with zero data loss — key count stays flat at 5 across all three nodes throughout:


![alt text](<Screenshot 2026-08-02 212023.png>)

![alt text](<Screenshot 2026-08-02 212113.png>)

## Architecture

```
        ┌─────────┐        ┌─────────┐        ┌─────────┐
        │ node 0  │◄──────►│ node 1  │◄──────►│ node 2  │
        │ :5001   │        │ :5002   │        │ :5003   │
        └────┬────┘        └────┬────┘        └────┬────┘
             │   heartbeats / replication / votes   │
             └───────────────────┬───────────────────┘
                                  │
                          each node owns its
                          own SQLite file
```

- Client writes (`PUT` / `DELETE`) must go to the current leader; the leader replicates to followers before acknowledging
- Client reads (`GET`) are served by any node directly from its local SQLite file (eventual consistency — a follower can briefly lag behind the leader)
- If the leader stops sending heartbeats, followers hold a leader election after a randomized timeout

## Tech stack

| Layer | Tech |
|---|---|
| Node API / inter-node RPC | Python, Flask, `requests` |
| Storage | SQLite (one file per node) |
| Dashboard | Flask + Chart.js |
| Chaos testing | Python (`subprocess` for process control) |

## How it works

**Replication.** All writes go through the leader. The leader writes locally, forwards the write to every follower, and waits for acknowledgment from a majority of nodes before confirming the write succeeded.

**Leader election.** Each node sends (if leader) or waits for (if follower) periodic heartbeats. A follower that doesn't hear from the leader within a randomized timeout starts an election, requesting votes from its peers; it becomes leader if it wins a majority.

**Recovery.** A node that boots up — fresh or after a crash — requests a full data snapshot from a peer and merges it in using last-write-wins (based on a timestamp per key), so a node that was offline catches back up automatically.

**Chaos testing.** A separate script randomly kills processes, partitions nodes from the network, and injects delays, then checks whether all live nodes still agree on the data after each attack. Results are logged and shown live on the dashboard.

