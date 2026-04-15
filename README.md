# 🎨 MiniRAFT Drawing Board

A **distributed, real-time collaborative drawing board** backed by a from-scratch implementation of the [RAFT consensus algorithm](https://raft.github.io/). Draw on a shared canvas while watching a 3-node cluster elect leaders, replicate log entries, and survive node failures — all visualized live in the browser.

![Stack](https://img.shields.io/badge/Node.js-20-green?logo=node.js) ![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker) ![License](https://img.shields.io/badge/license-MIT-lightgrey)

---

## ✨ Features

- **RAFT Consensus** — Leader election, log replication, heartbeats, and term management implemented from scratch in Node.js
- **Real-time Collaboration** — WebSocket-based gateway broadcasts strokes to every connected client instantly
- **Live Cluster Dashboard** — Watch each replica's role (Leader / Follower / Candidate), current term, log length, and commit index update in real time
- **Fault Tolerant** — The cluster tolerates one node failure and automatically re-elects a new leader
- **Canvas Replay** — Late-joining clients receive the full committed log and have the canvas rebuilt on connect
- **Touch & Pointer Support** — Works on mobile and tablet devices
- **Glassmorphic UI** — Dark-mode interface with color picker, brush size, eraser, and clear tools

---

## 🏗️ Architecture

```
Browser (frontend)
     │  WebSocket (ws://localhost:8080)
     ▼
┌─────────────┐         HTTP
│   Gateway   │ ──────────────────► Leader Replica
│  :8080 (WS) │                         │
│  :8081 (HTTP│                    AppendEntries
└─────────────┘                    Heartbeats
                                        │
                              ┌─────────┴─────────┐
                         Replica 2            Replica 3
                          :4002                 :4003
```

| Service    | Port(s)        | Role                                                                 |
|------------|----------------|----------------------------------------------------------------------|
| `frontend` | `3000`         | Static HTML/JS canvas app served by nginx                           |
| `gateway`  | `8080`, `8081` | WebSocket hub + HTTP API; discovers the leader and forwards writes  |
| `replica1` | `4001`         | RAFT node — participates in elections and log replication           |
| `replica2` | `4002`         | RAFT node                                                           |
| `replica3` | `4003`         | RAFT node                                                           |

### RAFT Implementation Highlights

- **Leader Election** — randomised election timeouts trigger candidacy; nodes vote based on log completeness
- **Log Replication** — `/append-entries` ensures followers stay consistent; the leader retries diverged followers
- **Heartbeats** — a dedicated `/heartbeat` endpoint resets follower timers without running consistency checks
- **Stroke forwarding** — `POST /stroke` is only accepted by the leader; followers reply with a `307` redirect so the gateway can re-route
- **Canvas sync** — `/sync-log` lets the gateway rebuild canvas state for new clients; `/log` returns only entries after the last committed `clear` command

---

## 🚀 Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) & [Docker Compose](https://docs.docker.com/compose/)

### Run

```bash
git clone https://github.com/thorOdinson16/MiniRAFT_DrawingBoard.git
cd MiniRAFT_DrawingBoard
docker compose up --build
```

Then open **http://localhost:3000** in your browser.

> First startup may take ~15 seconds while health checks confirm all replicas are ready before the gateway and frontend come up.

### Stop

```bash
docker compose down
```

---

## 🖥️ Usage

1. **Draw** — click and drag (or touch) anywhere on the canvas
2. **Change colour** — use the colour picker in the toolbar
3. **Brush size** — drag the size slider
4. **Eraser** — click the 🧽 Eraser button, then draw over existing strokes
5. **Clear canvas** — click 🗑️ Clear All (broadcasts a `clear` command through consensus)
6. **Watch the cluster** — the dashboard at the top shows each replica's live state; the current leader has a green left border and a **LEADER** badge

### Simulating Failures

You can kill a replica to observe leader re-election:

```bash
# Stop replica1 to force a new election
docker compose stop replica1

# Bring it back — it will rejoin as a follower and catch up
docker compose start replica1
```

The cluster tolerates **1 node failure** out of 3 (majority = 2).

---

## 📡 API Reference

### Replica endpoints (`:4001` / `:4002` / `:4003`)

| Method | Path              | Description                                      |
|--------|-------------------|--------------------------------------------------|
| GET    | `/status`         | Returns `{ role, term, logLength, commitIndex }` |
| GET    | `/log?from=N`     | Returns committed log entries from index `N`     |
| POST   | `/stroke`         | Submit a drawing stroke (leader only; 307 otherwise) |
| POST   | `/request-vote`   | RAFT vote request RPC                            |
| POST   | `/append-entries` | RAFT log replication RPC                         |
| POST   | `/heartbeat`      | Leader heartbeat                                 |
| POST   | `/sync-log`       | Bulk log sync for canvas rebuild                 |

### Gateway endpoints (`:8081`)

| Method | Path      | Description                    |
|--------|-----------|--------------------------------|
| GET    | `/health` | Health check (used by Docker)  |

---

## 🗂️ Project Structure

```
├── frontend/
│   └── index.html          # Single-page canvas app
├── gateway/
│   └── src/index.js        # WebSocket hub + leader discovery
├── replica1/               # RAFT node 1
├── replica2/               # RAFT node 2
├── replica3/               # RAFT node 3
│   └── src/index.js        # Shared replica logic (identical across nodes)
└── docker-compose.yml
```

---

## 🛠️ Tech Stack

- **Runtime** — Node.js 20 (ESM)
- **HTTP/WS** — Express, `ws`
- **Frontend** — Vanilla HTML5 Canvas + CSS (no frameworks)
- **Containerisation** — Docker + Docker Compose, nginx (frontend)
- **Dev** — nodemon with live reload via volume mounts


MIT — see [LICENSE](LICENSE) for details.
