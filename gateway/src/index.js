import { WebSocketServer } from 'ws';
import fetch from 'node-fetch';
import express from 'express';

const REPLICAS = process.env.REPLICAS.split(',');
const PORT = 8080;

let currentLeader   = null;
let isLeaderValid   = false;
let leaderDiscoveryPromise = null;
let leaderValidUntil = 0;
const clients       = new Set();
const pendingStrokes = [];

// ─── Canvas state (in-memory replay buffer) ────────────────────────────────
// Stores all committed strokes so late-joining clients can catch up
const canvasLog = [];

// ─── Leader discovery ──────────────────────────────────────────────────────
async function findLeader() {
  for (const url of REPLICAS) {
    try {
      const res = await fetch(`${url}/status`);
      if (!res.ok) continue;
      const data = await res.json();
      if (data.role === 'leader') {
        console.log(`[Gateway] Leader found: ${url}`);
        return url;
      }
    } catch (_) { }
  }
  return null;
}

async function ensureLeader(retries = 20) {
  // Fast path: trust current leader for 5s without re-validating
  if (currentLeader && isLeaderValid && Date.now() < leaderValidUntil) {
    return currentLeader;
  }

  // Deduplicate: if discovery is already in progress, wait for it
  if (leaderDiscoveryPromise) return leaderDiscoveryPromise;

  leaderDiscoveryPromise = (async () => {
    for (let i = 0; i < retries; i++) {
      const found = await findLeader();
      if (found) {
        currentLeader = found;
        isLeaderValid = true;
        leaderValidUntil = Date.now() + 5000;
        console.log(`[Gateway] New leader: ${currentLeader}`);
        if (pendingStrokes.length > 0) {
          console.log(`[Gateway] Draining ${pendingStrokes.length} pending strokes`);
          const toSend = pendingStrokes.splice(0);
          for (const { stroke } of toSend) {
            await forwardToLeader(stroke);
          }
        }
        leaderDiscoveryPromise = null;
        return currentLeader;
      }
      await new Promise(r => setTimeout(r, 300));
    }
    leaderDiscoveryPromise = null;
    console.log('[Gateway] Could not find a leader after retries');
    return null;
  })();

  return leaderDiscoveryPromise;
}

// ─── Forward a stroke to the leader (no recursion — iterative with retries) ─
async function forwardToLeader(stroke, maxAttempts = 5) {
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    const leader = await ensureLeader(10);
    if (!leader) {
      console.log(`[Gateway] No leader available, queuing stroke (attempt ${attempt + 1})`);
      pendingStrokes.push({ stroke });
      return false;
    }

    try {
      const res = await fetch(`${leader}/stroke`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(stroke)
      });

      if (res.status === 307) {
        const data = await res.json();

        if (data.redirect) {
          currentLeader = data.redirect;   // ✅ THIS IS KEY
          isLeaderValid = true;
          continue;
        }

        currentLeader = null;
        isLeaderValid = false;
        continue;
      }

      if (!res.ok) {
        console.log(`[Gateway] Leader returned ${res.status}, re-discovering`);
        currentLeader = null;
        isLeaderValid = false;
        continue;
      }

      return true;
    } catch (err) {
      console.log(`[Gateway] Stroke forward failed (${err.message}), attempt ${attempt + 1}`);
      if (err.name !== 'AbortError') {
        currentLeader = null;
        isLeaderValid = false;
      }
    }
  }

  console.log('[Gateway] Dropped stroke after max attempts');
  return false;
}

// ─── Broadcast to all WebSocket clients ───────────────────────────────────
function broadcastToClients(message) {
  const dead = [];
  let sent = 0;
  for (const ws of clients) {
    if (ws.readyState === 1) {
      try {
        ws.send(JSON.stringify(message));
        sent++;
      } catch (_) {
        dead.push(ws);
      }
    } else {
      dead.push(ws);
    }
  }
  dead.forEach(ws => clients.delete(ws));
  return sent;
}

// ─── Replay canvas state to a newly connected client ──────────────────────
async function replayCanvasToClient(ws) {
  if (canvasLog.length === 0) return;
  console.log(`[Gateway] Replaying ${canvasLog.length} strokes to new client`);

  // Send all strokes in order; batch as fast as possible
  for (const stroke of canvasLog) {
    if (ws.readyState !== 1) break;
    try {
      ws.send(JSON.stringify(stroke));
    } catch (_) {
      break;
    }
  }
}

// ─── WebSocket server ──────────────────────────────────────────────────────
const wss = new WebSocketServer({ port: PORT });

wss.on('connection', async (ws) => {
  clients.add(ws);
  console.log(`[Gateway] Client connected (total: ${clients.size})`);

  // Replay existing canvas state to the new client
  await replayCanvasToClient(ws);

  ws.on('message', async (raw) => {
    let message;
    try { message = JSON.parse(raw); } catch { return; }

    if (message.type === 'ping') {
      ws.send(JSON.stringify({ type: 'pong' }));
      return;
    }

    if (message.type === 'clear') {
      canvasLog.length = 0; // wipe replay buffer too
      broadcastToClients({ type: 'clear' });
      return;
    }

    // Regular stroke — forward to leader
    await forwardToLeader(message);
  });

  ws.on('close', () => {
    clients.delete(ws);
    console.log(`[Gateway] Client disconnected (total: ${clients.size})`);
  });

  ws.on('error', () => clients.delete(ws));
});

// ─── HTTP endpoints (called by replicas) ──────────────────────────────────
const app = express();
app.use(express.json());

app.post('/broadcast', (req, res) => {
  const stroke = req.body;

  // Store in replay buffer (only regular strokes, not pings/clears)
  if (stroke && !stroke.type) {
    canvasLog.push(stroke);
  }

  const sent = broadcastToClients(stroke);
  console.log(`[Gateway] Broadcast stroke to ${sent} clients (canvas log: ${canvasLog.length})`);
  res.json({ ok: true, sent });
});

app.post('/leader-announce', (req, res) => {
  const { leader } = req.body;
  if (!leader) return res.status(400).json({ error: 'No leader provided' });

  console.log(`[Gateway] Leader announced: ${leader}`);
  currentLeader = leader;
  isLeaderValid = true;
  leaderValidUntil = Date.now() + 5000;
  leaderDiscoveryPromise = null;
  res.json({ ok: true });
});

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    clients: clients.size,
    leader: currentLeader,
    pendingStrokes: pendingStrokes.length,
    canvasLogSize: canvasLog.length,
  });
});

app.listen(8081, () => console.log('[Gateway] HTTP listener on 8081'));
console.log(`[Gateway] WebSocket server on ws://localhost:${PORT}`);