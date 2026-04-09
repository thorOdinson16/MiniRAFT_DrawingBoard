import { WebSocketServer } from 'ws';
import fetch               from 'node-fetch';
import express             from 'express';

const REPLICAS  = process.env.REPLICAS.split(',');
const WS_PORT   = 8080;
const HTTP_PORT = 8081;

// ── Leader state ──────────────────────────────────────────────────────────────
let currentLeader          = null;
let isLeaderValid          = false;
let leaderDiscoveryPromise = null;
let leaderValidUntil       = 0;

// ── Client & canvas state ─────────────────────────────────────────────────────
const clients        = new Set();
const canvasLog      = [];   // replay buffer for late-joining clients
const pendingStrokes = [];

// ── Leader discovery ──────────────────────────────────────────────────────────
async function findLeader() {
  for (const url of REPLICAS) {
    try {
      const res = await fetch(`${url}/status`, { signal: AbortSignal.timeout(300) });
      if (!res.ok) continue;
      const data = await res.json();
      if (data.role === 'leader') {
        console.log(`[Gateway] Leader found: ${url}`);
        return url;
      }
    } catch (_) { /* replica down — try next */ }
  }
  return null;
}

async function ensureLeader(retries = 20) {
  if (currentLeader && isLeaderValid && Date.now() < leaderValidUntil) {
    return currentLeader;
  }
  if (leaderDiscoveryPromise) return leaderDiscoveryPromise;

  leaderDiscoveryPromise = (async () => {
    for (let i = 0; i < retries; i++) {
      const found = await findLeader();
      if (found) {
        currentLeader    = found;
        isLeaderValid    = true;
        // FIX: shorten cache TTL to 2 s so failovers are detected quickly
        leaderValidUntil = Date.now() + 2000;
        console.log(`[Gateway] Leader set: ${currentLeader}`);
        if (pendingStrokes.length > 0) {
          console.log(`[Gateway] Draining ${pendingStrokes.length} pending strokes`);
          const toSend = pendingStrokes.splice(0);
          for (const stroke of toSend) await forwardToLeader(stroke);
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

// ── Forward stroke to leader ──────────────────────────────────────────────────
async function forwardToLeader(stroke, maxAttempts = 10) {
  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    const leader = await ensureLeader(20);
    if (!leader) {
      pendingStrokes.push(stroke);
      return false;
    }
    try {
      const res = await fetch(`${leader}/stroke`, {
        method : 'POST',
        headers: { 'Content-Type': 'application/json' },
        body   : JSON.stringify(stroke),
        signal : AbortSignal.timeout(2000),
      });

      if (res.status === 307) {
        const data = await res.json();
        if (data.redirect) {
          currentLeader    = data.redirect;
          isLeaderValid    = true;
          leaderValidUntil = Date.now() + 2000;
        } else {
          currentLeader = null;
          isLeaderValid = false;
        }
        continue;
      }

      if (!res.ok) {
        console.log(`[Gateway] Leader ${leader} returned ${res.status} — re-discovering`);
        currentLeader = null;
        isLeaderValid = false;
        continue;
      }
      return true;
    } catch (err) {
      console.log(`[Gateway] Forward attempt ${attempt + 1} failed: ${err.message}`);
      currentLeader = null;
      isLeaderValid = false;
    }
  }
  console.log('[Gateway] Dropped stroke after max attempts');
  return false;
}

// ── Broadcast to all WebSocket clients ───────────────────────────────────────
function broadcastToClients(message) {
  const dead = [];
  let sent   = 0;
  for (const ws of clients) {
    if (ws.readyState === 1) {
      try { ws.send(JSON.stringify(message)); sent++; }
      catch (_) { dead.push(ws); }
    } else {
      dead.push(ws);
    }
  }
  dead.forEach(ws => clients.delete(ws));
  return sent;
}

// ── Replay canvas state to a new client ──────────────────────────────────────
async function replayCanvasToClient(ws) {
  if (canvasLog.length === 0) return;
  console.log(`[Gateway] Replaying ${canvasLog.length} strokes to new client`);
  for (const stroke of canvasLog) {
    if (ws.readyState !== 1) break;
    try { ws.send(JSON.stringify(stroke)); } catch (_) { break; }
  }
}

// ── WebSocket server ──────────────────────────────────────────────────────────
const wss = new WebSocketServer({ port: WS_PORT });

wss.on('connection', async (ws) => {
  clients.add(ws);
  console.log(`[Gateway] Client connected (total: ${clients.size})`);
  await replayCanvasToClient(ws);

  ws.on('message', async (raw) => {
    let message;
    try { message = JSON.parse(raw); } catch { return; }
    if (message.type === 'ping') {
      ws.send(JSON.stringify({ type: 'pong' }));
      return;
    }
    await forwardToLeader(message);
  });

  ws.on('close', () => {
    clients.delete(ws);
    console.log(`[Gateway] Client disconnected (total: ${clients.size})`);
  });
  ws.on('error', () => clients.delete(ws));
});

// ── HTTP endpoints (called by replicas) ──────────────────────────────────────
const app = express();
app.use(express.json({ limit: '1mb' }));

// Called by the leader after committing a stroke
app.post('/broadcast', (req, res) => {
  const message = req.body;
  // FIX: silently drop no-op entries — they are internal RAFT housekeeping
  if (message?.type === 'noop') {
    res.json({ ok: true, sent: 0 });
    return;
  }
  if (message.type === 'clear') {
    canvasLog.length = 0;
    broadcastToClients({ type: 'clear' });
    console.log(`[Gateway] Broadcast CLEAR to clients`);
    res.json({ ok: true, sent: clients.size });
    return;
  }
  if (message && !message.type) canvasLog.push(message);
  const sent = broadcastToClients(message);
  console.log(`[Gateway] Broadcast to ${sent} clients (log size: ${canvasLog.length})`);
  res.json({ ok: true, sent });
});

// Called by a newly elected leader
app.post('/leader-announce', (req, res) => {
  const { leader } = req.body;
  if (!leader) return res.status(400).json({ error: 'leader required' });
  console.log(`[Gateway] Leader announced: ${leader}`);
  currentLeader          = leader;
  isLeaderValid          = true;
  leaderValidUntil       = Date.now() + 2000;
  leaderDiscoveryPromise = null;
  res.json({ ok: true });
});

// Healthcheck
app.get('/health', (req, res) => {
  res.json({
    status        : 'ok',
    clients       : clients.size,
    leader        : currentLeader,
    pendingStrokes: pendingStrokes.length,
    canvasLogSize : canvasLog.length,
  });
});

// ── Rebuild canvasLog from leader on gateway startup ──────────────────────────
// Required so that late-joining clients see consistent state even after the
// gateway itself restarts (spec §6.1 "Consistent canvas state after restarts").
async function rebuildCanvasLog() {
  console.log('[Gateway] Attempting to rebuild canvasLog from leader...');
  const leader = await ensureLeader(30);
  if (!leader) {
    console.log('[Gateway] No leader available — canvasLog will be empty until first stroke');
    return;
  }
  try {
    const res = await fetch(`${leader}/log?from=0`, { signal: AbortSignal.timeout(3000) });
    const { entries } = await res.json();
    for (const e of entries) {
      if (e.data && e.data.type !== 'noop' && e.data.type !== 'clear') {
        canvasLog.push(e.data);
      }
    }
    console.log(`[Gateway] Rebuilt canvasLog: ${canvasLog.length} strokes from ${leader}`);
  } catch (err) {
    console.log(`[Gateway] Could not rebuild canvasLog: ${err.message}`);
  }
}

app.listen(HTTP_PORT, () => {
  console.log(`[Gateway] HTTP on :${HTTP_PORT}  WebSocket on :${WS_PORT}`);
  rebuildCanvasLog();   // ← add this call
});