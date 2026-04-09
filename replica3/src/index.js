import express from 'express';
import fetch from 'node-fetch';

const REPLICA_ID  = process.env.REPLICA_ID;
const PORT        = parseInt(process.env.PORT);
const PEERS       = process.env.PEERS.split(',').filter(Boolean);
const GATEWAY_URL = 'http://gateway:8081';

// ── Spec-compliant timing constants ──────────────────────────────────────────
const HEARTBEAT_MS          = 150;
const ELECTION_TIMEOUT      = () => 500 + Math.random() * 300; // 500–800 ms
const PEER_DEAD_COOLDOWN_MS = 2000;

// ── RAFT state ────────────────────────────────────────────────────────────────
let role               = 'follower';
let currentTerm        = 0;
let votedFor           = null;
let log                = [];
let commitIndex        = -1;
let lastApplied        = -1;
let lastCommittedIndex = -1;
let electionTimer      = null;
let heartbeatTimer     = null;
let leaderId           = null;
let isShuttingDown     = false;

// Per-peer replication tracking (canonical RAFT: nextIndex + matchIndex)
const nextIndex     = {};
const matchIndex    = {};
const peerAlive     = {};
const peerDeadSince = {};

const CLUSTER_SIZE = PEERS.length + 1;
const MAJORITY     = Math.floor(CLUSTER_SIZE / 2) + 1;

PEERS.forEach(p => {
  nextIndex[p]     = 0;
  matchIndex[p]    = -1;
  peerAlive[p]     = true;
  peerDeadSince[p] = null;
});

// ── Logging ───────────────────────────────────────────────────────────────────
function log_msg(...args) {
  console.log(`[${REPLICA_ID}][${role.toUpperCase()}][term=${currentTerm}]`, ...args);
}

// ── Election timer ────────────────────────────────────────────────────────────
function resetElectionTimer() {
  if (isShuttingDown) return;
  clearTimeout(electionTimer);
  electionTimer = setTimeout(startElection, ELECTION_TIMEOUT());
}

function stopElectionTimer() {
  clearTimeout(electionTimer);
  electionTimer = null;
}

// ── State transitions ─────────────────────────────────────────────────────────
function becomeFollower(term, leader = null) {
  if (term < currentTerm) return;
  const changed = term > currentTerm || leader !== leaderId || role !== 'follower';
  clearInterval(heartbeatTimer);
  heartbeatTimer = null;
  role        = 'follower';
  if (term > currentTerm) votedFor = null;
  currentTerm = term;
  leaderId    = leader;
  if (changed) log_msg(`→ FOLLOWER (term=${term}, leader=${leader})`);
  resetElectionTimer();
}

async function becomeLeader() {
  if (isShuttingDown) return;
  log_msg('→ LEADER');
  role     = 'leader';
  leaderId = `http://${REPLICA_ID}:${PORT}`;
  stopElectionTimer();

  PEERS.forEach(p => {
    nextIndex[p]     = log.length;
    matchIndex[p]    = -1;
    peerAlive[p]     = true;
    peerDeadSince[p] = null;
  });

  const noop = { term: currentTerm, index: log.length, data: { type: 'noop' } };
  log.push(noop);

  // FIX: heartbeat loop now calls the dedicated /heartbeat RPC, not sendAppendEntries.
  // sendAppendEntries is only called for log replication. This matches the spec's
  // explicit /heartbeat endpoint requirement and separates concerns correctly.
  sendHeartbeats();
  heartbeatTimer = setInterval(sendHeartbeats, HEARTBEAT_MS);

  for (let i = 0; i < 3; i++) {
    if (role !== 'leader') return;
    try {
      await fetch(`${GATEWAY_URL}/leader-announce`, {
        method : 'POST',
        headers: { 'Content-Type': 'application/json' },
        body   : JSON.stringify({ leader: `http://${REPLICA_ID}:${PORT}` }),
      });
      log_msg('Announced leadership to gateway');
      break;
    } catch (_) {
      await new Promise(r => setTimeout(r, 300));
    }
  }

  await _replicateEntry(noop, { type: 'noop' });
}

// ── Election ──────────────────────────────────────────────────────────────────
async function startElection() {
  if (isShuttingDown || role === 'leader') return;
  role        = 'candidate';
  currentTerm += 1;
  votedFor    = REPLICA_ID;
  leaderId    = null;
  log_msg(`Starting election for term ${currentTerm}`);

  const votes = await requestVotes();

  if (votes >= MAJORITY && role === 'candidate' && !isShuttingDown) {
    await becomeLeader();
  } else if (role === 'candidate') {
    log_msg(`Election lost (${votes} votes, need ${MAJORITY}). Backing off.`);
    const termAtLoss = currentTerm;
    await new Promise(r => setTimeout(r, 300 + Math.random() * 500));
    if (currentTerm === termAtLoss) {
      becomeFollower(currentTerm - 1);
    } else {
      becomeFollower(currentTerm);
    }
  }
}

async function requestVotes() {
  let count = 1; // self-vote
  const lastLog = log[log.length - 1];
  const voteReq = {
    term        : currentTerm,
    candidateId : REPLICA_ID,
    lastLogIndex: lastLog ? lastLog.index : -1,
    lastLogTerm : lastLog ? lastLog.term  : -1,
  };

  await Promise.all(PEERS.map(async (peer) => {
    const controller = new AbortController();
    const tid = setTimeout(() => controller.abort(), 300);
    try {
      const res = await fetch(`${peer}/request-vote`, {
        method : 'POST',
        signal : controller.signal,
        headers: { 'Content-Type': 'application/json' },
        body   : JSON.stringify(voteReq),
      });
      clearTimeout(tid);
      const data = await res.json();
      if (data.voteGranted) { log_msg(`Vote granted by ${peer}`); count++; }
      if (data.term > currentTerm) becomeFollower(data.term);
    } catch (_) {
      // peer unavailable — ignore
    } finally {
      clearTimeout(tid);
    }
  }));

  return count;
}

// ── FIX: Heartbeats now call the dedicated /heartbeat RPC ────────────────────
// Previously, sendHeartbeats() called sendAppendEntries() which is the log
// replication RPC. The spec explicitly lists /heartbeat as a separate endpoint.
// The leader's heartbeat loop must use /heartbeat; log replication (sendAppendEntries)
// is only triggered when there are new entries to replicate.
async function sendHeartbeats() {
  if (role !== 'leader' || isShuttingDown) return;
  for (const peer of PEERS) {
    sendHeartbeatToPeer(peer).catch(() => {});
  }
}

async function sendHeartbeatToPeer(peer) {
  if (isShuttingDown) return;
  const controller = new AbortController();
  const tid = setTimeout(() => controller.abort(), 300);
  try {
    // FIX: call /heartbeat (not /append-entries) for the heartbeat loop.
    const res = await fetch(`${peer}/heartbeat`, {
      method : 'POST',
      signal : controller.signal,
      headers: { 'Content-Type': 'application/json' },
      body   : JSON.stringify({ term: currentTerm, leaderId: `http://${REPLICA_ID}:${PORT}` }),
    });
    clearTimeout(tid);
    const data = await res.json();
    if (data.term > currentTerm) {
      log_msg(`Stale leader detected via heartbeat from ${peer} (their term=${data.term})`);
      becomeFollower(data.term);
      return;
    }
    // If peer is behind on log, trigger a catch-up replication
    if (data.ok && (nextIndex[peer] ?? 0) < log.length) {
      sendAppendEntries(peer).catch(() => {});
    }
    peerAlive[peer]     = true;
    peerDeadSince[peer] = null;
  } catch (_) {
    peerAlive[peer]     = false;
    peerDeadSince[peer] = peerDeadSince[peer] ?? Date.now();
  } finally {
    clearTimeout(tid);
  }
}

// ── AppendEntries (log replication RPC — called only for new entries) ─────────
async function sendAppendEntries(peer) {
  if (isShuttingDown) return { success: false };

  if (!peerAlive[peer] && peerDeadSince[peer]) {
    if (Date.now() - peerDeadSince[peer] < PEER_DEAD_COOLDOWN_MS) {
      return { success: false };
    }
  }

  if (!peerAlive[peer]) {
    const probe = new AbortController();
    const tid   = setTimeout(() => probe.abort(), 200);
    try {
      await fetch(`${peer}/status`, { signal: probe.signal });
      peerAlive[peer]     = true;
      peerDeadSince[peer] = null;
    } catch (_) {
      clearTimeout(tid);
      peerDeadSince[peer] = Date.now();
      return { success: false };
    }
    clearTimeout(tid);
  }

  const controller = new AbortController();
  const timeoutId  = setTimeout(() => controller.abort(), 2000);

  const from          = nextIndex[peer] ?? 0;
  const entriesToSend = log.slice(from);
  const prevLogIndex  = from - 1;
  const prevLog       = prevLogIndex >= 0 ? log[prevLogIndex] : null;

  const body = {
    term        : currentTerm,
    leaderId    : `http://${REPLICA_ID}:${PORT}`,
    prevLogIndex: prevLog ? prevLog.index : -1,
    prevLogTerm : prevLog ? prevLog.term  : -1,
    entries     : entriesToSend,
    leaderCommit: commitIndex,
  };

  try {
    const res = await fetch(`${peer}/append-entries`, {
      method : 'POST',
      signal : controller.signal,
      headers: { 'Content-Type': 'application/json' },
      body   : JSON.stringify(body),
    });
    clearTimeout(timeoutId);
    peerAlive[peer] = true;
    const data = await res.json();

    if (data.term > currentTerm) {
      becomeFollower(data.term);
      return { success: false };
    }

    if (!data.success) {
      if (typeof data.logLength === 'number') {
        nextIndex[peer] = data.logLength;
      } else {
        nextIndex[peer] = Math.max(0, (nextIndex[peer] ?? 1) - 1);
      }
      await pushSyncLog(peer);
      return { success: false };
    }

    if (entriesToSend.length > 0) {
      const ackedIndex = entriesToSend[entriesToSend.length - 1].index;
      matchIndex[peer] = Math.max(matchIndex[peer] ?? -1, ackedIndex);
      nextIndex[peer]  = ackedIndex + 1;
    }

    return { success: true, peer };
  } catch (_) {
    clearTimeout(timeoutId);
    peerAlive[peer]     = false;
    peerDeadSince[peer] = peerDeadSince[peer] ?? Date.now();
    return { success: false };
  }
}

// ── Sync-log: push all missing committed entries to a lagging follower ────────
async function pushSyncLog(peer) {
  const from    = nextIndex[peer] ?? 0;
  const missing = log.slice(from);
  if (missing.length === 0) return;

  const controller = new AbortController();
  const tid        = setTimeout(() => controller.abort(), 2000);
  try {
    const res = await fetch(`${peer}/sync-log`, {
      method : 'POST',
      signal : controller.signal,
      headers: { 'Content-Type': 'application/json' },
      body   : JSON.stringify({ entries: missing, leaderCommit: commitIndex, term: currentTerm }),
    });
    clearTimeout(tid);
    const data = await res.json();
    if (data.ok) {
      const lastSynced    = missing[missing.length - 1].index;
      nextIndex[peer]     = lastSynced + 1;
      matchIndex[peer]    = Math.max(matchIndex[peer] ?? -1, lastSynced);
      peerAlive[peer]     = true;
      peerDeadSince[peer] = null;
      log_msg(`sync-log → ${peer}: pushed ${missing.length} entries, follower now at ${data.logLength}`);
    }
  } catch (_) {
    peerAlive[peer]     = false;
    peerDeadSince[peer] = peerDeadSince[peer] ?? Date.now();
  } finally {
    clearTimeout(tid);
  }
}

// ── Advance commitIndex using matchIndex (canonical RAFT §5.3) ────────────────
function tryAdvanceCommitIndex() {
  if (role !== 'leader') return;

  for (let n = log.length - 1; n > commitIndex; n--) {
    if (log[n].term !== currentTerm) continue;

    let count = 1;
    for (const peer of PEERS) {
      if ((matchIndex[peer] ?? -1) >= n) count++;
    }

    if (count >= MAJORITY) {
      for (let i = commitIndex + 1; i <= n; i++) {
        commitIndex        = i;
        lastCommittedIndex = i;
        const entry = log[i];
        if (entry && entry.data && entry.data.type !== 'noop') {
          broadcastEntry(entry.data);
        }
      }
      log_msg(`Committed up to index=${n}`);
      break;
    }
  }
}

async function broadcastEntry(message) {
  try {
    await fetch(`${GATEWAY_URL}/broadcast`, {
      method : 'POST',
      headers: { 'Content-Type': 'application/json' },
      body   : JSON.stringify(message),
    });
  } catch (err) {
    log_msg(`Broadcast failed: ${err.message}`);
  }
}

// ── Message replication (strokes and clear) ───────────────────────────────────
let replicationQueue = Promise.resolve();

function replicateMessage(message) {
  replicationQueue = replicationQueue
    .catch(() => {})
    .then(() => {
      const entry = { term: currentTerm, index: log.length, data: message };
      log.push(entry);
      return _replicateEntry(entry, message);
    });
  return replicationQueue;
}

async function _replicateEntry(entry, message) {
  if (role !== 'leader') return false;

  await Promise.allSettled(PEERS.map(peer => sendAppendEntries(peer)));
  tryAdvanceCommitIndex();

  const retryInterval = setInterval(async () => {
    if (role !== 'leader' || isShuttingDown) {
      clearInterval(retryInterval);
      return;
    }
    if (entry.index <= commitIndex) {
      clearInterval(retryInterval);
      return;
    }
    if (entry.index > lastCommittedIndex) {
      log_msg(`Still waiting for majority index=${entry.index}`);
    }
    await Promise.allSettled(PEERS.map(peer => sendAppendEntries(peer)));
    tryAdvanceCommitIndex();
  }, 500);

  setTimeout(() => clearInterval(retryInterval), 30000);

  return true;
}

// ── Express routes ────────────────────────────────────────────────────────────
const app = express();
app.use(express.json({ limit: '1mb' }));

app.get('/status', (req, res) => {
  res.set('Access-Control-Allow-Origin', '*');
  res.json({
    id         : REPLICA_ID,
    role,
    term       : currentTerm,
    leader     : leaderId,
    logLength  : log.length,
    commitIndex,
  });
});

app.post('/stroke', async (req, res) => {
  if (role !== 'leader') {
    return res.status(307).json({ redirect: leaderId });
  }
  const ok = await replicateMessage(req.body);
  res.json({ ok });
});

app.post('/request-vote', (req, res) => {
  const { term, candidateId, lastLogIndex, lastLogTerm } = req.body;

  if (role === 'leader' && term <= currentTerm) {
    return res.json({ term: currentTerm, voteGranted: false });
  }

  if (term > currentTerm) becomeFollower(term);

  const myLastLog   = log[log.length - 1];
  const myLastIndex = myLastLog ? myLastLog.index : -1;
  const myLastTerm  = myLastLog ? myLastLog.term  : -1;

  const logOk = lastLogTerm > myLastTerm ||
    (lastLogTerm === myLastTerm && lastLogIndex >= myLastIndex);

  const grant = term >= currentTerm &&
    (votedFor === null || votedFor === candidateId) &&
    logOk;

  if (grant) {
    votedFor = candidateId;
    resetElectionTimer();
    log_msg(`Voted for ${candidateId} in term ${term}`);
  }

  res.json({ term: currentTerm, voteGranted: grant });
});

app.post('/append-entries', (req, res) => {
  const { term, leaderId: lId, prevLogIndex, prevLogTerm, entries, leaderCommit } = req.body;

  if (term < currentTerm) {
    return res.json({ term: currentTerm, success: false, logLength: log.length });
  }

  becomeFollower(term, lId);

  if (prevLogIndex >= 0) {
    const ourEntry = log[prevLogIndex];
    if (!ourEntry || ourEntry.term !== prevLogTerm) {
      resetElectionTimer();
      return res.json({ term: currentTerm, success: false, logLength: log.length });
    }
  }

  if (entries && entries.length > 0) {
    for (const entry of entries) {
      if (entry.index < log.length) {
        if (log[entry.index].term !== entry.term) {
          log = log.slice(0, entry.index);
          log.push(entry);
        }
      } else {
        log.push(entry);
      }
    }
  }

  if (leaderCommit > commitIndex) {
    commitIndex = Math.min(leaderCommit, log.length - 1);
    lastApplied = commitIndex;
  }

  res.json({ term: currentTerm, success: true, logLength: log.length });
});

// FIX: /heartbeat is now the proper target for the leader's heartbeat loop.
// It resets the election timer and replies with the current term so the leader
// can detect if it has become stale. It does NOT do log consistency checks —
// that is /append-entries's job.
app.post('/heartbeat', (req, res) => {
  const { term, leaderId: lId } = req.body;
  if (term >= currentTerm) {
    becomeFollower(term, lId);
    resetElectionTimer();
    return res.json({ term: currentTerm, ok: true });
  }
  log_msg(`Rejecting stale heartbeat from ${lId} (their term=${term}, ours=${currentTerm})`);
  res.json({ term: currentTerm, ok: false });
});

app.post('/sync-log', (req, res) => {
  const { entries, leaderCommit, term } = req.body;
  if (term && term > currentTerm) becomeFollower(term);
  if (entries && entries.length > 0) {
    const fromIndex = entries[0].index;
    log         = log.slice(0, fromIndex).concat(entries);
    commitIndex = Math.min(leaderCommit, log.length - 1);
    lastApplied = commitIndex;
    log_msg(`sync-log received: from=${fromIndex} entries=${entries.length} commitIndex=${commitIndex}`);
  }
  res.json({ ok: true, logLength: log.length });
});

// FIX: /log now returns entries strictly after the last 'clear' entry in the
// committed log. This ensures the gateway's rebuildCanvasLog does not replay
// strokes that were already cleared by a prior clear command.
app.get('/log', (req, res) => {
  const from = parseInt(req.query.from ?? 0);

  // Find the index of the last committed 'clear' entry so we only return
  // strokes that are visible on the current canvas.
  let lastClearIndex = -1;
  for (let i = commitIndex; i >= 0; i--) {
    if (log[i] && log[i].data && log[i].data.type === 'clear') {
      lastClearIndex = i;
      break;
    }
  }

  const effectiveFrom = Math.max(from, lastClearIndex + 1);
  const entries = log.filter(e => e.index >= effectiveFrom && e.index <= commitIndex);
  res.json({ entries, commitIndex, lastClearIndex });
});

// ── Graceful shutdown ─────────────────────────────────────────────────────────
async function gracefulShutdown(signal) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  log_msg(`Received ${signal} — shutting down gracefully`);
  stopElectionTimer();
  clearInterval(heartbeatTimer);
  if (role === 'leader') {
    await sendHeartbeats();
    await new Promise(r => setTimeout(r, 200));
  }
  process.exit(0);
}

process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT',  () => gracefulShutdown('SIGINT'));

// ── Boot ──────────────────────────────────────────────────────────────────────
app.listen(PORT, () => {
  log_msg(`Listening on port ${PORT} | peers: ${PEERS.join(', ')}`);
  becomeFollower(0);
  resetElectionTimer();
});
