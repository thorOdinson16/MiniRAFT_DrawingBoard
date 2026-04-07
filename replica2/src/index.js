import express from 'express';
import fetch from 'node-fetch';

const REPLICA_ID  = process.env.REPLICA_ID;
const PORT        = parseInt(process.env.PORT);
const PEERS       = process.env.PEERS.split(',').filter(Boolean);
const GATEWAY_URL = 'http://gateway:8081';

// ── Spec-compliant timing constants ──────────────────────────────────────────
const HEARTBEAT_MS     = 150;                          // spec: 150 ms
const ELECTION_TIMEOUT = () => 500 + Math.random() * 300; // spec: 500–800 ms

// ── RAFT state ────────────────────────────────────────────────────────────────
let role              = 'follower';
let currentTerm       = 0;
let votedFor          = null;
let log               = [];
let commitIndex       = -1;
let lastApplied       = -1;
let electionTimer     = null;
let heartbeatTimer    = null;
let leaderId          = null;
let isShuttingDown    = false;
let lastCommittedIndex = -1;

// Per-peer replication tracking
const nextIndex     = {};
const inFlight      = {};
const peerAlive     = {};
const peerDeadSince = {};
const PEER_DEAD_COOLDOWN_MS = 3000;
PEERS.forEach(p => { nextIndex[p] = 0; inFlight[p] = false; peerAlive[p] = true; peerDeadSince[p] = null; });

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
}

// ── State transitions ─────────────────────────────────────────────────────────
function becomeFollower(term, leader = null) {
  if (term < currentTerm) return;
  const changed = term > currentTerm || leader !== leaderId || role !== 'follower';
  // Stop heartbeat if we were leader
  clearInterval(heartbeatTimer);
  heartbeatTimer = null;
  role        = 'follower';
  currentTerm = term;
  votedFor    = null;
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
  // Initialise per-peer nextIndex to end of our log
  PEERS.forEach(p => { nextIndex[p] = log.length; inFlight[p] = false; peerAlive[p] = true; });
  // Immediately send heartbeats so followers don't start another election
  sendHeartbeats();
  heartbeatTimer = setInterval(sendHeartbeats, HEARTBEAT_MS);
  // Announce to gateway (retry 3 times)
  for (let i = 0; i < 3; i++) {
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
}

// ── Election ──────────────────────────────────────────────────────────────────
async function startElection() {
  if (isShuttingDown || role === 'leader') return;
  role        = 'candidate';
  currentTerm += 1;
  votedFor    = REPLICA_ID;
  leaderId    = null;
  log_msg(`Starting election for term ${currentTerm}`);

  const votes    = await requestVotes();
  const majority = Math.floor((PEERS.length + 1) / 2) + 1;

  if (votes >= majority && role === 'candidate' && !isShuttingDown) {
    await becomeLeader();
  } else if (role === 'candidate') {
    log_msg(`Election lost (${votes} votes, need ${majority}). Backing off.`);
    // Random back-off before retrying to avoid split-vote loops
    await new Promise(r => setTimeout(r, 150 + Math.random() * 150));
    becomeFollower(currentTerm);
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
      const res  = await fetch(`${peer}/request-vote`, {
        method : 'POST',
        signal : controller.signal,
        headers: { 'Content-Type': 'application/json' },
        body   : JSON.stringify(voteReq),
      });
      const data = await res.json();
      if (data.voteGranted) { log_msg(`Vote granted by ${peer}`); count++; }
      // If we discover a higher term, immediately revert to follower
      if (data.term > currentTerm) becomeFollower(data.term);
    } catch (_) {
      // peer unavailable — ignore
    } finally {
      clearTimeout(tid);
    }
  }));

  return count;
}

// ── Heartbeats / AppendEntries ─────────────────────────────────────────────────
async function sendHeartbeats() {
  if (role !== 'leader' || isShuttingDown) return;
  await Promise.all(PEERS.map(peer => sendAppendEntries(peer)));
}

async function sendAppendEntries(peer) {
  if (isShuttingDown)  return { success: false };
  if (inFlight[peer])  return { success: false };

  // Skip peer in cooldown period (dead for PEER_DEAD_COOLDOWN_MS)
  if (!peerAlive[peer] && peerDeadSince[peer]) {
    const now = Date.now();
    if (now - peerDeadSince[peer] < PEER_DEAD_COOLDOWN_MS) {
      return { success: false };
    }
  }

  // Fast dead-peer probe: skip immediately if known dead, probe cheaply first
  if (!peerAlive[peer]) {
    const probe = new AbortController();
    const tid   = setTimeout(() => probe.abort(), 200);
    try {
      await fetch(`${peer}/status`, { signal: probe.signal });
      peerAlive[peer] = true;
    } catch (_) {
      clearTimeout(tid);
      return { success: false };
    }
    clearTimeout(tid);
  }

  inFlight[peer] = true;
  const controller = new AbortController();
  const timeoutId  = setTimeout(() => controller.abort(), 1000);

  const from          = nextIndex[peer] ?? log.length;
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

    // Higher term discovered → step down immediately
    if (data.term > currentTerm) {
      becomeFollower(data.term);
      return { success: false };
    }

    if (!data.success) {
      // Follower log doesn't match — back off and push a full sync
      const followerLen    = data.logLength ?? 0;
      nextIndex[peer]      = Math.max(0, Math.min((nextIndex[peer] ?? 1) - 1, followerLen));
      pushSyncLog(peer);   // spec: leader → follower sync
      return { success: false };
    }

    if (entriesToSend.length > 0) {
      nextIndex[peer] = entriesToSend[entriesToSend.length - 1].index + 1;
    }
    return { success: true };
  } catch (_) {
    clearTimeout(timeoutId);
    peerAlive[peer] = false;
    peerDeadSince[peer] = Date.now();
    return { success: false };
  } finally {
    inFlight[peer] = false;
  }
}

// ── /sync-log: leader pushes missing committed entries to a lagging follower ──
// Spec §4 Catch-Up: leader calls /sync-log on the follower with all entries
// from its current log length onward.
async function pushSyncLog(peer) {
  const from    = nextIndex[peer] ?? 0;
  const missing = log.slice(from);
  if (missing.length === 0) return;

  const controller = new AbortController();
  const tid        = setTimeout(() => controller.abort(), 500);
  try {
    await fetch(`${peer}/sync-log`, {
      method : 'POST',
      signal : controller.signal,
      headers: { 'Content-Type': 'application/json' },
      body   : JSON.stringify({ entries: missing, leaderCommit: commitIndex, term: currentTerm }),
    });
    nextIndex[peer] = log.length;
    log_msg(`sync-log → ${peer}: pushed ${missing.length} entries`);
  } catch (_) {
    // will retry on next heartbeat cycle
  } finally {
    clearTimeout(tid);
  }
}

// ── Message replication (strokes and clear) ────────────────────────────────────
// Serialised queue so concurrent HTTP requests don't interleave log entries.
let replicationQueue = Promise.resolve();

function replicateMessage(message) {
  replicationQueue = replicationQueue
    .catch(() => {})  // prevent stuck chain on error
    .then(() => _replicateMessage(message));
  return replicationQueue;
}

async function _replicateMessage(message) {
  if (role !== 'leader') return false;

  const entry = { term: currentTerm, index: log.length, data: message };
  log.push(entry);

  const majority = Math.floor((PEERS.length + 1) / 2) + 1;
  let acks      = 1;
  let committed = false;

  await Promise.all(PEERS.map(async (peer) => {
    const result = await sendAppendEntries(peer);
    if (result?.success) {
      acks++;
      if (acks >= majority && !committed) {
        committed = true;
        await commitAndBroadcast(entry, message);
      }
    }
  }));

  if (!committed) {
    log.pop();
    PEERS.forEach(p => { nextIndex[p] = log.length; peerAlive[p] = true; });
    log_msg(`No majority for index=${entry.index} — rolled back`);
  }
  return committed;
}

async function commitAndBroadcast(entry, message) {
  if (entry.index <= lastCommittedIndex) return;
  lastCommittedIndex = entry.index;
  commitIndex        = Math.max(commitIndex, entry.index);
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

// ── Express routes ────────────────────────────────────────────────────────────
const app = express();
app.use(express.json({ limit: '1mb' }));

// Health / status (used by gateway discovery + docker healthcheck)
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

// Message entry-point (leader only; followers redirect)
app.post('/stroke', async (req, res) => {
  if (role !== 'leader') {
    return res.status(307).json({ redirect: leaderId });
  }
  const ok = await replicateMessage(req.body);
  res.json({ ok });
});

// RAFT: RequestVote RPC
app.post('/request-vote', (req, res) => {
  const { term, candidateId, lastLogIndex, lastLogTerm } = req.body;

  // Higher term always wins
  if (term > currentTerm) becomeFollower(term);

  const myLastLog   = log[log.length - 1];
  const myLastIndex = myLastLog ? myLastLog.index : -1;
  const myLastTerm  = myLastLog ? myLastLog.term  : -1;

  // Candidate's log must be at least as up-to-date as ours
  const logOk = lastLogTerm > myLastTerm ||
    (lastLogTerm === myLastTerm && lastLogIndex >= myLastIndex);

  const grant = term >= currentTerm &&
    (votedFor === null || votedFor === candidateId) &&
    logOk &&
    role !== 'leader'; // leaders never grant votes (prevents disruption)

  if (grant) {
    votedFor = candidateId;
    resetElectionTimer(); // reset timer so we don't also start an election
    log_msg(`Voted for ${candidateId} in term ${term}`);
  }

  res.json({ term: currentTerm, voteGranted: grant });
});

// RAFT: AppendEntries RPC (also serves as heartbeat when entries=[])
app.post('/append-entries', (req, res) => {
  const { term, leaderId: lId, prevLogIndex, prevLogTerm, entries, leaderCommit } = req.body;

  // Reject stale leaders
  if (term < currentTerm) {
    return res.json({ term: currentTerm, success: false, logLength: log.length });
  }

  // Valid leader — become follower and reset election timer
  becomeFollower(term, lId);

  // Consistency check
  if (prevLogIndex >= 0) {
    const ourEntry = log[prevLogIndex];
    if (!ourEntry || ourEntry.term !== prevLogTerm) {
      log_msg(`AE rejected: prevLogIndex=${prevLogIndex} prevLogTerm=${prevLogTerm} (our len=${log.length})`);
      return res.json({ term: currentTerm, success: false, logLength: log.length });
    }
  }

  // Append new entries, handling conflicts
  if (entries && entries.length > 0) {
    for (const entry of entries) {
      if (entry.index < log.length) {
        if (log[entry.index].term !== entry.term) {
          log = log.slice(0, entry.index); // truncate conflicting suffix
          log.push(entry);
        }
        // identical entry already present — skip
      } else {
        log.push(entry);
      }
    }
  }

  // Advance commit index
  if (leaderCommit > commitIndex) {
    commitIndex = Math.min(leaderCommit, log.length - 1);
  }

  res.json({ term: currentTerm, success: true, logLength: log.length });
});

// RAFT: Heartbeat RPC (separate endpoint as required by spec)
// In this implementation AppendEntries already acts as heartbeat, but this
// endpoint satisfies the spec requirement and provides an explicit fast-path.
app.post('/heartbeat', (req, res) => {
  const { term, leaderId: lId } = req.body;
  if (term >= currentTerm) {
    becomeFollower(term, lId);
  }
  res.json({ term: currentTerm, ok: true });
});

// RAFT: /sync-log — follower receives full missing entries from leader
// Spec §4 Catch-Up: leader calls this after follower reports a shorter log.
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

// Debug: read log entries
app.get('/log', (req, res) => {
  const from = parseInt(req.query.from ?? 0);
  res.json({
    entries    : log.filter(e => e.index >= from && e.index <= commitIndex),
    commitIndex,
  });
});

// ── Graceful shutdown ─────────────────────────────────────────────────────────
async function gracefulShutdown(signal) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  log_msg(`Received ${signal} — shutting down gracefully`);
  stopElectionTimer();
  clearInterval(heartbeatTimer);
  // If leader: send one last heartbeat so followers don't immediately time out
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
  becomeFollower(0);    // start as follower in term 0
  resetElectionTimer(); // begin waiting for a leader
});