import express from 'express';
import fetch from 'node-fetch';

const REPLICA_ID   = process.env.REPLICA_ID;
const PORT         = parseInt(process.env.PORT);
const PEERS        = process.env.PEERS.split(',').filter(Boolean);
const GATEWAY_URL  = 'http://gateway:8081';

const HEARTBEAT_MS     = 300;
const ELECTION_TIMEOUT = () => 1500 + Math.random() * 1500;

let role           = 'follower';
let currentTerm    = 0;
let votedFor       = null;
let log            = [];
let commitIndex    = -1;
let lastApplied    = -1;
let lastHeartbeat  = Date.now();
let electionTimer  = null;
let heartbeatTimer = null;
let leaderId       = null;
let isShuttingDown = false;

const nextIndex  = {};
const inFlight   = {};
const peerAlive  = {};
PEERS.forEach(p => { nextIndex[p] = 0; inFlight[p] = false; peerAlive[p] = true; });

function logMsg(...args) {
  console.log(`[${REPLICA_ID}][${role.toUpperCase()}][term=${currentTerm}]`, ...args);
}

function resetElectionTimer() {
  if (isShuttingDown) return;
  clearTimeout(electionTimer);
  electionTimer = setTimeout(startElection, ELECTION_TIMEOUT());
}

function stopElectionTimer() {
  clearTimeout(electionTimer);
}

function becomeFollower(term, leader = null) {
  if (term < currentTerm) return;
  const changed = term > currentTerm || leader !== leaderId || role !== 'follower';
  role = 'follower';
  currentTerm = term;
  votedFor = null;
  leaderId = leader;
  clearInterval(heartbeatTimer);
  heartbeatTimer = null;
  if (changed) logMsg(`→ FOLLOWER (new term=${term}, leader=${leader})`);
  resetElectionTimer();
}

async function becomeLeader() {
  if (isShuttingDown) return;
  logMsg('→ LEADER');
  role = 'leader';
  leaderId = `http://${REPLICA_ID}:${PORT}`;
  stopElectionTimer();
  PEERS.forEach(p => { nextIndex[p] = log.length; inFlight[p] = false; peerAlive[p] = true; });
  sendHeartbeats();
  heartbeatTimer = setInterval(sendHeartbeats, HEARTBEAT_MS);
  for (let i = 0; i < 3; i++) {
    try {
      await fetch(`${GATEWAY_URL}/leader-announce`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ leader: `http://${REPLICA_ID}:${PORT}` }),
      });
      logMsg('Announced leadership to gateway');
      break;
    } catch (_) {
      await new Promise(r => setTimeout(r, 300));
    }
  }
}

async function startElection() {
  if (isShuttingDown || role === 'leader') return;
  role = 'candidate';
  currentTerm++;
  votedFor = REPLICA_ID;
  leaderId = null;
  logMsg(`Starting election for term ${currentTerm}`);
  const votes = await requestVotes();
  const majority = Math.floor((PEERS.length + 1) / 2) + 1;
  if (votes >= majority && role === 'candidate' && !isShuttingDown) {
    becomeLeader();
  } else if (role === 'candidate') {
    logMsg(`Election lost (${votes} votes, need ${majority}). Retrying.`);
    await new Promise(r => setTimeout(r, 150 + Math.random() * 150));
    becomeFollower(currentTerm);
  }
}

async function requestVotes() {
  let count = 1;
  const lastLog = log[log.length - 1];
  const voteReq = {
    term:         currentTerm,
    candidateId:  REPLICA_ID,
    lastLogIndex: lastLog ? lastLog.index : -1,
    lastLogTerm:  lastLog ? lastLog.term  : -1,
  };
  await Promise.all(PEERS.map(async (peer) => {
    const controller = new AbortController();
    const tid = setTimeout(() => controller.abort(), 300);
    try {
      const res = await fetch(`${peer}/request-vote`, {
        method: 'POST',
        signal: controller.signal,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(voteReq),
      });
      const data = await res.json();
      if (data.voteGranted) { logMsg(`Vote from ${peer}`); count++; }
      if (data.term > currentTerm) becomeFollower(data.term);
    } catch (_) {
    } finally {
      clearTimeout(tid);
    }
  }));
  return count;
}

async function sendHeartbeats() {
  if (role !== 'leader' || isShuttingDown) return;
  await Promise.all(PEERS.map(peer => sendAppendEntries(peer)));
}

async function sendAppendEntries(peer) {
  if (isShuttingDown) return { success: false };
  if (inFlight[peer]) return { success: false };

  // If peer was marked dead, probe it cheaply before attempting full AE
  if (!peerAlive[peer]) {
    const probe = new AbortController();
    const tid = setTimeout(() => probe.abort(), 20);
    try {
      await fetch(`${peer}/status`, { signal: probe.signal });
      peerAlive[peer] = true; // recovered
    } catch (_) {
      clearTimeout(tid);
      return { success: false }; // still dead, skip instantly
    }
    clearTimeout(tid);
  }

  inFlight[peer] = true;
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 200);

  const from = nextIndex[peer] ?? log.length;
  const entriesToSend = log.slice(from);
  const prevLogIndex = from - 1;
  const prevLog = prevLogIndex >= 0 ? log[prevLogIndex] : null;

  const body = {
    term:         currentTerm,
    leaderId:     `http://${REPLICA_ID}:${PORT}`,
    prevLogIndex: prevLog ? prevLog.index : -1,
    prevLogTerm:  prevLog ? prevLog.term  : -1,
    entries:      entriesToSend,
    leaderCommit: commitIndex,
  };

  try {
    const res = await fetch(`${peer}/append-entries`, {
      method: 'POST',
      signal: controller.signal,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    clearTimeout(timeoutId);
    peerAlive[peer] = true;
    const data = await res.json();

    if (data.term > currentTerm) {
      becomeFollower(data.term);
      return { success: false };
    }

    if (!data.success) {
      const followerLen = data.logLength ?? 0;
      nextIndex[peer] = Math.max(0, Math.min((nextIndex[peer] ?? 1) - 1, followerLen));
      syncLog(peer);
      return { success: false };
    }

    if (entriesToSend.length > 0) {
      nextIndex[peer] = entriesToSend[entriesToSend.length - 1].index + 1;
    }
    return { success: true };
  } catch (_) {
    clearTimeout(timeoutId);
    peerAlive[peer] = false; // mark dead — skip instantly next time
    return { success: false };
  } finally {
    inFlight[peer] = false;
  }
}

async function syncLog(peer) {
  const from = nextIndex[peer] ?? 0;
  const missing = log.slice(from);
  if (missing.length === 0) return;
  const controller = new AbortController();
  const tid = setTimeout(() => controller.abort(), 500);
  try {
    await fetch(`${peer}/sync-log`, {
      method: 'POST',
      signal: controller.signal,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ entries: missing, leaderCommit: commitIndex, term: currentTerm }),
    });
    nextIndex[peer] = log.length;
    logMsg(`Synced ${missing.length} entries to ${peer}`);
  } catch (_) {
  } finally {
    clearTimeout(tid);
  }
}

let replicationQueue = Promise.resolve();

async function replicateStroke(stroke) {
  const result = await (replicationQueue = replicationQueue.then(() => _replicateStroke(stroke)));
  return result;
}

async function _replicateStroke(stroke) {
  if (role !== 'leader') return false;

  const entry = { term: currentTerm, index: log.length, data: stroke };
  log.push(entry);

  const majority = Math.floor((PEERS.length + 1) / 2) + 1;
  let acks = 1;
  let committed = false;

  await Promise.all(PEERS.map(async (peer) => {
    const result = await sendAppendEntries(peer);
    if (result?.success) {
      acks++;
      if (acks >= majority && !committed) {
        committed = true;
        await commitAndBroadcast(entry, stroke);
      }
    }
  }));

  if (!committed) {
    log.pop();
    PEERS.forEach(p => { nextIndex[p] = log.length; });
    logMsg(`No majority for index=${entry.index}, rolled back`);
  }
  return committed;
}

let lastCommittedIndex = -1;
async function commitAndBroadcast(entry, stroke) {
  if (entry.index <= lastCommittedIndex) return;
  lastCommittedIndex = entry.index;
  commitIndex = Math.max(commitIndex, entry.index);
  try {
    await fetch(`${GATEWAY_URL}/broadcast`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(stroke),
    });
  } catch (err) {
    logMsg(`Broadcast failed: ${err.message}`);
  }
}

const app = express();
app.use(express.json());

app.get('/status', (req, res) => {
  res.json({ id: REPLICA_ID, role, term: currentTerm, leader: leaderId, logLength: log.length, commitIndex });
});

app.post('/stroke', async (req, res) => {
  if (role !== 'leader') return res.status(307).json({ redirect: leaderId });
  const ok = await replicateStroke(req.body);
  res.json({ ok });
});

app.post('/request-vote', (req, res) => {
  const { term, candidateId, lastLogIndex, lastLogTerm } = req.body;
  if (term > currentTerm) becomeFollower(term);
  const myLastLog   = log[log.length - 1];
  const myLastIndex = myLastLog ? myLastLog.index : -1;
  const myLastTerm  = myLastLog ? myLastLog.term  : -1;
  const logOk = lastLogTerm > myLastTerm ||
    (lastLogTerm === myLastTerm && lastLogIndex >= myLastIndex);
  const grant = term >= currentTerm &&
    (votedFor === null || votedFor === candidateId) &&
    logOk &&
    role !== 'leader';
  if (grant) {
    votedFor = candidateId;
    resetElectionTimer();
    logMsg(`Voted for ${candidateId} in term ${term}`);
  }
  res.json({ term: currentTerm, voteGranted: grant });
});

app.post('/append-entries', (req, res) => {
  const { term, leaderId: lId, prevLogIndex, prevLogTerm, entries, leaderCommit } = req.body;
  if (term < currentTerm) return res.json({ term: currentTerm, success: false, logLength: log.length });
  becomeFollower(term, lId);
  lastHeartbeat = Date.now();
  if (prevLogIndex >= 0) {
    const ourEntry = log[prevLogIndex];
    if (!ourEntry || ourEntry.term !== prevLogTerm) {
      logMsg(`Rejected AE: prevLogIndex=${prevLogIndex} prevLogTerm=${prevLogTerm} (len=${log.length})`);
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
  if (leaderCommit > commitIndex) commitIndex = Math.min(leaderCommit, log.length - 1);
  res.json({ term: currentTerm, success: true, logLength: log.length });
});

app.post('/sync-log', (req, res) => {
  const { entries, leaderCommit, term } = req.body;
  if (term && term > currentTerm) becomeFollower(term);
  if (entries && entries.length > 0) {
    const fromIndex = entries[0].index;
    log = log.slice(0, fromIndex).concat(entries);
    commitIndex = Math.min(leaderCommit, log.length - 1);
    lastApplied = commitIndex;
    logMsg(`Synced from index ${fromIndex}, log=${log.length}, commitIndex=${commitIndex}`);
  }
  res.json({ ok: true, logLength: log.length });
});

app.post('/heartbeat', (req, res) => {
  const { term, leaderId: lId } = req.body;
  if (term >= currentTerm) { becomeFollower(term, lId); lastHeartbeat = Date.now(); }
  res.json({ term: currentTerm });
});

app.get('/log', (req, res) => {
  const from = parseInt(req.query.from ?? 0);
  res.json({ entries: log.filter(e => e.index >= from && e.index <= commitIndex), commitIndex });
});

async function gracefulShutdown() {
  if (isShuttingDown) return;
  isShuttingDown = true;
  logMsg('Shutting down...');
  stopElectionTimer();
  clearInterval(heartbeatTimer);
  if (role === 'leader') { await sendHeartbeats(); await new Promise(r => setTimeout(r, 200)); }
  process.exit(0);
}

process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

app.listen(PORT, () => {
  logMsg(`Listening on port ${PORT}`);
  becomeFollower(0);
  resetElectionTimer();
});