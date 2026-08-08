//! Single-group Raft consensus core ([`RaftNode`] / [`RaftConsensus`]).
//!
//! Soft state per node: [`Role`] (Follower / Candidate / Leader), `current_term`,
//! `voted_for`, and `commit_index`. Leader election uses randomized timeouts and
//! [`RequestVote`](crate::network); log replication / heartbeats use
//! [`AppendEntries`](crate::network) against a durable [`RaftLog`]. Committed
//! entries are applied to [`TakyonicEngine`] via
//! [`TakyonicEngine::apply_committed`] only after a majority quorum.
//!
//! Membership uses Ongaro's **single-server change** rule: a `ConfigChange`
//! takes effect for quorum calculations as soon as it is appended, and rolls
//! back if that log suffix is truncated.
//!
//! Vote safety (§5.4.1): a voter grants a ballot only when the candidate's log
//! is at least as up-to-date as its own ([`RaftLog::is_up_to_date`]).

/// Public name for the Raft node state machine (alias of [`RaftConsensus`]).
pub type RaftNode = RaftConsensus;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::{Notify, oneshot};
use tracing::{debug, info, warn};

use crate::engine::TakyonicEngine;
use crate::error::{Result, TakyonicError};
use crate::membership::ClusterMembership;
use crate::raft::{CommittedEntry, RaftCommand};
use crate::raft_log::{RaftLog, RaftLogEntry};

/// Soft state role.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Passive replica; accepts AppendEntries from the leader.
    Follower,
    /// Soliciting votes.
    Candidate,
    /// Accepts client proposals and replicates.
    Leader,
}

struct Waiter {
    index: u64,
    tx: oneshot::Sender<Result<u64>>,
}

/// Client proposal parked until durable batch + quorum commit.
struct PendingPropose {
    encoded: Bytes,
    tx: oneshot::Sender<Result<u64>>,
}

struct SoftState {
    current_term: u64,
    voted_for: Option<u64>,
    role: Role,
    leader_id: Option<u64>,
    commit_index: u64,
    /// Leader volatile: next entry to send per peer.
    next_index: HashMap<u64, u64>,
    /// Leader volatile: highest matched index per peer.
    match_index: HashMap<u64, u64>,
    /// Last time we heard from a leader / granted a vote / started election.
    last_heartbeat: Instant,
    /// When the current election attempt started (`None` if not candidate).
    election_started_at: Option<Instant>,
    election_timeout: Duration,
    waiters: Vec<Waiter>,
    /// Proposals waiting to be coalesced into a Raft log / AppendEntries batch.
    pending: Vec<PendingPropose>,
    /// In-flight InstallSnapshot receive buffer (follower).
    snapshot_buf: Option<(u64, u64, bytes::BytesMut)>,
    /// Membership as of the latest appended ConfigChange (or bootstrap/snapshot).
    membership: ClusterMembership,
    /// Membership frozen at the snapshot / committed-config base (for truncate rebuild).
    base_membership: ClusterMembership,
    /// Generation bumped on every membership mutation (PeerClients sync).
    membership_gen: u64,
    /// Set once RemoveNode(self) is committed — node becomes a passive learner.
    removed: bool,
    /// Per-peer cooldown for InstallSnapshot attempts (avoid SST-export storms).
    snapshot_attempt: HashMap<u64, Instant>,
}

/// Shared Raft node driving election + replication.
pub struct RaftConsensus {
    id: u64,
    raft_dir: PathBuf,
    log: RaftLog,
    engine: Arc<TakyonicEngine>,
    state: Mutex<SoftState>,
    /// Wakes the tick / replication loop.
    kick: Notify,
    /// Compact Raft log after this many in-memory entries (0 disables).
    snapshot_threshold: u64,
}

impl RaftConsensus {
    /// Create consensus state for `id` with bootstrap membership.
    pub fn new(
        id: u64,
        membership: ClusterMembership,
        log: RaftLog,
        engine: Arc<TakyonicEngine>,
    ) -> Arc<Self> {
        Self::new_with_threshold(id, membership, log, engine, 10_000)
    }

    /// Create consensus with an explicit Raft log snapshot threshold.
    pub fn new_with_threshold(
        id: u64,
        membership: ClusterMembership,
        log: RaftLog,
        engine: Arc<TakyonicEngine>,
        snapshot_threshold: u64,
    ) -> Arc<Self> {
        let raft_dir = log.dir().to_path_buf();
        // Prefer durable membership (post-snapshot / prior config change).
        let mut base = ClusterMembership::read_from_dir(&raft_dir)
            .ok()
            .flatten()
            .unwrap_or_else(|| membership.clone());
        if base.is_empty() && !membership.is_empty() {
            base = membership.clone();
        }
        let mut live = base.clone();
        // Replay ConfigChange entries still in the log (after snapshot boundary).
        for entry in log.entries_from(log.snapshot_meta().last_included_index.saturating_add(1)) {
            if let Ok(cmd) = RaftCommand::decode(entry.command.clone()) {
                apply_config_to(&mut live, &cmd);
            }
        }
        let _ = ClusterMembership::write_to_dir(&raft_dir, &live);

        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();
        let last = log.last_index() + 1;
        for p in live.peers_except(id) {
            next_index.insert(p, last);
            match_index.insert(p, 0);
        }
        let snap = log.snapshot_meta();
        let commit_index = snap.last_included_index;
        let removed = !live.contains(id) && !live.is_empty();
        Arc::new(Self {
            id,
            raft_dir,
            log,
            engine,
            state: Mutex::new(SoftState {
                current_term: 0,
                voted_for: None,
                role: Role::Follower,
                leader_id: None,
                commit_index,
                next_index,
                match_index,
                last_heartbeat: Instant::now(),
                election_started_at: None,
                election_timeout: randomized_election_timeout(id),
                waiters: Vec::new(),
                pending: Vec::new(),
                snapshot_buf: None,
                membership: live,
                base_membership: base,
                membership_gen: 1,
                removed,
                snapshot_attempt: HashMap::new(),
            }),
            kick: Notify::new(),
            snapshot_threshold,
        })
    }

    /// Local node id.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Current peer ids (voting members excluding self).
    pub fn peers(&self) -> Vec<u64> {
        self.state.lock().membership.peers_except(self.id)
    }

    /// Snapshot of the active membership.
    pub fn membership(&self) -> ClusterMembership {
        self.state.lock().membership.clone()
    }

    /// Membership generation — bumps when the voter set changes.
    pub fn membership_gen(&self) -> u64 {
        self.state.lock().membership_gen
    }

    /// True after this node has been removed from the cluster (committed).
    pub fn is_removed(&self) -> bool {
        self.state.lock().removed
    }

    /// Current role.
    pub fn role(&self) -> Role {
        self.state.lock().role
    }

    /// Current term.
    pub fn term(&self) -> u64 {
        self.state.lock().current_term
    }

    /// Candidate id this node voted for in the current term (`None` if unset).
    pub fn voted_for(&self) -> Option<u64> {
        self.state.lock().voted_for
    }

    /// Known leader, if any.
    pub fn leader_id(&self) -> Option<u64> {
        self.state.lock().leader_id
    }

    /// Commit index.
    pub fn commit_index(&self) -> u64 {
        self.state.lock().commit_index
    }

    /// Advertised `host:port` for the known leader, when membership has it.
    pub fn leader_address(&self) -> Option<String> {
        let st = self.state.lock();
        st.leader_id
            .and_then(|id| st.membership.address(id).map(str::to_string))
    }

    /// Durable Raft log.
    pub fn log(&self) -> &RaftLog {
        &self.log
    }

    /// Shared state machine.
    pub fn engine(&self) -> &Arc<TakyonicEngine> {
        &self.engine
    }

    /// Wake the background loop (after RPC or propose).
    pub fn kick(&self) {
        self.kick.notify_waiters();
    }

    /// Wait until woken or `timeout`.
    pub async fn wait_kick(&self, timeout: Duration) {
        let _ = tokio::time::timeout(timeout, self.kick.notified()).await;
    }

    /// Propose a command on the leader.
    ///
    /// Parks the caller while the leader coalesces pending proposals into one
    /// durable Raft-log batch and one multi-entry `AppendEntries` round. Returns
    /// after quorum commit (and local apply).
    pub async fn propose(&self, command: RaftCommand) -> Result<u64> {
        if command.is_config_change() && self.has_uncommitted_config_change() {
            return Err(TakyonicError::Raft(
                "previous configuration change is not yet committed".into(),
            ));
        }
        let encoded = command.encode()?;
        let (tx, rx) = oneshot::channel();
        {
            let mut st = self.state.lock();
            if st.removed {
                return Err(TakyonicError::Raft(
                    "node has been removed from the cluster".into(),
                ));
            }
            if st.role != Role::Leader {
                let leader_address = st
                    .leader_id
                    .and_then(|id| st.membership.address(id).map(str::to_string));
                return Err(TakyonicError::NotLeader { leader_address });
            }
            st.pending.push(PendingPropose { encoded, tx });
        }
        self.kick();
        match rx.await {
            Ok(Ok(index)) => Ok(index),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(TakyonicError::Raft("propose waiter dropped".into())),
        }
    }

    /// Propose AddNode (single-server change).
    pub async fn add_node(&self, id: u64, address: impl Into<String>) -> Result<u64> {
        self.propose(RaftCommand::add_node(id, address)).await
    }

    /// Propose RemoveNode (single-server change).
    pub async fn remove_node(&self, id: u64) -> Result<u64> {
        self.propose(RaftCommand::remove_node(id)).await
    }

    fn has_uncommitted_config_change(&self) -> bool {
        let st = self.state.lock();
        let commit = st.commit_index;
        drop(st);
        for idx in (commit + 1)..=self.log.last_index() {
            if let Some(entry) = self.log.entry(idx) {
                if let Ok(cmd) = RaftCommand::decode(entry.command) {
                    if cmd.is_config_change() {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Drain parked proposals into one Raft-log group commit.
    ///
    /// Called by the leader replication loop before broadcasting AppendEntries.
    /// Returns the number of entries appended.
    pub fn flush_pending_proposes(&self) -> Result<usize> {
        let (term, pending) = {
            let mut st = self.state.lock();
            if st.role != Role::Leader || st.removed {
                return Ok(0);
            }
            if st.pending.is_empty() {
                return Ok(0);
            }
            (st.current_term, std::mem::take(&mut st.pending))
        };
        let n = pending.len();
        let commands: Vec<Bytes> = pending.iter().map(|p| p.encoded.clone()).collect();
        let start = match self.log.append_commands(term, commands) {
            Ok(s) => s,
            Err(e) => {
                for p in pending {
                    let _ = p.tx.send(Err(TakyonicError::Raft(e.to_string())));
                }
                return Err(e);
            }
        };
        // Immediate-effect: apply ConfigChange entries as soon as they are appended.
        for i in 0..n {
            let index = start + i as u64;
            if let Some(entry) = self.log.entry(index) {
                if let Ok(cmd) = RaftCommand::decode(entry.command) {
                    if cmd.is_config_change() {
                        self.apply_config_change_immediate(&cmd);
                    }
                }
            }
        }
        let mut st = self.state.lock();
        if st.role != Role::Leader || st.current_term != term {
            for p in pending {
                let _ = p.tx.send(Err(TakyonicError::Raft(
                    "lost leadership during batched propose".into(),
                )));
            }
            return Err(TakyonicError::Raft(
                "lost leadership during batched propose".into(),
            ));
        }
        for (i, p) in pending.into_iter().enumerate() {
            let index = start + i as u64;
            st.waiters.push(Waiter { index, tx: p.tx });
        }
        let last = self.log.last_index();
        st.match_index.insert(self.id, last);
        advance_commit_index(&self.log, self.id, &mut st);
        let commit = st.commit_index;
        drop(st);
        if commit > 0 {
            let _ = self.apply_committed_to(commit);
            self.notify_waiters();
        }
        Ok(n)
    }

    /// True when the leader has parked client proposals waiting to flush.
    pub fn has_pending_proposes(&self) -> bool {
        !self.state.lock().pending.is_empty()
    }

    /// Handle an inbound RequestVote RPC.
    pub fn handle_request_vote(
        &self,
        term: u64,
        candidate_id: u64,
        last_log_index: u64,
        last_log_term: u64,
    ) -> (u64, bool) {
        let mut st = self.state.lock();
        if st.removed {
            return (st.current_term, false);
        }
        // Only voting members grant votes.
        if !st.membership.contains(self.id) {
            return (st.current_term, false);
        }
        if term < st.current_term {
            return (st.current_term, false);
        }
        if term > st.current_term {
            self.become_follower(&mut st, term, None);
        }
        let log_ok = self.log.is_up_to_date(last_log_index, last_log_term);
        let can_vote = st.voted_for.is_none_or(|v| v == candidate_id);
        if log_ok && can_vote {
            st.voted_for = Some(candidate_id);
            st.last_heartbeat = Instant::now();
            debug!(
                node = self.id,
                candidate = candidate_id,
                term = st.current_term,
                "granted vote"
            );
            (st.current_term, true)
        } else {
            (st.current_term, false)
        }
    }

    /// Handle an inbound AppendEntries RPC.
    pub fn handle_append_entries(
        &self,
        term: u64,
        leader_id: u64,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<RaftLogEntry>,
        leader_commit: u64,
    ) -> (u64, bool, u64) {
        let mut st = self.state.lock();
        if term < st.current_term {
            return (st.current_term, false, 0);
        }
        if term > st.current_term || st.role != Role::Follower {
            self.become_follower(&mut st, term, Some(leader_id));
        } else {
            st.leader_id = Some(leader_id);
            st.last_heartbeat = Instant::now();
        }

        // Log consistency check (snapshot boundary counts as a virtual entry).
        if prev_log_index > 0 {
            match self.log.term_at(prev_log_index) {
                Some(t) if t == prev_log_term => {}
                _ => {
                    return (st.current_term, false, self.log.last_index());
                }
            }
        }
        // Release soft-state lock before durable I/O.
        let term_now = st.current_term;
        drop(st);

        // Append new entries; truncate conflicts. Coalesce durable appends into
        // one group-commit batch for network group-commit symmetry.
        let snap_idx = self.log.snapshot_meta().last_included_index;
        let mut to_append: Vec<RaftLogEntry> = Vec::new();
        let mut truncated = false;
        for entry in &entries {
            if entry.index <= snap_idx {
                continue;
            }
            if let Some(existing) = self.log.term_at(entry.index) {
                if existing != entry.term {
                    if !to_append.is_empty() {
                        if let Err(e) = self.log.append_batch(&to_append) {
                            warn!(%e, "follower append failed");
                            return (term_now, false, self.log.last_index());
                        }
                        // Immediate-effect for the batch we just wrote.
                        self.apply_appended_config_changes(&to_append);
                        to_append.clear();
                    }
                    self.log.truncate_after(entry.index - 1);
                    truncated = true;
                    to_append.push(entry.clone());
                }
            } else if entry.index == self.log.last_index() + 1 + to_append.len() as u64 {
                to_append.push(entry.clone());
            } else if entry.index > self.log.last_index() + 1 + to_append.len() as u64 {
                return (term_now, false, self.log.last_index());
            }
        }
        if truncated {
            self.rebuild_membership_from_log();
        }
        if !to_append.is_empty() {
            if let Err(e) = self.log.append_batch(&to_append) {
                warn!(%e, "follower batch append failed");
                return (term_now, false, self.log.last_index());
            }
            self.apply_appended_config_changes(&to_append);
        }

        let match_index = self.log.last_index();
        let commit = {
            let mut st = self.state.lock();
            if leader_commit > st.commit_index {
                st.commit_index = leader_commit.min(match_index);
            }
            st.commit_index
        };
        if let Err(e) = self.apply_committed_to(commit) {
            warn!(%e, "follower apply failed");
        }
        (self.state.lock().current_term, true, match_index)
    }

    /// Handle InstallSnapshot — may arrive in chunks (`done == false` until last).
    pub fn handle_install_snapshot(
        &self,
        term: u64,
        leader_id: u64,
        last_included_index: u64,
        last_included_term: u64,
        data: Bytes,
        done: bool,
    ) -> Result<u64> {
        {
            let mut st = self.state.lock();
            if term < st.current_term {
                return Ok(st.current_term);
            }
            self.become_follower(&mut st, term, Some(leader_id));
        }

        {
            let mut st = self.state.lock();
            match &mut st.snapshot_buf {
                Some((idx, term_s, buf))
                    if *idx == last_included_index && *term_s == last_included_term =>
                {
                    buf.extend_from_slice(&data);
                }
                _ => {
                    let mut buf = bytes::BytesMut::with_capacity(data.len());
                    buf.extend_from_slice(&data);
                    st.snapshot_buf = Some((last_included_index, last_included_term, buf));
                }
            }
        }

        if !done {
            return Ok(self.state.lock().current_term);
        }

        let blob = {
            let mut st = self.state.lock();
            let Some((idx, term_s, buf)) = st.snapshot_buf.take() else {
                return Err(TakyonicError::Raft(
                    "InstallSnapshot done without buffered data".into(),
                ));
            };
            if idx != last_included_index || term_s != last_included_term {
                return Err(TakyonicError::Raft(
                    "InstallSnapshot chunk metadata mismatch".into(),
                ));
            }
            buf.freeze()
        };

        let membership =
            self.engine
                .install_sst_snapshot(blob, last_included_index, last_included_term)?;
        self.log
            .install_snapshot(last_included_index, last_included_term)?;
        {
            let mut st = self.state.lock();
            if last_included_index > st.commit_index {
                st.commit_index = last_included_index;
            }
            if !membership.is_empty() {
                st.base_membership = membership.clone();
                st.membership = membership;
                st.membership_gen = st.membership_gen.saturating_add(1);
                st.removed = !st.membership.contains(self.id);
                let _ = ClusterMembership::write_to_dir(&self.raft_dir, &st.membership);
            }
        }
        info!(
            node = self.id,
            last_included_index, last_included_term, "applied InstallSnapshot"
        );
        Ok(self.state.lock().current_term)
    }

    /// True when the leader must catch `peer` up via InstallSnapshot.
    pub fn peer_needs_snapshot(&self, peer: u64) -> bool {
        let mut st = self.state.lock();
        if st.role != Role::Leader {
            return false;
        }
        let next = *st.next_index.get(&peer).unwrap_or(&1);
        let snap = self.log.snapshot_meta();
        if snap.last_included_index == 0 || next > snap.last_included_index {
            return false;
        }
        // Cooldown: exporting SSTs every heartbeat for a dead peer starves the cluster.
        const COOLDOWN: Duration = Duration::from_secs(2);
        if let Some(prev) = st.snapshot_attempt.get(&peer) {
            if prev.elapsed() < COOLDOWN {
                return false;
            }
        }
        st.snapshot_attempt.insert(peer, Instant::now());
        true
    }

    /// Unreachable peer: back off next_index; jump to snapshot only after repeated lag.
    pub fn on_peer_unreachable(&self, peer: u64) {
        let mut st = self.state.lock();
        if st.role != Role::Leader {
            return;
        }
        let next = st.next_index.entry(peer).or_insert(1);
        let snap = self.log.snapshot_meta().last_included_index;
        // Halve toward the snapshot boundary rather than jumping every failure
        // (which would trigger perpetual InstallSnapshot rebuilds).
        if snap > 0 && *next > snap {
            *next = ((*next + snap) / 2).max(snap);
        } else {
            *next = (*next).saturating_sub(1).max(1);
        }
    }

    /// Build an InstallSnapshot payload for a lagging follower.
    pub fn build_install_snapshot(&self) -> Result<(u64, u64, u64, u64, Bytes)> {
        let st = self.state.lock();
        if st.role != Role::Leader {
            return Err(TakyonicError::Raft("not leader".into()));
        }
        let term = st.current_term;
        let membership = st.membership.clone();
        drop(st);
        let snap = self.log.snapshot_meta();
        if snap.last_included_index == 0 {
            // Force a compact so a brand-new joiner can receive SST state.
            // Export whatever is currently applied.
            let applied = self.engine.last_applied();
            if applied == 0 {
                return Err(TakyonicError::Raft("no local snapshot to install".into()));
            }
            let Some(t) = self.log.term_at(applied) else {
                return Err(TakyonicError::Raft("missing term at applied index".into()));
            };
            self.engine.force_flush()?;
            self.log.compact_through(applied, t)?;
        }
        let snap = self.log.snapshot_meta();
        if snap.last_included_index == 0 {
            return Err(TakyonicError::Raft("no local snapshot to install".into()));
        }
        let data = self.engine.export_sst_snapshot(
            snap.last_included_index,
            snap.last_included_term,
            membership,
        )?;
        Ok((
            term,
            self.id,
            snap.last_included_index,
            snap.last_included_term,
            data,
        ))
    }

    /// After a successful InstallSnapshot, advance peer cursors.
    pub fn on_snapshot_success(&self, peer: u64, last_included_index: u64) {
        let mut st = self.state.lock();
        if st.role != Role::Leader {
            return;
        }
        st.match_index.insert(peer, last_included_index);
        st.next_index.insert(peer, last_included_index + 1);
        st.snapshot_attempt.remove(&peer);
        advance_commit_index(&self.log, self.id, &mut st);
    }

    /// Transition to candidate and begin an election (caller sends RequestVote).
    pub fn start_election(&self) -> (u64, u64, u64) {
        let mut st = self.state.lock();
        if st.removed || !st.membership.contains(self.id) {
            return (st.current_term, self.log.last_index(), self.log.last_term());
        }
        st.current_term += 1;
        st.role = Role::Candidate;
        st.voted_for = Some(self.id);
        st.leader_id = None;
        st.last_heartbeat = Instant::now();
        st.election_started_at = Some(st.last_heartbeat);
        st.election_timeout = randomized_election_timeout(self.id ^ st.current_term);
        for w in st.waiters.drain(..) {
            let _ = w.tx.send(Err(TakyonicError::Raft(
                "leadership lost before commit".into(),
            )));
        }
        for p in st.pending.drain(..) {
            let _ = p.tx.send(Err(TakyonicError::Raft(
                "leadership lost before batch flush".into(),
            )));
        }
        let term = st.current_term;
        info!(node = self.id, term, "starting election");
        (term, self.log.last_index(), self.log.last_term())
    }

    /// Record a granted vote; return true if we won quorum this term.
    pub fn record_vote_granted(
        &self,
        term: u64,
        from: u64,
        votes: &mut HashMap<u64, bool>,
    ) -> bool {
        let st = self.state.lock();
        if st.role != Role::Candidate || st.current_term != term {
            return false;
        }
        if !st.membership.contains(from) && from != self.id {
            return false;
        }
        votes.insert(from, true);
        let granted = votes.values().filter(|&&v| v).count() + 1; // +1 self
        granted >= st.membership.quorum()
    }

    /// Become leader for `term`.
    pub fn become_leader(&self, term: u64) {
        let mut st = self.state.lock();
        if st.current_term != term || st.role != Role::Candidate {
            return;
        }
        if let Some(started) = st.election_started_at.take() {
            self.engine.metrics().record_raft_election(started.elapsed());
        } else {
            self.engine
                .metrics()
                .record_raft_election(Duration::from_micros(0));
        }
        st.role = Role::Leader;
        st.leader_id = Some(self.id);
        st.last_heartbeat = Instant::now();
        let last = self.log.last_index();
        let peers = st.membership.peers_except(self.id);
        for &p in &peers {
            st.next_index.insert(p, last + 1);
            st.match_index.insert(p, 0);
        }
        st.match_index.insert(self.id, last);
        // Raft §5.4.2: append a current-term noop so prior-term entries
        // (including ConfigChange) can be committed under the new leader.
        if let Ok(encoded) = RaftCommand::noop().encode() {
            st.pending.push(PendingPropose {
                encoded,
                tx: {
                    let (tx, _rx) = oneshot::channel();
                    tx
                },
            });
        }
        info!(node = self.id, term, last_index = last, "became leader");
        self.kick();
    }

    /// Step down if remote term is higher.
    pub fn maybe_step_down(&self, remote_term: u64) {
        let mut st = self.state.lock();
        if remote_term > st.current_term {
            self.become_follower(&mut st, remote_term, None);
        }
    }

    /// Whether election timeout has elapsed (followers/candidates).
    pub fn election_timed_out(&self) -> bool {
        let st = self.state.lock();
        if st.removed || !st.membership.contains(self.id) {
            return false;
        }
        st.role != Role::Leader && st.last_heartbeat.elapsed() >= st.election_timeout
    }

    /// Snapshot of leader replication cursors for one peer.
    pub fn leader_peer_cursor(
        &self,
        peer: u64,
    ) -> Option<(u64, u64, u64, u64, Vec<RaftLogEntry>, u64)> {
        let st = self.state.lock();
        if st.role != Role::Leader {
            return None;
        }
        let next = *st.next_index.get(&peer).unwrap_or(&1);
        let snap = self.log.snapshot_meta();
        if snap.last_included_index > 0 && next <= snap.last_included_index {
            // Caller should use InstallSnapshot instead.
            return None;
        }
        let prev = next.saturating_sub(1);
        let prev_term = self.log.term_at(prev).unwrap_or(0);
        let entries = self.log.entries_from(next);
        // Network group-commit: ship the entire pending suffix (cap guards RPC size).
        const MAX_BATCH: usize = 2048;
        let entries: Vec<_> = entries.into_iter().take(MAX_BATCH).collect();
        Some((
            st.current_term,
            self.id,
            prev,
            prev_term,
            entries,
            st.commit_index,
        ))
    }

    /// Ensure a newly added peer has replication cursors (call after AddNode).
    pub fn ensure_peer_cursors(&self, peer: u64) {
        let mut st = self.state.lock();
        if st.role != Role::Leader {
            return;
        }
        let last = self.log.last_index();
        st.next_index.entry(peer).or_insert(last + 1);
        // New peers start at match 0 so they need catch-up / snapshot.
        st.match_index.entry(peer).or_insert(0);
        // Force InstallSnapshot path when we already have a compacted prefix.
        let snap = self.log.snapshot_meta().last_included_index;
        if snap > 0 {
            st.next_index.insert(peer, snap);
        }
    }

    /// Update leader progress after a successful AppendEntries response.
    pub fn on_append_success(&self, peer: u64, match_index: u64) {
        let commit = {
            let mut st = self.state.lock();
            if st.role != Role::Leader {
                return;
            }
            st.match_index.insert(peer, match_index);
            st.next_index.insert(peer, match_index + 1);
            advance_commit_index(&self.log, self.id, &mut st);
            st.commit_index
        };
        if let Err(e) = self.apply_committed_to(commit) {
            warn!(%e, "leader apply failed");
        }
        self.notify_waiters();
    }

    /// Back off next_index on AppendEntries rejection.
    pub fn on_append_failure(&self, peer: u64, remote_term: u64, hint: u64) {
        let mut st = self.state.lock();
        if remote_term > st.current_term {
            self.become_follower(&mut st, remote_term, None);
            return;
        }
        if st.role != Role::Leader {
            return;
        }
        let next = st.next_index.entry(peer).or_insert(1);
        // Jump toward the follower's reported tip (match_index hint on conflict).
        if hint > 0 {
            *next = (hint + 1).min(*next).max(1);
        } else {
            *next = (*next).saturating_sub(1).max(1);
        }
    }

    /// Apply entries through `commit_index` to the engine.
    pub fn apply_committed_to(&self, commit_index: u64) -> Result<()> {
        let last_applied = self.engine.last_applied();
        if commit_index <= last_applied {
            return Ok(());
        }
        let snap_idx = self.log.snapshot_meta().last_included_index;
        let start = (last_applied + 1).max(snap_idx + 1);
        if start > commit_index {
            return Ok(());
        }
        let mut batch = Vec::new();
        let mut committed_removes = Vec::new();
        for idx in start..=commit_index {
            let Some(entry) = self.log.entry(idx) else {
                break;
            };
            let command = RaftCommand::decode(entry.command.clone())?;
            if let RaftCommand::RemoveNode { id } = &command {
                if *id == self.id {
                    committed_removes.push(idx);
                }
            }
            batch.push(CommittedEntry::new(idx, command));
        }
        if batch.is_empty() {
            return Ok(());
        }
        self.engine.apply_committed(&batch)?;
        // Freeze base membership at the highest committed ConfigChange.
        {
            let mut st = self.state.lock();
            let mut base = st.base_membership.clone();
            let mut changed = false;
            for e in &batch {
                if e.command.is_config_change() {
                    apply_config_to(&mut base, &e.command);
                    changed = true;
                }
            }
            if changed {
                st.base_membership = base;
                let _ = ClusterMembership::write_to_dir(&self.raft_dir, &st.membership);
            }
            if !committed_removes.is_empty() {
                st.removed = true;
                if st.role == Role::Leader {
                    info!(node = self.id, "RemoveNode(self) committed — stepping down");
                    st.role = Role::Follower;
                    st.leader_id = None;
                }
            }
        }
        self.maybe_compact()?;
        Ok(())
    }

    /// If the in-memory Raft log exceeds the threshold, flush SSTs and compact.
    pub fn maybe_compact(&self) -> Result<()> {
        if self.snapshot_threshold == 0 {
            return Ok(());
        }
        if (self.log.len() as u64) < self.snapshot_threshold {
            return Ok(());
        }
        let applied = self.engine.last_applied();
        if applied == 0 {
            return Ok(());
        }
        let snap = self.log.snapshot_meta();
        if applied <= snap.last_included_index {
            return Ok(());
        }
        let Some(term) = self.log.term_at(applied) else {
            return Ok(());
        };
        self.engine.force_flush()?;
        self.log.compact_through(applied, term)?;
        {
            let st = self.state.lock();
            // After compaction, base membership should reflect committed state.
            let _ = ClusterMembership::write_to_dir(&self.raft_dir, &st.membership);
        }
        info!(
            node = self.id,
            last_included_index = applied,
            last_included_term = term,
            remaining = self.log.len(),
            "Raft log compacted via SST snapshot"
        );
        Ok(())
    }

    /// Notify propose waiters whose index is now committed.
    pub fn notify_waiters(&self) {
        let commit = {
            let st = self.state.lock();
            st.commit_index
        };
        let mut st = self.state.lock();
        let mut rest = Vec::new();
        for w in st.waiters.drain(..) {
            if w.index <= commit {
                let _ = w.tx.send(Ok(w.index));
            } else {
                rest.push(w);
            }
        }
        st.waiters = rest;
    }

    fn become_follower(&self, st: &mut SoftState, term: u64, leader: Option<u64>) {
        st.current_term = term;
        st.role = Role::Follower;
        st.voted_for = None;
        st.leader_id = leader;
        st.last_heartbeat = Instant::now();
        st.election_started_at = None;
        st.election_timeout = randomized_election_timeout(self.id ^ term);
        for w in st.waiters.drain(..) {
            let _ = w.tx.send(Err(TakyonicError::Raft(
                "leadership lost before commit".into(),
            )));
        }
        for p in st.pending.drain(..) {
            let _ = p.tx.send(Err(TakyonicError::Raft(
                "leadership lost before batch flush".into(),
            )));
        }
    }

    fn apply_config_change_immediate(&self, cmd: &RaftCommand) {
        let peer_to_seed = match cmd {
            RaftCommand::AddNode { id, .. } => Some(*id),
            _ => None,
        };
        // For AddNode: ensure a snapshot boundary exists so the joiner can be
        // caught up via InstallSnapshot rather than shipping the entire log.
        if peer_to_seed.is_some() {
            let is_leader = self.state.lock().role == Role::Leader;
            if is_leader
                && self.log.snapshot_meta().last_included_index == 0
                && self.engine.last_applied() > 0
            {
                let applied = self.engine.last_applied();
                if let Some(t) = self.log.term_at(applied) {
                    let _ = self.engine.force_flush();
                    let _ = self.log.compact_through(applied, t);
                }
            }
        }

        let mut st = self.state.lock();
        let before = st.membership.clone();
        apply_config_to(&mut st.membership, cmd);
        if st.membership != before {
            st.membership_gen = st.membership_gen.saturating_add(1);
            info!(
                node = self.id,
                members = ?st.membership.members().collect::<Vec<_>>(),
                quorum = st.membership.quorum(),
                "membership updated (immediate effect)"
            );
            if let Some(peer) = peer_to_seed {
                if st.role == Role::Leader {
                    st.match_index.insert(peer, 0);
                    let snap = self.log.snapshot_meta().last_included_index;
                    if snap > 0 {
                        st.next_index.insert(peer, snap);
                    } else {
                        st.next_index.insert(peer, 1);
                    }
                }
            }
            if let RaftCommand::RemoveNode { id } = cmd {
                st.next_index.remove(id);
                st.match_index.remove(id);
            }
            let _ = ClusterMembership::write_to_dir(&self.raft_dir, &st.membership);
        }
    }

    fn apply_appended_config_changes(&self, entries: &[RaftLogEntry]) {
        for entry in entries {
            if let Ok(cmd) = RaftCommand::decode(entry.command.clone()) {
                if cmd.is_config_change() {
                    self.apply_config_change_immediate(&cmd);
                }
            }
        }
    }

    fn rebuild_membership_from_log(&self) {
        let (mut live, snap_idx) = {
            let st = self.state.lock();
            (
                st.base_membership.clone(),
                self.log.snapshot_meta().last_included_index,
            )
        };
        for entry in self.log.entries_from(snap_idx.saturating_add(1)) {
            if let Ok(cmd) = RaftCommand::decode(entry.command) {
                if cmd.is_config_change() {
                    apply_config_to(&mut live, &cmd);
                }
            }
        }
        let mut st = self.state.lock();
        if st.membership != live {
            st.membership = live;
            st.membership_gen = st.membership_gen.saturating_add(1);
            info!(
                node = self.id,
                members = ?st.membership.members().collect::<Vec<_>>(),
                "membership rebuilt after log truncate"
            );
            let _ = ClusterMembership::write_to_dir(&self.raft_dir, &st.membership);
        }
    }
}

fn apply_config_to(membership: &mut ClusterMembership, cmd: &RaftCommand) {
    match cmd {
        RaftCommand::AddNode { id, address } => {
            membership.add_node(*id, address.clone());
        }
        RaftCommand::RemoveNode { id } => {
            membership.remove_node(*id);
        }
        _ => {}
    }
}

fn advance_commit_index(log: &RaftLog, self_id: u64, st: &mut SoftState) {
    if st.role != Role::Leader {
        return;
    }
    let term = st.current_term;
    let last = log.last_index();
    // Ensure self match_index reflects local log (only if still a voter).
    if st.membership.contains(self_id) {
        let local = st.match_index.entry(self_id).or_insert(0);
        *local = (*local).max(last);
    }

    let quorum = st.membership.quorum();
    let voters: Vec<u64> = st.membership.members().collect();

    for n in (st.commit_index + 1)..=last {
        // Only commit current-term entries (Raft §5.4.2).
        if log.term_at(n) != Some(term) {
            continue;
        }
        let mut count = 0usize;
        for &p in &voters {
            if *st.match_index.get(&p).unwrap_or(&0) >= n {
                count += 1;
            }
        }
        if count >= quorum {
            st.commit_index = n;
        }
    }
}

fn randomized_election_timeout(seed: u64) -> Duration {
    // 300–600ms — comfortable for local loopback demos.
    let ms = 300 + (seed.wrapping_mul(0x9e37_79b9) % 300);
    Duration::from_millis(ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_engine(name: &str) -> (Arc<TakyonicEngine>, std::path::PathBuf) {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!("takyonic-cons-{name}-{nanos}"));
        let engine = Arc::new(
            TakyonicEngine::open(
                Config::default()
                    .data_dir(root.join("data"))
                    .wal_dir(root.join("wal"))
                    .memtable_size_bytes(64 * 1024 * 1024)
                    .l0_rapid_pool_threads(1)
                    .ln_haul_pool_threads(1)
                    .compaction_write_bytes_per_sec(1024 * 1024 * 1024),
            )
            .unwrap(),
        );
        (engine, root)
    }

    #[test]
    fn vote_requires_up_to_date_log() {
        let (engine, root) = temp_engine("vote");
        let log = RaftLog::open(root.join("raft")).unwrap();
        log.append(RaftLogEntry::new(2, 1, Bytes::from_static(b"x")))
            .unwrap();
        let membership = ClusterMembership::from_endpoints([
            (1, "127.0.0.1:1".into()),
            (2, "127.0.0.1:2".into()),
            (3, "127.0.0.1:3".into()),
        ]);
        let raft = RaftConsensus::new(1, membership, log, engine);
        // Stale candidate log must be rejected even after stepping up in term.
        let (term, granted) = raft.handle_request_vote(1, 2, 0, 0);
        assert!(!granted);
        assert_eq!(term, 1);
        // Candidate with matching last log entry is granted the vote.
        let (term, granted) = raft.handle_request_vote(3, 2, 1, 2);
        assert!(granted);
        assert_eq!(term, 3);
        assert_eq!(raft.voted_for(), Some(2));
        raft.log.shutdown().unwrap();
        raft.engine.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    /// Mock (no network): Follower → Candidate → Leader on a single-voter node.
    #[test]
    fn mock_election_follower_candidate_leader() {
        let (engine, root) = temp_engine("elect");
        let log = RaftLog::open(root.join("raft")).unwrap();
        let membership =
            ClusterMembership::from_endpoints([(1, "127.0.0.1:1".into())]);
        let raft: Arc<RaftNode> = RaftConsensus::new(1, membership, log, engine);
        assert_eq!(raft.role(), Role::Follower);
        assert_eq!(raft.term(), 0);
        assert!(raft.voted_for().is_none());

        let (term, last_idx, last_term) = raft.start_election();
        assert_eq!(raft.role(), Role::Candidate);
        assert_eq!(term, 1);
        assert_eq!(raft.voted_for(), Some(1));
        assert_eq!(last_idx, 0);
        assert_eq!(last_term, 0);

        raft.become_leader(term);
        assert_eq!(raft.role(), Role::Leader);
        assert_eq!(raft.leader_id(), Some(1));
        assert_eq!(raft.term(), 1);

        raft.log.shutdown().unwrap();
        raft.engine.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn add_node_raises_quorum_immediately() {
        let (engine, root) = temp_engine("cfg");
        let log = RaftLog::open(root.join("raft")).unwrap();
        let membership = ClusterMembership::from_endpoints([
            (1, "127.0.0.1:1".into()),
            (2, "127.0.0.1:2".into()),
            (3, "127.0.0.1:3".into()),
        ]);
        let raft = RaftConsensus::new(1, membership, log, engine);
        assert_eq!(raft.membership().quorum(), 2);
        raft.apply_config_change_immediate(&RaftCommand::add_node(4, "127.0.0.1:4"));
        assert_eq!(raft.membership().quorum(), 3);
        assert!(raft.membership().contains(4));
        raft.log.shutdown().unwrap();
        raft.engine.close().unwrap();
        let _ = std::fs::remove_dir_all(root);
    }
}
