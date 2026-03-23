use std::sync::Arc;
use tokio::sync::{Mutex, Notify, mpsc};

use crate::raft::{
    ApplyMsg,
    log::{LogEntry, RaftLog},
    replication,
    rpc::{AppendEntriesArgs, AppendEntriesReply},
    state::{LeaderState, RaftState, Role},
    storage::RaftStorage,
};

use super::mock_transport::MockTransport;

// region helpers

fn make_storage() -> (Arc<RaftStorage>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let s = Arc::new(RaftStorage::new(dir.path().to_path_buf()).unwrap());
    (s, dir)
}

fn make_follower_state(id: &str, peers: &[&str]) -> Arc<Mutex<RaftState>> {
    Arc::new(Mutex::new(RaftState::new(
        id.into(),
        peers.iter().map(|s| s.to_string()).collect(),
    )))
}

/// Make a state that is already a Leader with a given term and one peer.
fn make_leader_state(id: &str, peer: &str, term: u64) -> Arc<Mutex<RaftState>> {
    let mut state = RaftState::new(id.into(), vec![peer.into()]);
    state.persistent.current_term = term;
    state.role = Role::Leader;
    state.volatile.current_leader = Some(id.into());
    state.leader_state = Some(LeaderState::initialise(&[peer.into()], 0));
    Arc::new(Mutex::new(state))
}

fn empty_log() -> Arc<Mutex<RaftLog<u32>>> {
    Arc::new(Mutex::new(RaftLog::new()))
}

fn make_apply_channel() -> (mpsc::Sender<ApplyMsg<u32>>, mpsc::Receiver<ApplyMsg<u32>>) {
    mpsc::channel(64)
}

fn heartbeat_args(term: u64, leader_id: &str) -> AppendEntriesArgs<u32> {
    AppendEntriesArgs {
        term,
        leader_id: leader_id.into(),
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit: 0,
    }
}

// endregion

// region handle_append_entries - rejection cases

#[tokio::test]
async fn rejects_stale_term() {
    let (storage, _dir) = make_storage();
    let state = make_follower_state("n1", &[]);
    state.lock().await.persistent.current_term = 5;

    let log = empty_log();
    let (tx, _rx) = make_apply_channel();
    let timer_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let timer_flag = Arc::clone(&timer_called);

    let args = AppendEntriesArgs {
        term: 4, // stale
        leader_id: "leader".into(),
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit: 0,
    };

    let reply =
        replication::handle_append_entries(args, state.clone(), log, storage, tx, move || {
            timer_flag.store(true, std::sync::atomic::Ordering::SeqCst);
        })
        .await;

    assert!(!reply.success);
    assert_eq!(reply.term, 5);
    // Timer must NOT be reset for stale RPCs.
    assert!(!timer_called.load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test]
async fn resets_timer_on_valid_rpc() {
    let (storage, _dir) = make_storage();
    let state = make_follower_state("n1", &[]);
    let log = empty_log();
    let (tx, _rx) = make_apply_channel();

    let timer_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let timer_flag = Arc::clone(&timer_called);

    let reply = replication::handle_append_entries(
        heartbeat_args(1, "leader"),
        state,
        log,
        storage,
        tx,
        move || {
            timer_flag.store(true, std::sync::atomic::Ordering::SeqCst);
        },
    )
    .await;

    assert!(reply.success);
    assert!(timer_called.load(std::sync::atomic::Ordering::SeqCst));
}

#[tokio::test]
async fn steps_down_on_higher_term() {
    let (storage, _dir) = make_storage();
    let state = make_follower_state("n1", &[]);
    state.lock().await.persistent.current_term = 3;

    let log = empty_log();
    let (tx, _rx) = make_apply_channel();

    replication::handle_append_entries(
        heartbeat_args(7, "leader"),
        state.clone(),
        log,
        storage,
        tx,
        || {},
    )
    .await;

    let sg = state.lock().await;
    assert_eq!(sg.persistent.current_term, 7);
    assert_eq!(sg.role, Role::Follower);
}

#[tokio::test]
async fn records_leader_id() {
    let (storage, _dir) = make_storage();
    let state = make_follower_state("n1", &[]);
    let log = empty_log();
    let (tx, _rx) = make_apply_channel();

    replication::handle_append_entries(
        heartbeat_args(1, "leader42"),
        state.clone(),
        log,
        storage,
        tx,
        || {},
    )
    .await;

    assert_eq!(
        state.lock().await.volatile.current_leader,
        Some("leader42".into())
    );
}

#[tokio::test]
async fn rejects_missing_prev_log_entry() {
    let (storage, _dir) = make_storage();
    let state = make_follower_state("n1", &[]);
    let log = empty_log(); // empty log
    let (tx, _rx) = make_apply_channel();

    let args = AppendEntriesArgs {
        term: 1,
        leader_id: "leader".into(),
        prev_log_index: 5, // follower has nothing at index 5
        prev_log_term: 1,
        entries: vec![],
        leader_commit: 0,
    };

    let reply = replication::handle_append_entries(args, state, log, storage, tx, || {}).await;

    assert!(!reply.success);
    assert!(reply.conflict_index.is_some());
}

#[tokio::test]
async fn rejects_prev_log_term_mismatch() {
    let (storage, _dir) = make_storage();
    let state = make_follower_state("n1", &[]);
    let log = empty_log();
    {
        let mut lg = log.lock().await;
        lg.append_command(1, 0u32); // index 1, term 1
    }
    let (tx, _rx) = make_apply_channel();

    let args = AppendEntriesArgs {
        term: 2,
        leader_id: "leader".into(),
        prev_log_index: 1,
        prev_log_term: 2, // mismatch — our index 1 has term 1
        entries: vec![],
        leader_commit: 0,
    };

    let reply = replication::handle_append_entries(args, state, log, storage, tx, || {}).await;

    assert!(!reply.success);
    assert!(reply.conflict_index.is_some());
    assert!(reply.conflict_term.is_some());
}

#[tokio::test]
async fn appends_entries_successfully() {
    let (storage, _dir) = make_storage();
    let state = make_follower_state("n1", &[]);
    let log = empty_log();
    let (tx, _rx) = make_apply_channel();

    let args = AppendEntriesArgs {
        term: 1,
        leader_id: "leader".into(),
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![
            LogEntry {
                term: 1,
                index: 1,
                command: 10u32,
            },
            LogEntry {
                term: 1,
                index: 2,
                command: 20u32,
            },
        ],
        leader_commit: 0,
    };

    let reply =
        replication::handle_append_entries(args, state, log.clone(), storage, tx, || {}).await;

    assert!(reply.success);
    assert_eq!(log.lock().await.last_index(), 2);
}

#[tokio::test]
async fn advances_commit_index_on_append() {
    let (storage, _dir) = make_storage();
    let state = make_follower_state("n1", &[]);
    let log = empty_log();
    let (tx, mut rx) = make_apply_channel();

    let args = AppendEntriesArgs {
        term: 1,
        leader_id: "leader".into(),
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![LogEntry {
            term: 1,
            index: 1,
            command: 42u32,
        }],
        leader_commit: 1,
    };

    let reply =
        replication::handle_append_entries(args, state.clone(), log, storage, tx, || {}).await;

    assert!(reply.success);
    assert_eq!(state.lock().await.volatile.commit_index, 1);

    // An ApplyMsg should have been sent.
    let msg = rx.try_recv().expect("expected ApplyMsg");
    match msg {
        ApplyMsg::Command { command, .. } => assert_eq!(command, 42),
    }
}

#[tokio::test]
async fn idempotent_on_duplicate_entries() {
    let (storage, _dir) = make_storage();
    let state = make_follower_state("n1", &[]);
    let log = empty_log();
    log.lock().await.append_command(1, 7u32);

    let (tx, _rx) = make_apply_channel();

    // Re-send the same entry.
    let args = AppendEntriesArgs {
        term: 1,
        leader_id: "leader".into(),
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![LogEntry {
            term: 1,
            index: 1,
            command: 7u32,
        }],
        leader_commit: 0,
    };

    let reply =
        replication::handle_append_entries(args, state, log.clone(), storage, tx, || {}).await;

    assert!(reply.success);
    assert_eq!(log.lock().await.last_index(), 1); // not doubled
}

#[tokio::test]
async fn candidate_steps_down_on_valid_heartbeat() {
    let (storage, _dir) = make_storage();
    let state = make_follower_state("n1", &[]);
    state.lock().await.role = Role::Candidate;
    state.lock().await.persistent.current_term = 1;

    let log = empty_log();
    let (tx, _rx) = make_apply_channel();

    replication::handle_append_entries(
        heartbeat_args(1, "leader"),
        state.clone(),
        log,
        storage,
        tx,
        || {},
    )
    .await;

    assert_eq!(state.lock().await.role, Role::Follower);
}

// endregion

// region advance_commit_index

#[tokio::test]
async fn no_advance_if_not_leader() {
    let state = make_follower_state("n1", &[]);
    // Follower — no leader_state.
    let log = empty_log();
    log.lock().await.append_command(1, 0u32);

    let (tx, mut rx) = make_apply_channel();
    replication::advance_commit_index(state.clone(), log, tx).await;

    assert_eq!(state.lock().await.volatile.commit_index, 0);
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn no_advance_if_previous_term_entry() {
    // Leader in term 2, but the only log entry is from term 1.
    // Raft §5.4: must not commit previous-term entries directly.
    let state = make_leader_state("n1", "n2", 2);
    let log = empty_log();
    {
        let mut lg = log.lock().await;
        lg.append_command(1, 0u32); // index 1, term 1 (old term)
    }
    // Give n2 a match_index of 1 so it "has" the entry.
    state
        .lock()
        .await
        .leader_state
        .as_mut()
        .unwrap()
        .peers
        .get_mut("n2")
        .unwrap()
        .match_index = 1;

    let (tx, mut rx) = make_apply_channel();
    replication::advance_commit_index(state.clone(), log, tx).await;

    // commit_index must remain 0 — we cannot commit a previous-term entry.
    assert_eq!(state.lock().await.volatile.commit_index, 0);
    assert!(rx.try_recv().is_err());
}

#[tokio::test]
async fn advances_when_majority_match() {
    // 3-node cluster: leader + 2 peers.
    let state = {
        let mut s = RaftState::new("n1".into(), vec!["n2".into(), "n3".into()]);
        s.persistent.current_term = 1;
        s.role = Role::Leader;
        s.volatile.current_leader = Some("n1".into());
        s.leader_state = Some(LeaderState::initialise(&["n2".into(), "n3".into()], 0));
        Arc::new(Mutex::new(s))
    };
    let log = empty_log();
    log.lock().await.append_command(1, 99u32); // index 1, term 1

    // Give both peers match_index = 1 → 3/3 have the entry → commit.
    {
        let mut sg = state.lock().await;
        let ls = sg.leader_state.as_mut().unwrap();
        ls.peers.get_mut("n2").unwrap().match_index = 1;
        ls.peers.get_mut("n3").unwrap().match_index = 1;
    }

    let (tx, mut rx) = make_apply_channel();
    replication::advance_commit_index(state.clone(), log, tx).await;

    assert_eq!(state.lock().await.volatile.commit_index, 1);
    let msg = rx.try_recv().expect("expected ApplyMsg");
    match msg {
        ApplyMsg::Command { index, command, .. } => {
            assert_eq!(index, 1);
            assert_eq!(command, 99);
        }
    }
}

#[tokio::test]
async fn does_not_advance_without_majority() {
    // 5-node cluster: leader + 4 peers.  Only 1 peer has matched.
    let peers: Vec<String> = (2..=5).map(|i| format!("n{i}")).collect();
    let state = {
        let mut s = RaftState::new("n1".into(), peers.clone());
        s.persistent.current_term = 1;
        s.role = Role::Leader;
        s.leader_state = Some(LeaderState::initialise(&peers, 0));
        Arc::new(Mutex::new(s))
    };
    let log = empty_log();
    log.lock().await.append_command(1, 0u32);

    // Only one peer has the entry.
    state
        .lock()
        .await
        .leader_state
        .as_mut()
        .unwrap()
        .peers
        .get_mut("n2")
        .unwrap()
        .match_index = 1;

    let (tx, mut rx) = make_apply_channel();
    replication::advance_commit_index(state.clone(), log, tx).await;

    assert_eq!(state.lock().await.volatile.commit_index, 0);
    assert!(rx.try_recv().is_err());
}

// endregion

// region apply_commited_entries

#[tokio::test]
async fn noop_when_already_applied() {
    let state = make_follower_state("n1", &[]);
    {
        let mut sg = state.lock().await;
        sg.volatile.commit_index = 2;
        sg.volatile.last_applied = 2;
    }
    let log = empty_log();
    {
        let mut lg = log.lock().await;
        lg.append_command(1, 10u32);
        lg.append_command(1, 20u32);
    }

    let (tx, mut rx) = make_apply_channel();
    replication::apply_committed_entries(state.clone(), log, tx).await;

    assert!(rx.try_recv().is_err(), "no messages expected");
}

#[tokio::test]
async fn applies_range_in_order() {
    let state = make_follower_state("n1", &[]);
    {
        let mut sg = state.lock().await;
        sg.volatile.commit_index = 3;
        sg.volatile.last_applied = 0;
    }
    let log = empty_log();
    {
        let mut lg = log.lock().await;
        lg.append_command(1, 10u32);
        lg.append_command(1, 20u32);
        lg.append_command(1, 30u32);
    }

    let (tx, mut rx) = make_apply_channel();
    replication::apply_committed_entries(state.clone(), log, tx).await;

    let commands: Vec<u32> = (0..3)
        .map(|_| match rx.try_recv().unwrap() {
            ApplyMsg::Command { command, .. } => command,
        })
        .collect();
    assert_eq!(commands, vec![10, 20, 30]);
}

#[tokio::test]
async fn advances_last_applied() {
    let state = make_follower_state("n1", &[]);
    {
        let mut sg = state.lock().await;
        sg.volatile.commit_index = 2;
        sg.volatile.last_applied = 0;
    }
    let log = empty_log();
    {
        let mut lg = log.lock().await;
        lg.append_command(1, 1u32);
        lg.append_command(1, 2u32);
    }

    let (tx, _rx) = make_apply_channel();
    replication::apply_committed_entries(state.clone(), log, tx).await;

    assert_eq!(state.lock().await.volatile.last_applied, 2);
}

// endregion

// region replicate_to_peer

#[tokio::test]
async fn returns_not_leader_if_not_leader() {
    let (storage, _dir) = make_storage();
    let state = make_follower_state("n1", &["n2"]);
    let log = empty_log();
    let transport = MockTransport::<u32>::new();
    let notify = Arc::new(Notify::new());

    let result =
        replication::replicate_to_peer(&"n2".to_string(), state, log, transport, storage, notify)
            .await;

    assert!(matches!(result, Err(crate::raft::RaftError::NotLeader)));
}

#[tokio::test]
async fn updates_match_and_next_index_on_success() {
    let (storage, _dir) = make_storage();
    let state = make_leader_state("n1", "n2", 1);
    let log = empty_log();
    {
        let mut lg = log.lock().await;
        lg.append_command(1, 1u32);
        lg.append_command(1, 2u32);
    }
    // next_index for n2 starts at 1 (leader's log was empty at election time).
    let transport = MockTransport::<u32>::new();
    transport.queue_append_entries_reply(
        "n2",
        Ok(AppendEntriesReply {
            term: 1,
            success: true,
            conflict_index: None,
            conflict_term: None,
        }),
    );
    let notify = Arc::new(Notify::new());

    replication::replicate_to_peer(
        &"n2".to_string(),
        state.clone(),
        log,
        transport,
        storage,
        notify.clone(),
    )
    .await
    .unwrap();

    let sg = state.lock().await;
    let ps = &sg.leader_state.as_ref().unwrap().peers["n2"];
    // prev_log_index was 0 (next_index - 1 = 1 - 1), entries.len() = 2
    assert_eq!(ps.match_index, 2);
    assert_eq!(ps.next_index, 3);
}

#[tokio::test]
async fn notifies_commit_on_success() {
    let (storage, _dir) = make_storage();
    let state = make_leader_state("n1", "n2", 1);
    let log = empty_log();
    log.lock().await.append_command(1, 0u32);

    let transport = MockTransport::<u32>::new();
    transport.queue_append_entries_reply(
        "n2",
        Ok(AppendEntriesReply {
            term: 1,
            success: true,
            conflict_index: None,
            conflict_term: None,
        }),
    );
    let notify = Arc::new(Notify::new());

    replication::replicate_to_peer(
        &"n2".to_string(),
        state,
        log,
        transport,
        storage,
        notify.clone(),
    )
    .await
    .unwrap();

    // Notify should have been signalled — try_recv via a small future.
    let received =
        tokio::time::timeout(std::time::Duration::from_millis(10), notify.notified()).await;
    assert!(received.is_ok(), "commit_notify should have been signalled");
}

#[tokio::test]
async fn steps_down_on_higher_term_reply() {
    let (storage, _dir) = make_storage();
    let state = make_leader_state("n1", "n2", 1);
    let log = empty_log();

    let transport = MockTransport::<u32>::new();
    transport.queue_append_entries_reply(
        "n2",
        Ok(AppendEntriesReply {
            term: 10, // higher term → leader must step down
            success: false,
            conflict_index: None,
            conflict_term: None,
        }),
    );
    let notify = Arc::new(Notify::new());

    replication::replicate_to_peer(
        &"n2".to_string(),
        state.clone(),
        log,
        transport,
        storage,
        notify,
    )
    .await
    .unwrap();

    let sg = state.lock().await;
    assert_eq!(sg.role, Role::Follower);
    assert_eq!(sg.persistent.current_term, 10);
}

#[tokio::test]
async fn backtracks_next_index_on_failure() {
    let (storage, _dir) = make_storage();
    let state = make_leader_state("n1", "n2", 1);
    // Set next_index to 5 manually.
    state
        .lock()
        .await
        .leader_state
        .as_mut()
        .unwrap()
        .peers
        .get_mut("n2")
        .unwrap()
        .next_index = 5;
    let log = empty_log();
    for _ in 0..4 {
        log.lock().await.append_command(1, 0u32);
    }

    let transport = MockTransport::<u32>::new();
    transport.queue_append_entries_reply(
        "n2",
        Ok(AppendEntriesReply {
            term: 1,
            success: false,
            conflict_index: None, // no hint → decrement by 1
            conflict_term: None,
        }),
    );
    let notify = Arc::new(Notify::new());

    replication::replicate_to_peer(
        &"n2".to_string(),
        state.clone(),
        log,
        transport,
        storage,
        notify,
    )
    .await
    .unwrap();

    let sg = state.lock().await;
    assert_eq!(
        sg.leader_state.as_ref().unwrap().peers["n2"].next_index,
        4 // decremented from 5
    );
}

#[tokio::test]
async fn uses_conflict_index_hint() {
    let (storage, _dir) = make_storage();
    let state = make_leader_state("n1", "n2", 1);
    state
        .lock()
        .await
        .leader_state
        .as_mut()
        .unwrap()
        .peers
        .get_mut("n2")
        .unwrap()
        .next_index = 5;
    let log = empty_log();
    for _ in 0..4 {
        log.lock().await.append_command(1, 0u32);
    }

    let transport = MockTransport::<u32>::new();
    transport.queue_append_entries_reply(
        "n2",
        Ok(AppendEntriesReply {
            term: 1,
            success: false,
            conflict_index: Some(2), // jump directly to 2
            conflict_term: Some(1),
        }),
    );
    let notify = Arc::new(Notify::new());

    replication::replicate_to_peer(
        &"n2".to_string(),
        state.clone(),
        log,
        transport,
        storage,
        notify,
    )
    .await
    .unwrap();

    assert_eq!(
        state.lock().await.leader_state.as_ref().unwrap().peers["n2"].next_index,
        2
    );
}

// endregion
