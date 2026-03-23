use crate::raft::{
    state::{LeaderState, RaftState, Role},
    storage::RaftStorage,
};

fn make_storage() -> (RaftStorage, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let storage = RaftStorage::new(dir.path().to_path_buf()).unwrap();
    (storage, dir) // keep dir alive so it isn't dropped (and deleted) early
}

// region initial_state

#[test]
fn new_node_starts_as_follower() {
    let state = RaftState::new("node1".into(), vec!["node2".into()]);
    assert_eq!(state.role, Role::Follower);
    assert_eq!(state.persistent.current_term, 0);
    assert!(state.persistent.voted_for.is_none());
    assert!(state.volatile.current_leader.is_none());
    assert!(state.leader_state.is_none());
}

// endregion

// region step_down_to_follower

#[test]
fn step_down_updates_term_and_clears_vote() {
    let (storage, _dir) = make_storage();
    let mut state = RaftState::new("node1".into(), vec![]);
    state.persistent.current_term = 3;
    state.persistent.voted_for = Some("candidate".into());
    state.role = Role::Candidate;

    state.step_down_to_follower(&storage, 7);

    assert_eq!(state.persistent.current_term, 7);
    assert!(state.persistent.voted_for.is_none());
    assert_eq!(state.role, Role::Follower);
}

#[test]
fn step_down_clears_leader_state() {
    let (storage, _dir) = make_storage();
    let mut state = RaftState::new("node1".into(), vec!["node2".into()]);
    state.role = Role::Leader;
    state.leader_state = Some(LeaderState::initialise(&["node2".into()], 0));

    state.step_down_to_follower(&storage, 5);

    assert!(state.leader_state.is_none());
}

#[test]
fn step_down_persists_to_storage() {
    let (storage, dir) = make_storage();
    let mut state = RaftState::new("node1".into(), vec![]);
    state.persistent.current_term = 2;

    state.step_down_to_follower(&storage, 9);

    // Re-open storage and verify the persisted term.
    let storage2 = RaftStorage::new(dir.path().to_path_buf()).unwrap();
    let ps = storage2.load_persistent_state().unwrap().unwrap();
    assert_eq!(ps.current_term, 9);
    assert!(ps.voted_for.is_none());
}

// endregion

// region is_leader

#[test]
fn is_leader_true_only_when_leader_role() {
    let mut state = RaftState::new("n1".into(), vec![]);
    assert!(!state.is_leader());
    state.role = Role::Candidate;
    assert!(!state.is_leader());
    state.role = Role::Leader;
    assert!(state.is_leader());
}

// endregion

// region initialize LeaderState

#[test]
fn leader_state_initialise_sets_next_index() {
    let peers = ["a".to_string(), "b".to_string(), "c".to_string()];
    let ls = LeaderState::initialise(&peers, 5);
    for peer in &peers {
        assert_eq!(ls.peers[peer].next_index, 6, "next_index for {peer}");
    }
}

#[test]
fn leader_state_initialise_sets_match_index_zero() {
    let peers = ["x".to_string(), "y".to_string()];
    let ls = LeaderState::initialise(&peers, 10);
    for peer in &peers {
        assert_eq!(ls.peers[peer].match_index, 0, "match_index for {peer}");
    }
}

#[test]
fn leader_state_initialise_with_empty_log() {
    let peers = ["p1".to_string()];
    let ls = LeaderState::initialise(&peers, 0);
    assert_eq!(ls.peers["p1"].next_index, 1);
}

// endregion
