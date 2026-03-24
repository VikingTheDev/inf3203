/// Handles reading and writing Raft persistent state to the local filesystem.
///
/// Raft paper: §5.2, paragraph 1 (persistent state); Figure 2, "Persistent state on all servers".
///
/// Each node writes to its own data directory (`/tmp/inf3203_raft_<node_id>/`).
/// This directory must be local, not on distributed fs (can't use distributed fs for communication).
use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json;

use super::log::LogEntry;
use super::state::PersistentState;

pub struct RaftStorage {
    data_dir: PathBuf,
}

impl RaftStorage {
    /// Open (or create) a storage directory for this node.
    pub fn new(data_dir: PathBuf) -> Result<Self> {
        fs::create_dir_all(&data_dir)?;
        Ok(Self { data_dir })
    }

    /// Persist `current_term` and `voted_for` to stable storage.
    ///
    /// **Must be called before responding to any RPC that mutates these fields.**
    /// Uses a write-then-rename pattern to avoid partial writes.
    pub fn save_persistent_state(&self, state: &PersistentState) -> Result<()> {
        let path = self.data_dir.join("persistent_state.json");
        let tmp = self.data_dir.join("persistent_state.json.tmp");

        serde_json::to_writer(fs::File::create(&tmp)?, state)?;
        fs::rename(tmp, path)?;

        Ok(())
    }

    /// Load `PersistentState` from disk, or return `None` on a fresh node.
    pub fn load_persistent_state(&self) -> Result<Option<PersistentState>> {
        let path = self.data_dir.join("persistent_state.json");

        if !path.exists() {
            return Ok(None);
        }

        let state: PersistentState = serde_json::from_reader(fs::File::open(path)?)?;
        Ok(Some(state))
    }

    /// Persist the full log to stable storage.
    ///
    /// Called after every append or truncation.
    pub fn save_log<C: Serialize>(&self, entries: &[LogEntry<C>]) -> Result<()> {
        let path = self.data_dir.join("persistent_log.json");
        let tmp = self.data_dir.join("persistent_log.json.tmp");

        serde_json::to_writer(fs::File::create(&tmp)?, entries)?;
        fs::rename(tmp, path)?;

        Ok(())
    }

    /// Load the log from disk, or return an empty vec on a fresh node.
    pub fn load_log<C: for<'de> Deserialize<'de>>(&self) -> Result<Vec<LogEntry<C>>> {
        let path = self.data_dir.join("persistent_log.json");

        if !path.exists() {
            return Ok(Vec::new());
        }

        let entries: Vec<LogEntry<C>> = serde_json::from_reader(fs::File::open(path)?)?;
        Ok(entries)
    }
}
