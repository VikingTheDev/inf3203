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
        // Return or create and return directory for persistent storage
        fs::create_dir_all(&data_dir).expect(
            format!(
                "Failed to create data directory at {}",
                data_dir.to_string_lossy()
            )
            .as_str(),
        );
        Ok(Self { data_dir })
    }

    /// Persist `current_term` and `voted_for` to stable storage.
    ///
    /// **Must be called before responding to any RPC that mutates these fields.**
    /// Uses a write-then-rename pattern to avoid partial writes.
    pub fn save_persistent_state(&self, state: &PersistentState) -> Result<()> {
        // Construct the path to the state file and a temporary file for atomic writes
        let path = self.data_dir.join("persistent_state.json");
        let tmp = self.data_dir.join("persistent_state.json.tmp");

        // Write state to tmp file
        serde_json::to_writer(fs::File::create(&tmp)?, state)
            .expect("Failed to create or write to temp state file");

        // Rename to overwrite old persistent state with new state
        fs::rename(tmp, path)
            .expect("Failed to rename temp state file (overwrite old state with new");

        Ok(())
    }

    /// Load `PersistentState` from disk, or return `None` on a fresh node.
    pub fn load_persistent_state(&self) -> Result<Option<PersistentState>> {
        // Construct path where state should be found
        let path = self.data_dir.join("persistent_state.json");

        // Check if state file exists, if not return None
        if !path.exists() {
            return Ok(None);
        }

        // File exists, load and deserialize
        let state: PersistentState = serde_json::from_reader(fs::File::open(path)?)
            .expect("Failed to open or deserialize persistent state file");

        Ok(Some(state))
    }

    /// Persist the full log to stable storage.
    ///
    /// Called after every append or truncation.
    pub fn save_log<C: Serialize>(&self, entries: &[LogEntry<C>]) -> Result<()> {
        // Construct the path to the log file and a temporary file for atomic writes
        let path = self.data_dir.join("persistent_log.json");
        let tmp = self.data_dir.join("persistent_log.json.tmp");

        // Write logs to tmp file
        serde_json::to_writer(fs::File::create(&tmp)?, entries)
            .expect("Failed to create or write to temp log file");

        // Rename to overwrite old persistent log with new logs
        fs::rename(tmp, path).expect("Failed to rename temp log file (overwrite old log with new");

        Ok(())
    }

    /// Load the log from disk, or return an empty vec on a fresh node.
    pub fn load_log<C: for<'de> Deserialize<'de>>(&self) -> Result<Vec<LogEntry<C>>> {
        // Construct path where log should be found
        let path = self.data_dir.join("persistent_log.json");

        // Check if log file exists, if not return None
        if !path.exists() {
            return Ok(Vec::new());
        }

        // File exists, load and deserialize
        let entries: Vec<LogEntry<C>> = serde_json::from_reader(fs::File::open(path)?)
            .expect("Failed to open or deserialize persistent log file");

        Ok(entries)
    }
}
