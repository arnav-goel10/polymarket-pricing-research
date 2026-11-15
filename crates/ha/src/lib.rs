//! High availability building blocks for running several identical ingest
//! nodes side by side.
//!
//! Every node reads the same public venues, so the venue is the source of
//! truth and no replicated log is needed. Three pieces make that work:
//!
//! - [`key`]: an identity for each venue event built only from fields the
//!   venue sent, so two nodes that saw the same event produce the same key.
//! - [`merge`]: a client side merger that takes the streams of N nodes and
//!   delivers each key once, inside a bounded window.
//! - [`lease`]: a leader lease with a fencing epoch, so exactly one node
//!   archives and a stale leader's writes are refused.
//!
//! [`keycheck`] measures how well the key assumption holds on a real
//! recording.

pub mod key;
pub mod keycheck;
pub mod lease;
pub mod merge;

pub use key::{EventKey, Feed, FrameKind, KeyKind, Stamped};
pub use lease::{Clock, Epoch, Fenced, Grant, Lease, LeaseError, ManualClock, MemoryLease};
pub use merge::{MergeConfig, Merger, Verdict, WindowConfig};
