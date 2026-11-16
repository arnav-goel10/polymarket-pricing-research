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
//!   archives and a stale leader's writes are refused at the sink, which
//!   checks each write against the live grant.
//!
//! [`keycheck`] measures how well the key assumption holds on a real
//! recording.
//!
//! The stream a node serves is built from two more: [`ring`] numbers every
//! venue frame and keeps the recent ones for resume and resync, and
//! [`wire`] is the JSON protocol a client speaks to a node. [`client`] is
//! the hedged client that subscribes to several nodes and merges them.

pub mod client;
pub mod key;
pub mod keycheck;
pub mod lease;
pub mod merge;
pub mod ring;
pub mod wire;

pub use key::{EventKey, Feed, FrameKind, KeyKind};
pub use lease::{
    Clock, Epoch, FenceError, Fenced, Grant, Lease, LeaseError, ManualClock, MemoryLease,
    SystemClock,
};
pub use merge::{FeedCounters, MergeConfig, Merger, Verdict, WindowConfig};
