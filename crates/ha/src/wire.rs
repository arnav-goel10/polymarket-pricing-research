//! Wire protocol v1 between a node and a client: JSON text messages over a
//! WebSocket, each an object tagged by `t`.
//!
//! The client sends one `sub` after connecting. The node answers `hello`,
//! then, for a snapshot, the resync frames as `ev` with `snap:true` and a
//! `snap_end`, then live `ev` frames in sequence order, with an `hb` every
//! 250 ms. The node ends a connection with `bye`.
//!
//! ```text
//! client  {"t":"sub","v":1,"feeds":["rtds","binance","clob"],"resume":{"boot":"..","seq":N}}
//! node    {"t":"hello","v":1,"node":1,"boot":"..","mode":"resume","next":N,"oldest":O,"head":H,"complete":true}
//!         {"t":"ev","seq":N,"src":3,"rx":1700000000000000,"snap":false,"frame":"<raw venue text>"}
//!         {"t":"snap_end","next":N}
//!         {"t":"hb","head":H,"epoch":0,"leader":false}
//!         {"t":"bye","reason":"lagged"}
//! ```
//!
//! Keys are not on the wire. A client derives them from `src` and `frame`
//! with [`crate::key::frame_keys`], the same code the node runs.
//!
//! `boot` is a random 64-bit number per node process, sent as 16 lower case
//! hex digits so no JSON reader rounds it. `src` is the recording source
//! number (1 RTDS, 2 Binance, 3 CLOB book). `rx` is the node's receive time
//! in microseconds since the Unix epoch.
//!
//! In v1 a subscription carries every feed: `feeds` is empty (meaning all)
//! or names all three. Any other list is refused with `bye bad_request`,
//! because a filtered stream would have holes in its sequence and the
//! client could no longer tell a filtered frame from a lost one.

use feeds::recording::Source;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub const VERSION: u32 = 1;

/// Feed names accepted in `sub.feeds`.
pub const FEED_NAMES: [&str; 3] = ["rtds", "binance", "clob"];

/// A node process's boot id. Sequence numbers restart at 1 on every boot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Boot(pub u64);

impl std::fmt::Display for Boot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl std::str::FromStr for Boot {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        if s.len() != 16 {
            return Err(format!("boot must be 16 hex digits, got {s:?}"));
        }
        u64::from_str_radix(s, 16)
            .map(Boot)
            .map_err(|e| format!("bad boot {s:?}: {e}"))
    }
}

impl Serialize for Boot {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Boot {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// Where a client left off: the last sequence number it received in order
/// from that boot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumeFrom {
    pub boot: Boot,
    pub seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum ClientMsg {
    Sub {
        v: u32,
        #[serde(default)]
        feeds: Vec<String>,
        #[serde(default)]
        resume: Option<ResumeFrom>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// The node continues from `next = resume.seq + 1`.
    Resume,
    /// The node sends resync frames, then continues from `next`.
    Snapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ByeReason {
    Shutdown,
    Lagged,
    BadRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum NodeMsg {
    Hello {
        v: u32,
        node: u32,
        boot: Boot,
        mode: Mode,
        /// The first live sequence number this connection will receive.
        next: u64,
        /// The oldest sequence number the node still holds.
        oldest: u64,
        /// The newest sequence number assigned so far, 0 before the first.
        head: u64,
        /// False when a snapshot could not include every order-book change
        /// since the oldest current book, because the ring no longer held it.
        complete: bool,
    },
    Ev {
        seq: u64,
        src: u8,
        rx: i64,
        snap: bool,
        frame: String,
    },
    SnapEnd {
        next: u64,
    },
    Hb {
        head: u64,
        epoch: u64,
        leader: bool,
    },
    Bye {
        reason: ByeReason,
    },
}

#[derive(Serialize)]
struct EvOut<'a> {
    t: &'static str,
    seq: u64,
    src: u8,
    rx: i64,
    snap: bool,
    frame: &'a str,
}

/// Encode an `ev` without copying the frame into an owned message first.
pub fn encode_ev(seq: u64, src: Source, rx: i64, snap: bool, frame: &str) -> String {
    serde_json::to_string(&EvOut {
        t: "ev",
        seq,
        src: src as u8,
        rx,
        snap,
        frame,
    })
    .expect("ev serializes")
}

pub fn encode<T: Serialize>(msg: &T) -> String {
    serde_json::to_string(msg).expect("wire message serializes")
}

pub fn decode_node(text: &str) -> Result<NodeMsg, serde_json::Error> {
    serde_json::from_str(text)
}

/// A subscription the node accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sub {
    pub resume: Option<ResumeFrom>,
}

/// Parse and check a client's first message. `Err` means `bye bad_request`.
pub fn decode_sub(text: &str) -> Result<Sub, String> {
    let ClientMsg::Sub { v, feeds, resume } =
        serde_json::from_str::<ClientMsg>(text).map_err(|e| e.to_string())?;
    if v != VERSION {
        return Err(format!("unsupported version {v}"));
    }
    let all = feeds.is_empty()
        || (feeds.len() == FEED_NAMES.len()
            && FEED_NAMES.iter().all(|n| feeds.iter().any(|f| f == n)));
    if !all {
        return Err(format!("v1 serves all feeds, got {feeds:?}"));
    }
    Ok(Sub { resume })
}

/// The `sub` a client sends.
pub fn encode_sub(resume: Option<ResumeFrom>) -> String {
    encode(&ClientMsg::Sub {
        v: VERSION,
        feeds: FEED_NAMES.iter().map(|s| s.to_string()).collect(),
        resume,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ev_encodes_to_the_same_message_it_decodes_to() {
        let frame = r#"{"topic":"crypto_prices","payload":{"value":1.5,"note":"a \"q\" \\ b"}}"#;
        let s = encode_ev(42, Source::Rtds, 1_700_000_000_000_123, false, frame);
        assert_eq!(
            decode_node(&s).unwrap(),
            NodeMsg::Ev {
                seq: 42,
                src: 1,
                rx: 1_700_000_000_000_123,
                snap: false,
                frame: frame.to_string()
            }
        );
        assert!(s.starts_with(r#"{"t":"ev","seq":42,"src":1,"#), "{s}");
    }

    #[test]
    fn every_node_message_round_trips() {
        let msgs = [
            NodeMsg::Hello {
                v: 1,
                node: 2,
                boot: Boot(u64::MAX),
                mode: Mode::Snapshot,
                next: 10,
                oldest: 3,
                head: 9,
                complete: false,
            },
            NodeMsg::SnapEnd { next: 10 },
            NodeMsg::Hb {
                head: 9,
                epoch: 7,
                leader: true,
            },
            NodeMsg::Bye {
                reason: ByeReason::BadRequest,
            },
        ];
        for m in msgs {
            assert_eq!(decode_node(&encode(&m)).unwrap(), m);
        }
        let hello = encode(&NodeMsg::Hello {
            v: 1,
            node: 1,
            boot: Boot(255),
            mode: Mode::Resume,
            next: 1,
            oldest: 1,
            head: 0,
            complete: true,
        });
        assert!(hello.contains(r#""t":"hello""#), "{hello}");
        assert!(hello.contains(r#""boot":"00000000000000ff""#), "{hello}");
        assert!(hello.contains(r#""mode":"resume""#), "{hello}");
        let bye = encode(&NodeMsg::Bye {
            reason: ByeReason::Lagged,
        });
        assert_eq!(bye, r#"{"t":"bye","reason":"lagged"}"#);
        let snap_end = encode(&NodeMsg::SnapEnd { next: 5 });
        assert_eq!(snap_end, r#"{"t":"snap_end","next":5}"#);
    }

    #[test]
    fn sub_accepts_all_feeds_and_resume() {
        let resume = ResumeFrom {
            boot: Boot(0xdead_beef),
            seq: 99,
        };
        let s = encode_sub(Some(resume));
        assert_eq!(
            s,
            r#"{"t":"sub","v":1,"feeds":["rtds","binance","clob"],"resume":{"boot":"00000000deadbeef","seq":99}}"#
        );
        assert_eq!(decode_sub(&s).unwrap().resume, Some(resume));
        assert_eq!(decode_sub(&encode_sub(None)).unwrap().resume, None);
        let bare = r#"{"t":"sub","v":1,"feeds":[],"resume":null}"#;
        assert_eq!(decode_sub(bare).unwrap().resume, None);
        let reordered = r#"{"t":"sub","v":1,"feeds":["clob","rtds","binance"]}"#;
        assert!(decode_sub(reordered).is_ok());
    }

    #[test]
    fn sub_refuses_what_v1_cannot_serve() {
        for bad in [
            r#"{"t":"sub","v":2,"feeds":[]}"#,
            r#"{"t":"sub","v":1,"feeds":["rtds"]}"#,
            r#"{"t":"sub","v":1,"feeds":["rtds","binance","binance"]}"#,
            r#"{"t":"sub","v":1,"feeds":["rtds","binance","clob","x"]}"#,
            r#"{"t":"sub","v":1,"resume":{"boot":"12","seq":1}}"#,
            r#"{"t":"sub","v":1,"resume":{"boot":"zzzzzzzzzzzzzzzz","seq":1}}"#,
            r#"{"t":"hello"}"#,
            "not json",
        ] {
            assert!(decode_sub(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn boot_text_round_trips_at_the_edges() {
        for b in [0, 1, 0x0123_4567_89ab_cdef, u64::MAX] {
            let s = Boot(b).to_string();
            assert_eq!(s.len(), 16);
            assert_eq!(s.parse::<Boot>().unwrap(), Boot(b));
        }
    }
}
