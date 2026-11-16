//! Hedged client: subscribe to several nodes at once and merge their
//! streams by event key, so one node dying loses nothing and nothing comes
//! out twice.
//!
//! [`Core`] is the whole protocol with no IO in it. It is fed the text of
//! each message, connection ends and the time, and returns [`Output`]s:
//! every `ev` with the merger's verdict for each of its keys, and each
//! connection going live or down. [`run`] is a thin tokio-tungstenite
//! driver around it that dials, redials with jittered backoff and forwards
//! messages. Every frame from every node goes through one shared
//! [`Merger`].
//!
//! Per connection:
//!
//! - A live `ev` whose `seq` is not the one expected counts a `seq_gap`, is
//!   not offered, and ends the connection. The redial resumes at the last
//!   contiguous `seq`.
//! - A `hello` in resume mode whose `next` is not the resume point plus one
//!   counts a `resume_violation`. The client then follows the node's `next`.
//! - No message for [`DEAD_AFTER`] means the connection is dead. Dialing
//!   starts that clock too, so a connect that hangs is dead as well.
//! - Redial waits 100 ms to 2 s, doubling per failed attempt, with jitter,
//!   and starts again at 100 ms once a connection goes live.
//! - A second gap at the same `seq` of the same boot means the resume
//!   cannot get past it, so the next dial asks for a snapshot instead.
//!
//! The merger's unit is a key and one frame can carry several (an RTDS
//! history snapshot, a CLOB array, a two-entry price change), so one frame
//! can come back with mixed verdicts. A consumer that applies frames uses
//! [`Event::delivered_frame`], the frame cut down to its delivered events;
//! applying the whole frame would apply the duplicate parts twice.
//!
//! Not guaranteed, and counted instead: order across nodes
//! (`order_inversions`, a delivered timed key older than the newest
//! delivered key of its feed when that one came from another node).
//!
//! [`BookView`] is a helper for order-book consumers: a merged stream can
//! deliver a price change from one node after a newer `book` from another,
//! and the view drops such a stale delta.

use crate::key::{frame_keys, EventKey, Feed};
use crate::merge::{MergeConfig, Merger, Verdict};
use crate::wire::{self, Boot, ByeReason, Mode, NodeMsg, ResumeFrom};
use feeds::recording::Source;
use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::time::{Duration, Instant};

/// A connection with no message for this long is dead.
pub const DEAD_AFTER: Duration = Duration::from_secs(1);
pub const BACKOFF_MIN: Duration = Duration::from_millis(100);
pub const BACKOFF_MAX: Duration = Duration::from_secs(2);
/// Keys remembered per connection for counting a node repeating itself.
const RECENT_KEYS: usize = 8_192;

/// Seeded client faults for the HA harness. Only a build with the
/// `mutants` feature can select one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(any(test, feature = "mutants")), allow(dead_code))]
pub enum ClientMutant {
    None,
    /// Deliver every copy: a duplicate verdict is passed on as a delivery.
    NoDedup,
    /// Dial node 0 only.
    SingleNode,
    /// Dial each node once and never again.
    NoReconnect,
    /// Deliver late copies: a late verdict is passed on as a delivery.
    LateDeliver,
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub nodes: usize,
    pub merge: MergeConfig,
    pub dead_after: Duration,
    mutant: ClientMutant,
}

impl ClientConfig {
    pub fn new(nodes: usize) -> Self {
        ClientConfig {
            nodes,
            merge: MergeConfig::default(),
            dead_after: DEAD_AFTER,
            mutant: ClientMutant::None,
        }
    }

    #[cfg(any(test, feature = "mutants"))]
    pub fn with_mutant(mut self, m: ClientMutant) -> Self {
        self.mutant = m;
        self
    }

    pub fn mutant(&self) -> ClientMutant {
        self.mutant
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnState {
    Down,
    /// Dialed, waiting for `hello`.
    Handshake,
    /// Reading snapshot frames until `snap_end`.
    Snapshot,
    Live,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownReason {
    /// The socket closed or the connect failed.
    Closed,
    /// No message for the dead-after time.
    Dead,
    SeqGap,
    Bye(ByeReason),
    /// A message that does not parse or does not fit the protocol state.
    Protocol,
}

/// Client counters. The HA checks gate on the first three.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ClientCounters {
    pub seq_gaps: u64,
    pub resume_violations: u64,
    /// Offered events with no venue identity.
    pub unkeyed: u64,
    /// Hellos from a different boot than the last one from that node.
    pub resets: u64,
    /// Dials after the first, per node, summed.
    pub reconnects: u64,
    pub incomplete_snapshots: u64,
    pub order_inversions: u64,
    /// Keys a node sent again on the same connection.
    pub self_repeats: u64,
    pub disconnects: u64,
    /// `ev` messages offered to the merger.
    pub frames: u64,
    /// Snapshots from a boot the client already followed (after `bye
    /// lagged`, or a dial with no resume point). That connection skips the
    /// node's frames between; the other node covers them.
    pub resyncs: u64,
    /// Frames with some events delivered and some not, which a frame
    /// consumer must cut (see [`Event::delivered_frame`]).
    pub mixed_frames: u64,
}

/// One `ev` and what the merger did with each of its keys.
#[derive(Debug, Clone)]
pub struct Event {
    pub node: usize,
    pub seq: u64,
    pub src: Source,
    pub rx_micros: i64,
    pub snap: bool,
    pub frame: String,
    pub keys: Vec<(EventKey, Verdict)>,
}

impl Event {
    /// The frame cut down to the events the merger delivered, in the
    /// venue's own shape: `None` when none was, the frame as received when
    /// all were. Otherwise RTDS history points and CLOB array items and
    /// price change entries that were not delivered are removed, and a
    /// price change left with no entry goes too.
    ///
    /// Each piece kept is keyed again on its own and must be the key at its
    /// position in `keys`, so the cut never hands on a duplicate or late
    /// piece. If the pieces do not line up with `keys` (which the key rules
    /// never produce) the result is `None`: the frame is lost, not doubled.
    pub fn delivered_frame(&self) -> Option<String> {
        let n = self
            .keys
            .iter()
            .filter(|(_, v)| *v == Verdict::Deliver)
            .count();
        if n == 0 {
            return None;
        }
        if n == self.keys.len() {
            return Some(self.frame.clone());
        }
        cut_frame(self.src, &self.frame, &self.keys)
    }
}

/// Walks the pieces of a frame in the order `frame_keys` emits their keys.
struct Cutter<'a> {
    src: Source,
    keys: std::slice::Iter<'a, (EventKey, Verdict)>,
    buf: Vec<EventKey>,
    ok: bool,
}

impl Cutter<'_> {
    /// Whether `piece`, one event as a frame of its own, is delivered.
    fn keep(&mut self, piece: &serde_json::Value) -> bool {
        self.buf.clear();
        frame_keys(self.src, &piece.to_string(), &mut self.buf);
        if self.buf.len() != 1 {
            self.ok = false;
            return false;
        }
        match self.keys.next() {
            Some((k, v)) if *k == self.buf[0] => *v == Verdict::Deliver,
            _ => {
                self.ok = false;
                false
            }
        }
    }

    /// One CLOB event. `None` drops it.
    fn clob_item(&mut self, mut e: serde_json::Value) -> Option<serde_json::Value> {
        use serde_json::Value;
        let split = e.get("event_type").and_then(Value::as_str) == Some("price_change")
            && matches!(e.get("price_changes"), Some(Value::Array(_)));
        if !split {
            return self.keep(&e).then_some(e);
        }
        let changes = match e.get_mut("price_changes").map(Value::take) {
            Some(Value::Array(c)) => c,
            _ => Vec::new(),
        };
        let mut kept = Vec::with_capacity(changes.len());
        for c in changes {
            e["price_changes"] = Value::Array(vec![c]);
            if self.keep(&e) {
                if let Some(Value::Array(mut one)) = e.get_mut("price_changes").map(Value::take) {
                    kept.append(&mut one);
                }
            }
        }
        if kept.is_empty() {
            return None;
        }
        e["price_changes"] = Value::Array(kept);
        Some(e)
    }
}

fn cut_frame(src: Source, frame: &str, keys: &[(EventKey, Verdict)]) -> Option<String> {
    use serde_json::Value;
    let mut root: Value = serde_json::from_str(frame).ok()?;
    let mut cut = Cutter {
        src,
        keys: keys.iter(),
        buf: Vec::with_capacity(4),
        ok: true,
    };
    match Feed::from_source(src)? {
        Feed::Rtds => {
            let points = match root.pointer_mut("/payload/data") {
                Some(Value::Array(p)) => std::mem::take(p),
                _ => return None,
            };
            let mut kept = Vec::with_capacity(points.len());
            for p in points {
                root["payload"]["data"] = Value::Array(vec![p]);
                if cut.keep(&root) {
                    if let Some(Value::Array(mut one)) =
                        root.pointer_mut("/payload/data").map(Value::take)
                    {
                        kept.append(&mut one);
                    }
                }
            }
            root["payload"]["data"] = Value::Array(kept);
        }
        Feed::Clob => {
            root = match root {
                Value::Array(items) => {
                    Value::Array(items.into_iter().filter_map(|e| cut.clob_item(e)).collect())
                }
                e => cut.clob_item(e)?,
            };
        }
        // A Binance frame is one event.
        Feed::Binance => return None,
    }
    (cut.ok && cut.keys.next().is_none()).then(|| root.to_string())
}

#[derive(Debug, Clone)]
pub enum Output {
    Event(Event),
    /// The connection is live: resumed, or its snapshot is complete.
    Live {
        node: usize,
    },
    /// The connection is gone. `last_msg` is when the last `ev` or `hb`
    /// arrived from that node, the fault instant as the client saw it.
    Down {
        node: usize,
        reason: DownReason,
        last_msg: Option<Instant>,
    },
}

#[derive(Debug)]
struct Conn {
    state: ConnState,
    /// Boot of the latest hello from this node.
    boot: Option<Boot>,
    /// Last sequence number received in order from `boot`.
    last_seq: Option<u64>,
    expect: u64,
    resume_sent: Option<ResumeFrom>,
    /// Last message of any kind, or the dial.
    heard_at: Instant,
    /// Last `ev` or `hb`.
    last_msg: Option<Instant>,
    dials: u64,
    /// Where the last seq gap was, as (boot, last contiguous seq).
    gap_at: Option<(Boot, u64)>,
    /// The next dial asks for a snapshot, not a resume.
    no_resume: bool,
    recent: HashSet<EventKey>,
    recent_order: VecDeque<EventKey>,
}

impl Conn {
    fn new(now: Instant) -> Self {
        Conn {
            state: ConnState::Down,
            boot: None,
            last_seq: None,
            expect: 0,
            resume_sent: None,
            heard_at: now,
            last_msg: None,
            dials: 0,
            gap_at: None,
            no_resume: false,
            recent: HashSet::new(),
            recent_order: VecDeque::new(),
        }
    }

    fn clear_recent(&mut self) {
        self.recent.clear();
        self.recent_order.clear();
    }

    /// Remember `k`; true if this connection already sent it.
    fn repeat(&mut self, k: EventKey) -> bool {
        if self.recent.contains(&k) {
            return true;
        }
        if self.recent_order.len() >= RECENT_KEYS {
            if let Some(old) = self.recent_order.pop_front() {
                self.recent.remove(&old);
            }
        }
        self.recent.insert(k);
        self.recent_order.push_back(k);
        false
    }
}

/// The client protocol with no IO.
#[derive(Debug)]
pub struct Core {
    cfg: ClientConfig,
    conns: Vec<Conn>,
    merger: Merger,
    counters: ClientCounters,
    /// Newest delivered timestamp per feed and the node that delivered it.
    newest: [Option<(i64, usize)>; 3],
    keys: Vec<EventKey>,
}

impl Core {
    pub fn new(cfg: ClientConfig) -> Self {
        let now = Instant::now();
        Core {
            conns: (0..cfg.nodes).map(|_| Conn::new(now)).collect(),
            merger: Merger::new(cfg.nodes, cfg.merge),
            counters: ClientCounters::default(),
            newest: [None; 3],
            keys: Vec::with_capacity(16),
            cfg,
        }
    }

    pub fn counters(&self) -> ClientCounters {
        self.counters
    }

    pub fn merger(&self) -> &Merger {
        &self.merger
    }

    pub fn state(&self, node: usize) -> ConnState {
        self.conns[node].state
    }

    pub fn nodes(&self) -> usize {
        self.conns.len()
    }

    /// Whether the driver should dial `node` now (backoff aside).
    pub fn should_dial(&self, node: usize) -> bool {
        let c = &self.conns[node];
        if c.state != ConnState::Down {
            return false;
        }
        match self.cfg.mutant {
            ClientMutant::SingleNode => node == 0,
            ClientMutant::NoReconnect => c.dials == 0,
            _ => true,
        }
    }

    /// Start a connection to `node`. Returns the `sub` text to send.
    pub fn dial(&mut self, node: usize, now: Instant) -> String {
        let c = &mut self.conns[node];
        c.dials += 1;
        if c.dials > 1 {
            self.counters.reconnects += 1;
        }
        c.state = ConnState::Handshake;
        c.heard_at = now;
        let resume = match (c.boot, c.last_seq, std::mem::take(&mut c.no_resume)) {
            (Some(boot), Some(seq), false) => Some(ResumeFrom { boot, seq }),
            _ => None,
        };
        c.resume_sent = resume;
        wire::encode_sub(resume)
    }

    /// The socket for `node` closed or its connect failed.
    pub fn on_closed(&mut self, node: usize, _now: Instant, out: &mut Vec<Output>) {
        if self.conns[node].state != ConnState::Down {
            self.down(node, DownReason::Closed, out);
        }
    }

    /// Mark every silent connection dead.
    pub fn tick(&mut self, now: Instant, out: &mut Vec<Output>) {
        for node in 0..self.conns.len() {
            let c = &self.conns[node];
            if c.state != ConnState::Down
                && now.saturating_duration_since(c.heard_at) >= self.cfg.dead_after
            {
                self.down(node, DownReason::Dead, out);
            }
        }
    }

    fn down(&mut self, node: usize, reason: DownReason, out: &mut Vec<Output>) {
        let c = &mut self.conns[node];
        c.state = ConnState::Down;
        self.counters.disconnects += 1;
        out.push(Output::Down {
            node,
            reason,
            last_msg: c.last_msg,
        });
    }

    /// One text message from `node`.
    pub fn on_text(&mut self, node: usize, now: Instant, text: &str, out: &mut Vec<Output>) {
        if self.conns[node].state == ConnState::Down {
            return;
        }
        let Ok(msg) = wire::decode_node(text) else {
            return self.down(node, DownReason::Protocol, out);
        };
        let c = &mut self.conns[node];
        c.heard_at = now;
        match (c.state, msg) {
            (
                ConnState::Handshake,
                NodeMsg::Hello {
                    boot,
                    mode,
                    next,
                    complete,
                    ..
                },
            ) => {
                let same_boot = c.boot == Some(boot);
                if c.boot.is_some_and(|b| b != boot) {
                    self.counters.resets += 1;
                }
                c.boot = Some(boot);
                c.clear_recent();
                match mode {
                    Mode::Resume => {
                        let honest = c
                            .resume_sent
                            .is_some_and(|r| r.boot == boot && next == r.seq + 1);
                        if !honest {
                            self.counters.resume_violations += 1;
                        }
                        c.expect = next;
                        c.last_seq = Some(next.saturating_sub(1));
                        c.state = ConnState::Live;
                        out.push(Output::Live { node });
                    }
                    Mode::Snapshot => {
                        if same_boot {
                            self.counters.resyncs += 1;
                        }
                        if !complete {
                            self.counters.incomplete_snapshots += 1;
                        }
                        c.last_seq = None;
                        c.expect = next;
                        c.state = ConnState::Snapshot;
                    }
                }
            }
            (
                ConnState::Snapshot,
                NodeMsg::Ev {
                    seq,
                    src,
                    rx,
                    snap: true,
                    frame,
                },
            ) => {
                c.last_msg = Some(now);
                self.offer(node, seq, src, rx, true, frame, out);
            }
            (ConnState::Snapshot, NodeMsg::SnapEnd { next }) => {
                c.expect = next;
                c.last_seq = Some(next.saturating_sub(1));
                c.state = ConnState::Live;
                out.push(Output::Live { node });
            }
            (
                ConnState::Live,
                NodeMsg::Ev {
                    seq,
                    src,
                    rx,
                    snap: false,
                    frame,
                },
            ) => {
                c.last_msg = Some(now);
                if seq != c.expect {
                    self.counters.seq_gaps += 1;
                    // A resume that ends in the same gap again would loop
                    // forever; the next dial takes a snapshot instead.
                    let at = c.boot.zip(c.last_seq);
                    if at.is_some() && c.gap_at == at {
                        c.no_resume = true;
                    }
                    c.gap_at = at;
                    return self.down(node, DownReason::SeqGap, out);
                }
                c.expect = seq + 1;
                c.last_seq = Some(seq);
                self.offer(node, seq, src, rx, false, frame, out);
            }
            (ConnState::Snapshot | ConnState::Live, NodeMsg::Hb { .. }) => {
                c.last_msg = Some(now);
            }
            (_, NodeMsg::Bye { reason }) => self.down(node, DownReason::Bye(reason), out),
            _ => self.down(node, DownReason::Protocol, out),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn offer(
        &mut self,
        node: usize,
        seq: u64,
        src: u8,
        rx: i64,
        snap: bool,
        frame: String,
        out: &mut Vec<Output>,
    ) {
        let Some(source) = Source::from_u8(src).filter(|s| Feed::from_source(*s).is_some()) else {
            return self.down(node, DownReason::Protocol, out);
        };
        self.counters.frames += 1;
        self.keys.clear();
        frame_keys(source, &frame, &mut self.keys);
        let mut keys = Vec::with_capacity(self.keys.len());
        let mut delivered = 0;
        for &k in &self.keys {
            if !k.kind.is_keyed() {
                self.counters.unkeyed += 1;
            }
            if self.conns[node].repeat(k) {
                self.counters.self_repeats += 1;
            }
            let mut v = self.merger.offer(node, k);
            match (v, self.cfg.mutant) {
                (Verdict::Duplicate, ClientMutant::NoDedup)
                | (Verdict::Late, ClientMutant::LateDeliver) => v = Verdict::Deliver,
                _ => {}
            }
            delivered += (v == Verdict::Deliver) as usize;
            if let (Verdict::Deliver, Some(t)) = (v, k.ts_millis) {
                let f = k.kind.feed().index();
                match self.newest[f] {
                    Some((n, by)) if t < n => {
                        if by != node {
                            self.counters.order_inversions += 1;
                        }
                    }
                    _ => self.newest[f] = Some((t, node)),
                }
            }
            keys.push((k, v));
        }
        if delivered > 0 && delivered < keys.len() {
            self.counters.mixed_frames += 1;
        }
        out.push(Output::Event(Event {
            node,
            seq,
            src: source,
            rx_micros: rx,
            snap,
            frame,
            keys,
        }));
    }
}

/// Redial delay after `attempt` failed tries: 100 ms doubling to 2 s, then
/// scaled by `jitter` in [0.5, 1], never under 100 ms.
pub fn backoff(attempt: u32, jitter: f64) -> Duration {
    let base = BACKOFF_MIN
        .saturating_mul(1u32 << attempt.min(16))
        .min(BACKOFF_MAX);
    base.mul_f64(jitter.clamp(0.5, 1.0)).max(BACKOFF_MIN)
}

enum InMsg {
    Text(String),
    Closed,
}

struct In {
    node: usize,
    gen: u64,
    msg: InMsg,
}

async fn conn_task(
    node: usize,
    gen: u64,
    url: String,
    sub: String,
    tx: tokio::sync::mpsc::Sender<In>,
) {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::tungstenite::Message;
    let closed = In {
        node,
        gen,
        msg: InMsg::Closed,
    };
    let Ok((mut ws, _)) = tokio_tungstenite::connect_async(url.as_str()).await else {
        let _ = tx.send(closed).await;
        return;
    };
    if ws.send(Message::Text(sub)).await.is_ok() {
        while let Some(m) = ws.next().await {
            match m {
                Ok(Message::Text(t)) => {
                    let msg = In {
                        node,
                        gen,
                        msg: InMsg::Text(t),
                    };
                    if tx.send(msg).await.is_err() {
                        return;
                    }
                }
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(_) => {}
            }
        }
    }
    let _ = tx.send(closed).await;
}

/// A small per-process random source for backoff jitter.
fn jitter_seed() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u32(std::process::id());
    h.finish() | 1
}

fn next_jitter(state: &mut u64) -> f64 {
    // xorshift64
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    0.5 + 0.5 * ((x >> 11) as f64 / (1u64 << 53) as f64)
}

/// Run the client against `urls` (one per node) until `stop` resolves.
/// Every output goes to `on_output` in order. Returns the core, with its
/// counters and merger, at the end.
pub async fn run<S, F>(urls: Vec<String>, cfg: ClientConfig, stop: S, mut on_output: F) -> Core
where
    S: Future<Output = ()>,
    F: FnMut(Output),
{
    assert_eq!(urls.len(), cfg.nodes, "one url per node");
    let n = urls.len();
    let mut core = Core::new(cfg);
    let (tx, mut rx) = tokio::sync::mpsc::channel::<In>(4_096);
    let mut gens = vec![0u64; n];
    let mut tasks: Vec<Option<tokio::task::JoinHandle<()>>> = (0..n).map(|_| None).collect();
    let mut attempt = vec![0u32; n];
    let mut dial_at = vec![Instant::now(); n];
    let mut rng = jitter_seed();
    let mut out = Vec::new();
    let mut tick = tokio::time::interval(Duration::from_millis(20));
    tokio::pin!(stop);
    loop {
        let now = Instant::now();
        for i in 0..n {
            if core.should_dial(i) && now >= dial_at[i] {
                gens[i] += 1;
                let sub = core.dial(i, now);
                tasks[i] = Some(tokio::spawn(conn_task(
                    i,
                    gens[i],
                    urls[i].clone(),
                    sub,
                    tx.clone(),
                )));
            }
        }
        tokio::select! {
            _ = &mut stop => break,
            m = rx.recv() => {
                let Some(m) = m else { break };
                if m.gen == gens[m.node] {
                    let now = Instant::now();
                    match m.msg {
                        InMsg::Text(t) => core.on_text(m.node, now, &t, &mut out),
                        InMsg::Closed => core.on_closed(m.node, now, &mut out),
                    }
                }
            }
            _ = tick.tick() => {}
        }
        // Checked on every turn, so a flood from one node cannot hide the
        // silence of another.
        core.tick(Instant::now(), &mut out);
        for o in out.drain(..) {
            match &o {
                Output::Down { node, .. } => {
                    if let Some(t) = tasks[*node].take() {
                        t.abort();
                    }
                    // Anything still queued from that socket is stale.
                    gens[*node] += 1;
                    let d = backoff(attempt[*node], next_jitter(&mut rng));
                    attempt[*node] = attempt[*node].saturating_add(1);
                    dial_at[*node] = Instant::now() + d;
                }
                Output::Live { node } => attempt[*node] = 0,
                Output::Event(_) => {}
            }
            on_output(o);
        }
    }
    for t in tasks.into_iter().flatten() {
        t.abort();
    }
    core
}

/// Order-book view over a merged stream: drops a CLOB price change whose
/// venue timestamp is below the latest delivered `book` for its asset. Feed
/// it CLOB events with [`BookView::admit_event`], or frames already cut to
/// their delivered events with [`BookView::admit`].
#[derive(Debug, Default)]
pub struct BookView {
    latest_book: HashMap<String, i64>,
    /// Price change entries dropped as older than the asset's book.
    pub dropped: u64,
}

impl BookView {
    pub fn new() -> Self {
        Self::default()
    }

    /// The timestamp of the latest delivered book for `asset`.
    pub fn book_ts(&self, asset: &str) -> Option<i64> {
        self.latest_book.get(asset).copied()
    }

    /// Take one CLOB event from the client: only its delivered part is
    /// applied (see [`Event::delivered_frame`]).
    pub fn admit_event(&mut self, e: &Event) -> Vec<serde_json::Value> {
        match e.delivered_frame() {
            Some(f) => self.admit(&f),
            None => Vec::new(),
        }
    }

    /// Take one delivered CLOB frame (one event or an array). Returns the
    /// events to apply, in order, with stale price change entries removed
    /// and a price change left with no entry dropped.
    pub fn admit(&mut self, frame: &str) -> Vec<serde_json::Value> {
        use serde_json::Value;
        let Ok(root) = serde_json::from_str::<Value>(frame) else {
            return Vec::new();
        };
        let items = match root {
            Value::Array(a) => a,
            v => vec![v],
        };
        let mut out = Vec::with_capacity(items.len());
        for mut e in items {
            let ts = crate::key::clob_ts(&e);
            match e.get("event_type").and_then(Value::as_str) {
                Some("book") => {
                    if let (Some(a), Some(t)) = (e.get("asset_id").and_then(Value::as_str), ts) {
                        let slot = self.latest_book.entry(a.to_string()).or_insert(t);
                        *slot = (*slot).max(t);
                    }
                    out.push(e);
                }
                Some("price_change") => {
                    let Some(t) = ts else {
                        out.push(e);
                        continue;
                    };
                    let mut dropped = 0;
                    if let Some(changes) = e.get_mut("price_changes").and_then(Value::as_array_mut)
                    {
                        changes.retain(|c| {
                            let stale = c
                                .get("asset_id")
                                .and_then(Value::as_str)
                                .and_then(|a| self.latest_book.get(a))
                                .is_some_and(|&b| t < b);
                            dropped += stale as u64;
                            !stale
                        });
                        let empty = changes.is_empty();
                        self.dropped += dropped;
                        if empty && dropped > 0 {
                            continue;
                        }
                    }
                    out.push(e);
                }
                _ => out.push(e),
            }
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::key::KeyKind;
    use std::collections::HashSet;

    const B1: Boot = Boot(0x1111);
    const B2: Boot = Boot(0x2222);

    fn hello(boot: Boot, mode: Mode, next: u64, complete: bool) -> String {
        wire::encode(&NodeMsg::Hello {
            v: 1,
            node: 1,
            boot,
            mode,
            next,
            oldest: 1,
            head: next.saturating_sub(1),
            complete,
        })
    }

    fn trade(a: u64) -> String {
        format!(
            r#"{{"stream":"btcusdt@aggTrade","data":{{"a":{a},"T":{t},"p":"1","q":"1"}}}}"#,
            t = 1_000 + a
        )
    }

    fn ev(seq: u64, frame: &str, snap: bool) -> String {
        wire::encode_ev(seq, Source::Binance, 0, snap, frame)
    }

    fn snap_end(next: u64) -> String {
        wire::encode(&NodeMsg::SnapEnd { next })
    }

    fn hb() -> String {
        wire::encode(&NodeMsg::Hb {
            head: 0,
            epoch: 0,
            leader: false,
        })
    }

    struct T {
        core: Core,
        now: Instant,
        out: Vec<Output>,
    }

    impl T {
        fn new(nodes: usize) -> T {
            T::with(ClientConfig::new(nodes))
        }

        fn with(cfg: ClientConfig) -> T {
            T {
                core: Core::new(cfg),
                now: Instant::now(),
                out: Vec::new(),
            }
        }

        fn send(&mut self, node: usize, text: &str) {
            self.core.on_text(node, self.now, text, &mut self.out);
        }

        fn advance(&mut self, ms: u64) {
            self.now += Duration::from_millis(ms);
            self.core.tick(self.now, &mut self.out);
        }

        /// Open a fresh snapshot connection that is live at `next`.
        fn open(&mut self, node: usize, boot: Boot, next: u64) {
            self.core.dial(node, self.now);
            self.send(node, &hello(boot, Mode::Snapshot, next, true));
            self.send(node, &snap_end(next));
        }

        fn delivered(&self) -> Vec<(usize, EventKey)> {
            self.out
                .iter()
                .filter_map(|o| match o {
                    Output::Event(e) => Some(e),
                    _ => None,
                })
                .flat_map(|e| {
                    e.keys
                        .iter()
                        .filter(|(_, v)| *v == Verdict::Deliver)
                        .map(move |(k, _)| (e.node, *k))
                })
                .collect()
        }

        fn downs(&self) -> Vec<(usize, DownReason)> {
            self.out
                .iter()
                .filter_map(|o| match o {
                    Output::Down { node, reason, .. } => Some((*node, *reason)),
                    _ => None,
                })
                .collect()
        }
    }

    #[test]
    fn two_nodes_merge_to_one_copy_per_key() {
        let mut t = T::new(2);
        t.open(0, B1, 1);
        t.open(1, B2, 1);
        assert_eq!(t.core.state(0), ConnState::Live);
        for a in 1..=3 {
            t.send(0, &ev(a, &trade(a), false));
        }
        for a in 1..=4 {
            t.send(1, &ev(a, &trade(a), false));
        }
        let d = t.delivered();
        assert_eq!(d.len(), 4);
        assert_eq!(d.iter().filter(|(n, _)| *n == 1).count(), 1);
        assert_eq!(t.core.merger().totals().duplicates, 3);
        assert_eq!(t.core.merger().first_from(), &[3, 1]);
        let c = t.core.counters();
        assert_eq!((c.seq_gaps, c.resume_violations, c.unkeyed), (0, 0, 0));
        assert_eq!(c.self_repeats, 0);
    }

    #[test]
    fn a_seq_gap_is_counted_dropped_and_resumed_from_the_last_contiguous_seq() {
        let mut t = T::new(1);
        t.open(0, B1, 10);
        t.send(0, &ev(10, &trade(10), false));
        t.send(0, &ev(11, &trade(11), false));
        t.send(0, &ev(13, &trade(13), false));
        assert_eq!(t.core.counters().seq_gaps, 1);
        assert_eq!(t.downs(), vec![(0, DownReason::SeqGap)]);
        // The out-of-order frame was not offered.
        assert_eq!(t.delivered().len(), 2);
        assert!(t.core.should_dial(0));
        let sub = t.core.dial(0, t.now);
        let parsed = wire::decode_sub(&sub).unwrap();
        assert_eq!(parsed.resume, Some(ResumeFrom { boot: B1, seq: 11 }));
        assert_eq!(t.core.counters().reconnects, 1);
        // An honest resume is not a violation and continues the stream.
        t.send(0, &hello(B1, Mode::Resume, 12, true));
        t.send(0, &ev(12, &trade(12), false));
        t.send(0, &ev(13, &trade(13), false));
        assert_eq!(t.core.counters().resume_violations, 0);
        assert_eq!(t.core.counters().seq_gaps, 1);
        assert_eq!(t.delivered().len(), 4);
    }

    #[test]
    fn a_resume_that_skips_ahead_is_a_violation() {
        let mut t = T::new(1);
        t.open(0, B1, 1);
        t.send(0, &ev(1, &trade(1), false));
        t.core.on_closed(0, t.now, &mut t.out);
        t.core.dial(0, t.now);
        t.send(0, &hello(B1, Mode::Resume, 50, true));
        assert_eq!(t.core.counters().resume_violations, 1);
        // A resume nobody asked for is one as well.
        let mut u = T::new(1);
        u.core.dial(0, u.now);
        u.send(0, &hello(B1, Mode::Resume, 1, true));
        assert_eq!(u.core.counters().resume_violations, 1);
    }

    #[test]
    fn silence_for_the_dead_time_ends_the_connection() {
        let mut t = T::new(2);
        t.open(0, B1, 1);
        t.open(1, B2, 1);
        t.advance(600);
        t.send(0, &hb());
        let heard = t.now;
        t.advance(600);
        // Node 1 said nothing for 1.2 s; node 0 spoke 0.6 s ago.
        assert_eq!(t.downs(), vec![(1, DownReason::Dead)]);
        t.advance(400);
        assert_eq!(
            t.downs(),
            vec![(1, DownReason::Dead), (0, DownReason::Dead)]
        );
        let last = t.out.iter().rev().find_map(|o| match o {
            Output::Down { last_msg, .. } => *last_msg,
            _ => None,
        });
        assert_eq!(last, Some(heard));
        // A dial that never gets a hello is dead too.
        t.core.dial(0, t.now);
        t.advance(1_000);
        assert_eq!(t.downs().last(), Some(&(0, DownReason::Dead)));
    }

    #[test]
    fn a_new_boot_is_a_reset_and_an_incomplete_snapshot_is_counted() {
        let mut t = T::new(1);
        t.open(0, B1, 1);
        t.send(0, &ev(1, &trade(1), false));
        t.core.on_closed(0, t.now, &mut t.out);
        let sub = t.core.dial(0, t.now);
        assert!(wire::decode_sub(&sub).unwrap().resume.is_some());
        t.send(0, &hello(B2, Mode::Snapshot, 7, false));
        t.send(0, &ev(3, &trade(3), true));
        t.send(0, &snap_end(7));
        t.send(0, &ev(7, &trade(7), false));
        let c = t.core.counters();
        assert_eq!((c.resets, c.incomplete_snapshots, c.seq_gaps), (1, 1, 0));
        assert_eq!(t.delivered().len(), 3);
    }

    #[test]
    fn protocol_breaks_and_bye_end_the_connection() {
        let mut t = T::new(1);
        t.open(0, B1, 1);
        t.send(0, &ev(1, &trade(1), true));
        assert_eq!(t.downs(), vec![(0, DownReason::Protocol)]);
        t.open(0, B1, 1);
        t.send(
            0,
            &wire::encode(&NodeMsg::Bye {
                reason: ByeReason::Lagged,
            }),
        );
        assert_eq!(t.downs()[1], (0, DownReason::Bye(ByeReason::Lagged)));
        t.open(0, B1, 1);
        t.send(0, "not json");
        assert_eq!(t.downs()[2], (0, DownReason::Protocol));
        // A closed socket after the end is not a second disconnect.
        t.core.on_closed(0, t.now, &mut t.out);
        assert_eq!(t.core.counters().disconnects, 3);
    }

    #[test]
    fn unkeyed_events_self_repeats_and_inversions_are_counted() {
        let mut t = T::new(2);
        t.open(0, B1, 1);
        t.open(1, B2, 1);
        t.send(
            0,
            &wire::encode_ev(
                1,
                Source::Rtds,
                0,
                false,
                r#"{"topic":"other","payload":{"x":1}}"#,
            ),
        );
        assert_eq!(t.core.counters().unkeyed, 1);
        t.send(0, &ev(2, &trade(5), false));
        t.send(0, &ev(3, &trade(5), false));
        assert_eq!(t.core.counters().self_repeats, 1);
        // Node 1 delivers an older trade after node 0's newer one.
        t.send(1, &ev(1, &trade(2), false));
        assert_eq!(t.core.counters().order_inversions, 1);
        let kinds: Vec<KeyKind> = t.delivered().iter().map(|(_, k)| k.kind).collect();
        assert!(kinds.contains(&KeyKind::Unkeyed(Feed::Rtds)));
    }

    #[test]
    fn the_client_mutants_change_what_they_claim_to() {
        let mut t = T::with(ClientConfig::new(2).with_mutant(ClientMutant::NoDedup));
        t.open(0, B1, 1);
        t.open(1, B2, 1);
        t.send(0, &ev(1, &trade(1), false));
        t.send(1, &ev(1, &trade(1), false));
        assert_eq!(t.delivered().len(), 2);

        let single = Core::new(ClientConfig::new(2).with_mutant(ClientMutant::SingleNode));
        assert!(single.should_dial(0) && !single.should_dial(1));

        let mut once = T::with(ClientConfig::new(2).with_mutant(ClientMutant::NoReconnect));
        once.open(0, B1, 1);
        once.core.on_closed(0, once.now, &mut once.out);
        assert!(!once.core.should_dial(0));
        assert!(once.core.should_dial(1));

        let real = Core::new(ClientConfig::new(2));
        assert!(real.should_dial(0) && real.should_dial(1));

        // A copy older than the window: dropped as late, unless the mutant
        // passes it on.
        for (mutant, want) in [(ClientMutant::None, 1), (ClientMutant::LateDeliver, 2)] {
            let mut t = T::with(ClientConfig::new(1).with_mutant(mutant));
            t.open(0, B1, 1);
            t.send(0, &ev(1, &trade(20_000), false));
            t.send(0, &ev(2, &trade(1), false));
            assert_eq!(t.delivered().len(), want, "{mutant:?}");
            assert_eq!(t.core.merger().totals().late, 1);
        }
    }

    #[test]
    fn a_gap_at_the_same_seq_twice_takes_a_snapshot_next() {
        let mut t = T::new(1);
        t.open(0, B1, 10);
        t.send(0, &ev(10, &trade(10), false));
        t.send(0, &ev(12, &trade(12), false));
        let sub = wire::decode_sub(&t.core.dial(0, t.now)).unwrap();
        assert_eq!(sub.resume, Some(ResumeFrom { boot: B1, seq: 10 }));
        // The node resumes honestly and skips the same frame again.
        t.send(0, &hello(B1, Mode::Resume, 11, true));
        t.send(0, &ev(12, &trade(12), false));
        assert_eq!(t.core.counters().seq_gaps, 2);
        let sub = wire::decode_sub(&t.core.dial(0, t.now)).unwrap();
        assert_eq!(sub.resume, None, "a third resume would hit the same gap");
        // The snapshot moves the stream past it, counted as a resync.
        t.send(0, &hello(B1, Mode::Snapshot, 13, true));
        t.send(0, &snap_end(13));
        t.send(0, &ev(13, &trade(13), false));
        let c = t.core.counters();
        assert_eq!((c.seq_gaps, c.resyncs, c.resets), (2, 1, 0));
        // Progress since the gap means the next gap resumes again.
        t.send(0, &ev(15, &trade(15), false));
        let sub = wire::decode_sub(&t.core.dial(0, t.now)).unwrap();
        assert_eq!(sub.resume, Some(ResumeFrom { boot: B1, seq: 13 }));
    }

    fn rtds_update(ts: i64) -> String {
        format!(
            r#"{{"topic":"crypto_prices","type":"update","timestamp":{ts},"payload":{{"symbol":"btcusdt","timestamp":{ts},"value":1.5}}}}"#
        )
    }

    fn rtds_history(ts: &[i64]) -> String {
        let pts: Vec<String> = ts
            .iter()
            .map(|t| format!(r#"{{"timestamp":{t},"value":1.5}}"#))
            .collect();
        format!(
            r#"{{"topic":"crypto_prices","type":"subscribe","payload":{{"symbol":"btcusdt","data":[{}]}}}}"#,
            pts.join(",")
        )
    }

    fn last_event(t: &T) -> Event {
        t.out
            .iter()
            .rev()
            .find_map(|o| match o {
                Output::Event(e) => Some(e.clone()),
                _ => None,
            })
            .expect("an event")
    }

    fn keys_of(src: Source, frame: &str) -> Vec<EventKey> {
        let mut out = Vec::new();
        frame_keys(src, frame, &mut out);
        out
    }

    /// Every event a frame consumer applies, keyed, with whole frames
    /// (`whole`) or with each frame cut to its delivered events.
    fn applied(t: &T, whole: bool) -> Vec<EventKey> {
        let mut out = Vec::new();
        for o in &t.out {
            let Output::Event(e) = o else { continue };
            let f = if whole {
                e.keys
                    .iter()
                    .any(|(_, v)| *v == Verdict::Deliver)
                    .then(|| e.frame.clone())
            } else {
                e.delivered_frame()
            };
            if let Some(f) = f {
                out.extend(keys_of(e.src, &f));
            }
        }
        out
    }

    fn once_each(keys: &[EventKey]) -> bool {
        let set: HashSet<_> = keys.iter().collect();
        set.len() == keys.len()
    }

    #[test]
    fn a_mixed_verdict_frame_is_cut_to_its_delivered_events() {
        let mut t = T::new(2);
        t.open(0, B1, 1);
        t.open(1, B2, 1);
        let rtds = |seq, f: &str| wire::encode_ev(seq, Source::Rtds, 0, false, f);
        // Node 0 delivers two live updates; node 1, just reconnected to the
        // venue, sends its history snapshot of three points.
        t.send(0, &rtds(1, &rtds_update(1_000)));
        t.send(0, &rtds(2, &rtds_update(20_000)));
        t.send(1, &rtds(1, &rtds_history(&[1_000, 15_000, 20_000])));
        let e = last_event(&t);
        let v: Vec<Verdict> = e.keys.iter().map(|(_, v)| *v).collect();
        assert_eq!(v, vec![Verdict::Late, Verdict::Deliver, Verdict::Duplicate]);
        let cut = e.delivered_frame().expect("one point delivered");
        assert_eq!(
            keys_of(Source::Rtds, &cut),
            keys_of(Source::Rtds, &rtds_update(15_000))
        );
        assert!(cut.contains(r#""type":"subscribe""#), "venue shape kept");

        // A CLOB array: a book and a two-entry price change, one entry of
        // which node 0 already delivered.
        let c1 = r#"{"asset_id":"a","price":"0.40","size":"5","side":"BUY","hash":"h1"}"#;
        let c2 = r#"{"asset_id":"a","price":"0.41","size":"6","side":"BUY","hash":"h2"}"#;
        let pc = |cs: &[&str]| {
            format!(
                r#"{{"event_type":"price_change","timestamp":"50","price_changes":[{}]}}"#,
                cs.join(",")
            )
        };
        let book = r#"{"event_type":"book","asset_id":"a","hash":"x","timestamp":"40","bids":[],"asks":[]}"#;
        let clob = |seq, f: &str| wire::encode_ev(seq, Source::ClobBook, 0, false, f);
        t.send(0, &clob(3, &pc(&[c1])));
        t.send(1, &clob(2, &format!("[{book},{}]", pc(&[c1, c2]))));
        let e = last_event(&t);
        let v: Vec<Verdict> = e.keys.iter().map(|(_, v)| *v).collect();
        assert_eq!(
            v,
            vec![Verdict::Deliver, Verdict::Duplicate, Verdict::Deliver]
        );
        let cut = e.delivered_frame().unwrap();
        let mut want = keys_of(Source::ClobBook, book);
        want.extend(keys_of(Source::ClobBook, &pc(&[c2])));
        assert_eq!(keys_of(Source::ClobBook, &cut), want);
        let mut view = BookView::new();
        let items = view.admit_event(&e);
        assert_eq!(items.len(), 2);
        assert_eq!(items[1]["price_changes"].as_array().unwrap().len(), 1);
        assert_eq!(items[1]["price_changes"][0]["hash"], "h2");
        assert_eq!(t.core.counters().mixed_frames, 2);

        // A frame with nothing delivered gives nothing, a whole one itself.
        t.send(1, &clob(3, &pc(&[c1])));
        assert_eq!(last_event(&t).delivered_frame(), None);
        assert!(view.admit_event(&last_event(&t)).is_empty());
        t.send(0, &clob(4, &pc(&["{\"asset_id\":\"b\",\"price\":\"0.5\",\"size\":\"1\",\"side\":\"SELL\",\"hash\":\"h3\"}"])));
        let e = last_event(&t);
        assert_eq!(e.delivered_frame().as_deref(), Some(e.frame.as_str()));

        // The point of the cut: a consumer applying whole frames applies
        // the duplicate and late parts again; the cut applies each once.
        assert!(!once_each(&applied(&t, true)));
        let cut_all = applied(&t, false);
        assert!(once_each(&cut_all));
        let mut delivered: Vec<EventKey> = t.delivered().into_iter().map(|(_, k)| k).collect();
        let mut got = cut_all.clone();
        delivered.sort_by_key(|k| (k.ts_millis, k.id));
        got.sort_by_key(|k| (k.ts_millis, k.id));
        assert_eq!(got, delivered);
    }

    #[test]
    fn a_cut_that_does_not_line_up_with_the_keys_gives_nothing() {
        let f = rtds_history(&[1, 2]);
        let mut keys: Vec<(EventKey, Verdict)> = keys_of(Source::Rtds, &f)
            .into_iter()
            .zip([Verdict::Deliver, Verdict::Duplicate])
            .collect();
        let e = |keys: Vec<(EventKey, Verdict)>| Event {
            node: 0,
            seq: 1,
            src: Source::Rtds,
            rx_micros: 0,
            snap: false,
            frame: f.clone(),
            keys,
        };
        assert!(e(keys.clone()).delivered_frame().is_some());
        keys.swap(0, 1);
        assert_eq!(e(keys).delivered_frame(), None);
    }

    #[test]
    fn backoff_doubles_from_100_ms_to_2_s() {
        assert_eq!(backoff(0, 1.0), Duration::from_millis(100));
        assert_eq!(backoff(0, 0.5), Duration::from_millis(100));
        assert_eq!(backoff(1, 1.0), Duration::from_millis(200));
        assert_eq!(backoff(3, 0.5), Duration::from_millis(400));
        assert_eq!(backoff(9, 1.0), Duration::from_secs(2));
        assert_eq!(backoff(40, 0.5), Duration::from_secs(1));
        let mut s = jitter_seed();
        for _ in 0..1_000 {
            let j = next_jitter(&mut s);
            assert!((0.5..=1.0).contains(&j));
        }
    }

    #[test]
    fn book_view_drops_deltas_older_than_the_latest_book() {
        let mut v = BookView::new();
        let pc = |ts: i64, asset: &str| {
            format!(
                r#"{{"event_type":"price_change","timestamp":"{ts}","price_changes":[{{"asset_id":"{asset}","hash":"h","price":"0.4","size":"1","side":"BUY"}}]}}"#
            )
        };
        // Before any book every delta is kept.
        assert_eq!(v.admit(&pc(5, "a")).len(), 1);
        let book = r#"{"event_type":"book","asset_id":"a","hash":"x","timestamp":"10","bids":[],"asks":[]}"#;
        assert_eq!(v.admit(book).len(), 1);
        assert_eq!(v.book_ts("a"), Some(10));
        assert!(v.admit(&pc(9, "a")).is_empty());
        assert_eq!(v.dropped, 1);
        assert_eq!(v.admit(&pc(10, "a")).len(), 1);
        assert_eq!(v.admit(&pc(9, "b")).len(), 1);
        // An older book does not move the view back.
        let old = r#"{"event_type":"book","asset_id":"a","hash":"y","timestamp":"3","bids":[],"asks":[]}"#;
        v.admit(old);
        assert_eq!(v.book_ts("a"), Some(10));
        // Mixed entries: only the stale one goes.
        let mixed = r#"[{"event_type":"price_change","timestamp":"8","price_changes":[
            {"asset_id":"a","hash":"h","price":"0.4","size":"1","side":"BUY"},
            {"asset_id":"b","hash":"h","price":"0.6","size":"1","side":"SELL"}]}]"#;
        let kept = v.admit(mixed);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0]["price_changes"].as_array().unwrap().len(), 1);
        assert_eq!(v.dropped, 2);
    }

    #[tokio::test]
    async fn the_driver_redials_and_resumes_after_a_drop() {
        use feeds::testkit::{serve, text, Step};
        // First connection: snapshot, two live frames, then the socket drops.
        // Second: the node resumes where the client left off.
        let url = serve(vec![
            vec![
                text(hello(B1, Mode::Snapshot, 1, true)),
                text(snap_end(1)),
                text(ev(1, &trade(1), false)),
                text(ev(2, &trade(2), false)),
                Step::Drop,
            ],
            vec![
                text(hello(B1, Mode::Resume, 3, true)),
                text(ev(3, &trade(3), false)),
                Step::Silent(Duration::from_secs(30)),
            ],
        ])
        .await;
        let (done_tx, done_rx) = tokio::sync::oneshot::channel::<()>();
        let mut done_tx = Some(done_tx);
        let mut seqs = Vec::new();
        let mut downs = 0;
        let core = run(
            vec![url],
            ClientConfig::new(1),
            async {
                let _ = tokio::time::timeout(Duration::from_secs(10), done_rx).await;
            },
            |o| match o {
                Output::Event(e) => {
                    seqs.push(e.seq);
                    if e.seq == 3 {
                        if let Some(tx) = done_tx.take() {
                            let _ = tx.send(());
                        }
                    }
                }
                Output::Down { .. } => downs += 1,
                Output::Live { .. } => {}
            },
        )
        .await;
        assert_eq!(seqs, vec![1, 2, 3]);
        assert_eq!(downs, 1);
        let c = core.counters();
        assert_eq!((c.reconnects, c.seq_gaps, c.resume_violations), (1, 0, 0));
    }
}
