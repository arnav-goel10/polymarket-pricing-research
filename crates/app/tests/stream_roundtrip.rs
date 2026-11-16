//! The `node` binary end to end: three scripted venues feed it, clients
//! subscribe over the wire protocol, and the stream is checked against the
//! segments the node wrote.
//!
//! The same checks that pass on the real node are run against the node's
//! seeded faults (build with `--features mutants`), and each fault must be
//! caught by the check it targets.

#![cfg(unix)]

use feeds::recording::{read_all, Record, Source};
use feeds::testkit::serve_broadcast;
use futures_util::{SinkExt, StreamExt};
use ha::key::{frame_keys, FrameKind};
use ha::wire::{self, Boot, ByeReason, Mode, NodeMsg, ResumeFrom};
use std::collections::{BTreeMap, HashSet};
use std::io::{BufRead, BufReader, Read};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Message;

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Three venues that publish unique, keyed frames about 500 times a second
/// each. The order-book venue sends a fresh `book` to every new connection.
struct Venues {
    rtds: String,
    binance: String,
    clob: String,
    pump: tokio::task::JoinHandle<()>,
}

impl Drop for Venues {
    fn drop(&mut self) {
        self.pump.abort();
    }
}

async fn venues() -> Venues {
    venues_with(0).await
}

/// As [`venues`], with `pad` bytes of padding in every order-book change,
/// so a stream carries megabytes a second and fills socket buffers fast.
async fn venues_with(pad: usize) -> Venues {
    let pad = "x".repeat(pad);
    let (rt, _) = broadcast::channel::<String>(8192);
    let (bn, _) = broadcast::channel::<String>(8192);
    let (cl, _) = broadcast::channel::<String>(8192);
    let rtds = serve_broadcast(rt.clone(), Vec::<String>::new).await;
    let binance = serve_broadcast(bn.clone(), Vec::<String>::new).await;
    let books = Arc::new(AtomicU64::new(0));
    let clob = serve_broadcast(cl.clone(), move || {
        let b = books.fetch_add(1, Relaxed);
        vec![format!(
            r#"{{"event_type":"book","asset_id":"tok","hash":"book{b}","timestamp":"{b}","bids":[{{"price":"0.4","size":"1"}}],"asks":[{{"price":"0.6","size":"1"}}]}}"#
        )]
    })
    .await;
    let pump = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_millis(2));
        let mut i: u64 = 1_000;
        loop {
            tick.tick().await;
            i += 1;
            let _ = rt.send(format!(
                r#"{{"topic":"crypto_prices","type":"update","payload":{{"symbol":"btcusdt","value":{i}.5,"timestamp":{i}}}}}"#
            ));
            let _ = bn.send(format!(
                r#"{{"stream":"btcusdt@aggTrade","data":{{"a":{i},"T":{i},"p":"100000","q":"1","m":false}}}}"#
            ));
            let _ = cl.send(format!(
                r#"{{"event_type":"price_change","pad":"{pad}","timestamp":"{i}","price_changes":[{{"asset_id":"tok","hash":"h{i}","price":"0.41","size":"{i}","side":"BUY","best_bid":"0.41","best_ask":"0.6"}}]}}"#
            ));
        }
    });
    Venues {
        rtds,
        binance,
        clob,
        pump,
    }
}

struct Node {
    child: Child,
    stdout: BufReader<ChildStdout>,
    stream: String,
    http: SocketAddr,
    boot: Boot,
    data: PathBuf,
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("node-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn spawn_node(v: &Venues, name: &str, extra: &[&str], mutant: Option<&str>) -> Node {
    let data = temp_dir(name);
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_node"));
    cmd.args(["--data", data.to_str().unwrap()])
        .args(["--listen", "127.0.0.1:0", "--http", "127.0.0.1:0"])
        .args(["--rtds-url", &v.rtds, "--binance-url", &v.binance])
        .args(["--clob-url", &v.clob, "--token", "tok"])
        .args(extra)
        .env("RUST_LOG", "warn")
        .env_remove("POLY_MUTANT")
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    if let Some(m) = mutant {
        cmd.env("POLY_MUTANT", m);
    }
    let mut child = cmd.spawn().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    // node <id> boot <hex> stream <addr> http <addr>
    let w: Vec<&str> = line.split_whitespace().collect();
    assert_eq!(w.len(), 8, "start line: {line:?}");
    assert_eq!((w[0], w[2], w[4], w[6]), ("node", "boot", "stream", "http"));
    Node {
        boot: w[3].parse().unwrap(),
        stream: format!("ws://{}", w[5]),
        http: w[7].parse().unwrap(),
        child,
        stdout,
        data,
    }
}

impl Node {
    fn stop(mut self) -> String {
        let ok = Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .unwrap();
        assert!(ok.success());
        let started = Instant::now();
        let status = loop {
            if let Some(s) = self.child.try_wait().unwrap() {
                break s;
            }
            assert!(
                started.elapsed() < Duration::from_secs(15),
                "node did not exit"
            );
            std::thread::sleep(Duration::from_millis(50));
        };
        assert!(status.success(), "exit status {status:?}");
        let mut rest = String::new();
        self.stdout.read_to_string(&mut rest).unwrap();
        rest
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn http_get(addr: SocketAddr, path: &str) -> Option<(u16, String)> {
    let mut s = tokio::net::TcpStream::connect(addr).await.ok()?;
    let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    s.write_all(req.as_bytes()).await.ok()?;
    let mut buf = String::new();
    s.read_to_string(&mut buf).await.ok()?;
    let code = buf.split_whitespace().nth(1)?.parse().ok()?;
    Some((code, buf))
}

async fn wait_ready(n: &Node) {
    let started = Instant::now();
    loop {
        if let Some((200, _)) = http_get(n.http, "/readyz").await {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "node never ready"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Let some frames into the ring, including the order book.
    tokio::time::sleep(Duration::from_millis(400)).await;
}

async fn next_msg(ws: &mut Ws) -> NodeMsg {
    loop {
        let m = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("node silent for 5 s")
            .expect("stream ended")
            .expect("websocket error");
        if let Message::Text(t) = m {
            return wire::decode_node(&t).unwrap_or_else(|e| panic!("bad message {t}: {e}"));
        }
    }
}

struct Hello {
    boot: Boot,
    mode: Mode,
    next: u64,
    complete: bool,
}

async fn subscribe(url: &str, resume: Option<ResumeFrom>) -> (Ws, Hello) {
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    ws.send(Message::Text(wire::encode_sub(resume)))
        .await
        .unwrap();
    match next_msg(&mut ws).await {
        NodeMsg::Hello {
            v,
            boot,
            mode,
            next,
            oldest,
            head,
            complete,
            ..
        } => {
            assert_eq!(v, 1);
            assert!(
                oldest <= head + 1 && next <= head + 1,
                "hello {oldest} {next} {head}"
            );
            (
                ws,
                Hello {
                    boot,
                    mode,
                    next,
                    complete,
                },
            )
        }
        m => panic!("expected hello, got {m:?}"),
    }
}

/// What a client saw, with the counters the HA checks gate on.
#[derive(Default)]
struct Seen {
    live: BTreeMap<u64, (u8, String)>,
    snapshot: Vec<(u64, u8, String)>,
    seq_gaps: u64,
    resume_violations: u64,
}

impl Seen {
    async fn read_snapshot(&mut self, ws: &mut Ws, hello: &Hello) {
        loop {
            match next_msg(ws).await {
                NodeMsg::Ev {
                    seq,
                    src,
                    snap: true,
                    frame,
                    ..
                } => self.snapshot.push((seq, src, frame)),
                NodeMsg::SnapEnd { next } => {
                    assert_eq!(next, hello.next);
                    return;
                }
                m => panic!("unexpected in snapshot: {m:?}"),
            }
        }
    }

    /// Read live events until `until` has been received. Returns the last
    /// sequence number received in order.
    async fn read_live(&mut self, ws: &mut Ws, mut expect: u64, until: u64) -> u64 {
        while expect <= until {
            match next_msg(ws).await {
                NodeMsg::Ev {
                    seq,
                    src,
                    snap,
                    frame,
                    ..
                } => {
                    assert!(!snap, "snapshot frame after snap_end");
                    if seq != expect {
                        self.seq_gaps += 1;
                    }
                    expect = seq + 1;
                    self.live.insert(seq, (src, frame));
                }
                NodeMsg::Hb { .. } => {}
                m => panic!("unexpected live message: {m:?}"),
            }
        }
        expect - 1
    }

    fn check_resume(&mut self, hello: &Hello, last: u64) {
        if hello.mode == Mode::Resume && hello.next != last + 1 {
            self.resume_violations += 1;
        }
    }

    /// Keys delivered more than once, derived the way a client derives them.
    fn duplicate_keys(&self) -> usize {
        let mut seen = HashSet::new();
        let mut dups = 0;
        let mut keys = Vec::new();
        for (src, frame) in self.live.values() {
            keys.clear();
            frame_keys(Source::from_u8(*src).unwrap(), frame, &mut keys);
            for k in &keys {
                if !seen.insert(*k) {
                    dups += 1;
                }
            }
        }
        dups
    }
}

/// Every record in the node's closed segments, in slot order.
fn segments(data: &Path) -> Vec<Record> {
    let dir = data.join("seg");
    let mut files: Vec<(i64, PathBuf)> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .map(|p| {
            let name = p.file_name().unwrap().to_str().unwrap().to_string();
            assert!(name.ends_with(".prec"), "segment left open: {name}");
            (name.split('.').next().unwrap().parse().unwrap(), p)
        })
        .collect();
    assert!(!files.is_empty(), "no segments written");
    files.sort();
    let mut out = Vec::new();
    for (_, p) in files {
        let (recs, tail) = read_all(&p).unwrap();
        assert!(tail.is_none(), "truncated segment {p:?}");
        out.extend(recs);
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn node_streams_resumes_and_matches_its_segments() {
    let v = venues().await;
    let node = spawn_node(&v, "roundtrip", &[], None);
    wait_ready(&node).await;

    let (status, ha) = http_get(node.http, "/ha").await.unwrap();
    assert_eq!(status, 200);
    assert!(ha.contains(&format!(r#""boot":"{}""#, node.boot)), "{ha}");
    let (_, metrics) = http_get(node.http, "/metrics").await.unwrap();
    assert!(metrics.contains("poly_node_head_seq "), "{metrics}");
    assert!(
        metrics.contains(r#"poly_feed_connected{feed="clob_book"} 1"#),
        "{metrics}"
    );

    // A fresh client gets a snapshot that can rebuild every feed.
    let mut seen = Seen::default();
    let (mut ws, hello) = subscribe(&node.stream, None).await;
    assert_eq!(hello.mode, Mode::Snapshot);
    assert_eq!(hello.boot, node.boot);
    assert!(hello.complete);
    seen.read_snapshot(&mut ws, &hello).await;
    let snap = &seen.snapshot;
    assert!(
        snap.windows(2).all(|w| w[0].0 < w[1].0),
        "snapshot out of order"
    );
    let book = snap
        .iter()
        .position(|(_, s, f)| *s == 3 && f.contains(r#""event_type":"book""#))
        .expect("snapshot has the order book");
    assert!(snap.iter().any(|(_, s, _)| *s == 1), "snapshot has RTDS");
    assert!(snap.iter().any(|(_, s, _)| *s == 2), "snapshot has Binance");
    let deltas = snap[book + 1..].iter().filter(|(_, s, _)| *s == 3).count();
    assert!(deltas > 0, "snapshot has the book changes since the book");
    let last = seen.read_live(&mut ws, hello.next, hello.next + 400).await;
    ws.close(None).await.unwrap();
    drop(ws);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Resuming from the last seq continues exactly after it.
    let resume = ResumeFrom {
        boot: node.boot,
        seq: last,
    };
    let (mut ws, hello) = subscribe(&node.stream, Some(resume)).await;
    assert_eq!(hello.mode, Mode::Resume);
    assert_eq!(hello.next, last + 1);
    seen.check_resume(&hello, last);
    let last = seen.read_live(&mut ws, hello.next, last + 600).await;

    // Another boot's resume point gets a snapshot.
    let other = ResumeFrom {
        boot: Boot(node.boot.0 ^ 1),
        seq: last,
    };
    let (_ws3, hello3) = subscribe(&node.stream, Some(other)).await;
    assert_eq!(hello3.mode, Mode::Snapshot);

    // v1 serves all feeds or none.
    let (mut bad, _) = tokio_tungstenite::connect_async(&node.stream)
        .await
        .unwrap();
    bad.send(Message::Text(
        r#"{"t":"sub","v":1,"feeds":["rtds"]}"#.into(),
    ))
    .await
    .unwrap();
    assert_eq!(
        next_msg(&mut bad).await,
        NodeMsg::Bye {
            reason: ByeReason::BadRequest
        }
    );

    // The real node: no gap, no resume violation, no repeated key.
    assert_eq!(seen.seq_gaps, 0);
    assert_eq!(seen.resume_violations, 0);
    assert_eq!(seen.duplicate_keys(), 0);

    // Shutdown tells the client why.
    let data = node.data.clone();
    let stop = tokio::task::spawn_blocking(move || node.stop());
    // next_msg panics if the stream ends without a bye.
    loop {
        match next_msg(&mut ws).await {
            NodeMsg::Bye { reason } => {
                assert_eq!(reason, ByeReason::Shutdown);
                break;
            }
            NodeMsg::Ev {
                seq, src, frame, ..
            } => {
                seen.live.insert(seq, (src, frame));
            }
            _ => {}
        }
    }
    let out = stop.await.unwrap();
    assert!(out.contains("node stopped"), "{out}");
    assert!(out.contains("dropped 0"), "{out}");

    // Every streamed frame is the matching venue frame in the segments:
    // seq n is the n-th venue frame the node wrote.
    let recs = segments(&data);
    let mut keys = Vec::new();
    let venue: Vec<&Record> = recs
        .iter()
        .filter(|r| {
            keys.clear();
            frame_keys(r.source, &r.frame, &mut keys) == FrameKind::Venue
        })
        .collect();
    assert!(
        recs.iter().any(|r| r.source == Source::Control),
        "subscribe record kept"
    );
    let max = *seen.live.keys().last().unwrap();
    assert!(
        venue.len() as u64 >= max,
        "{} venue records, seq {max}",
        venue.len()
    );
    for (seq, (src, frame)) in &seen.live {
        let r = venue[*seq as usize - 1];
        assert_eq!(r.source as u8, *src, "seq {seq}");
        assert_eq!(&r.frame, frame, "seq {seq}");
    }
    for (seq, src, frame) in &seen.snapshot {
        let r = venue[*seq as usize - 1];
        assert_eq!(
            (r.source as u8, &r.frame),
            (*src, frame),
            "snapshot seq {seq}"
        );
    }
    let _ = std::fs::remove_dir_all(data);
}

#[tokio::test(flavor = "multi_thread")]
async fn resume_from_outside_the_ring_gets_a_snapshot() {
    let v = venues().await;
    let node = spawn_node(&v, "small-ring", &["--ring-frames", "200"], None);
    wait_ready(&node).await;
    let mut seen = Seen::default();
    let (mut ws, hello) = subscribe(&node.stream, None).await;
    seen.read_snapshot(&mut ws, &hello).await;
    let last = seen.read_live(&mut ws, hello.next, hello.next + 10).await;
    drop(ws);
    // About 1,500 frames a second against a ring of 200.
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let resume = ResumeFrom {
        boot: node.boot,
        seq: last,
    };
    let (mut ws, hello) = subscribe(&node.stream, Some(resume)).await;
    assert_eq!(hello.mode, Mode::Snapshot);
    assert!(hello.next > last + 200);
    // The book is older than the ring, so the changes since it are not all
    // there and the node says so.
    assert!(!hello.complete);
    seen.read_snapshot(&mut ws, &hello).await;
    seen.read_live(&mut ws, hello.next, hello.next + 50).await;
    assert_eq!(seen.seq_gaps, 0);
    drop(ws);
    node.stop();
}

/// The value of one unlabelled metric line.
async fn metric(n: &Node, name: &str) -> u64 {
    let (_, body) = http_get(n.http, "/metrics").await.expect("metrics");
    body.lines()
        .find_map(|l| l.strip_prefix(name)?.strip_prefix(' ')?.trim().parse().ok())
        .unwrap_or_else(|| panic!("no {name} in {body}"))
}

/// What a stall run leaves open, held until the check is done.
struct Stalled {
    _venues: Venues,
    _idle: tokio::net::TcpStream,
    _ws: Ws,
}

/// Two clients that stop reading: one opens TCP and never sends the
/// WebSocket handshake, the other subscribes and never reads its
/// snapshot. The order-book changes carry 8 KiB each and the node has run
/// 5 s, so the snapshot is about 20 MB, far past the socket buffers.
/// Returns the node, the clients and the open connection count 12 s
/// after both connected.
async fn stalled_clients(mutant: Option<&str>) -> (Node, Stalled, u64) {
    let v = venues_with(8 * 1024).await;
    let name = format!("stall-{}", mutant.unwrap_or("real"));
    let node = spawn_node(&v, &name, &["--ring-bytes", "268435456"], mutant);
    wait_ready(&node).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    let addr = node.stream.strip_prefix("ws://").unwrap().to_string();
    let idle = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(&node.stream)
        .await
        .unwrap();
    ws.send(Message::Text(wire::encode_sub(None)))
        .await
        .unwrap();
    let started = Instant::now();
    while metric(&node, "poly_node_connections").await < 2 {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "never connected"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    tokio::time::sleep(Duration::from_secs(12)).await;
    let open = metric(&node, "poly_node_connections").await;
    let held = Stalled {
        _venues: v,
        _idle: idle,
        _ws: ws,
    };
    (node, held, open)
}

#[tokio::test(flavor = "multi_thread")]
async fn clients_that_stop_reading_in_the_handshake_or_snapshot_are_dropped() {
    let (node, held, open) = stalled_clients(None).await;
    // Each is dropped 5 s after its last progress.
    assert_eq!(open, 0, "a stalled client still holds its connection");
    drop(held);
    node.stop();
}

/// A client that reads about 500 messages a second against a stream of
/// 1,500, through a 3,000 frame ring, falls out of the ring. The node must
/// say `bye lagged`, and the reconnect must land in a snapshot followed by
/// a contiguous live stream.
#[tokio::test(flavor = "multi_thread")]
async fn a_slow_reader_gets_bye_lagged_and_resyncs_through_a_snapshot() {
    let v = venues_with(8 * 1024).await;
    let node = spawn_node(&v, "lagged", &["--ring-frames", "3000"], None);
    wait_ready(&node).await;
    let mut seen = Seen::default();
    let (mut ws, hello) = subscribe(&node.stream, None).await;
    seen.read_snapshot(&mut ws, &hello).await;
    let mut expect = hello.next;
    let started = Instant::now();
    let reason = loop {
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "never lagged out"
        );
        match next_msg(&mut ws).await {
            NodeMsg::Ev { seq, snap, .. } => {
                assert!(!snap);
                if seq != expect {
                    seen.seq_gaps += 1;
                }
                expect = seq + 1;
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            NodeMsg::Hb { .. } => {}
            NodeMsg::Bye { reason } => break reason,
            m => panic!("unexpected live message: {m:?}"),
        }
    };
    assert_eq!(reason, ByeReason::Lagged);
    assert_eq!(
        seen.seq_gaps, 0,
        "the node skipped instead of saying lagged"
    );
    assert_eq!(metric(&node, "poly_node_lagged_total").await, 1);
    drop(ws);
    let last = expect - 1;
    let resume = ResumeFrom {
        boot: node.boot,
        seq: last,
    };
    let (mut ws, hello) = subscribe(&node.stream, Some(resume)).await;
    assert_eq!(hello.mode, Mode::Snapshot);
    assert!(hello.next > last + 1);
    seen.read_snapshot(&mut ws, &hello).await;
    seen.read_live(&mut ws, hello.next, hello.next + 300).await;
    assert_eq!(seen.seq_gaps, 0);
    drop(ws);
    node.stop();
}

#[cfg(not(feature = "mutants"))]
#[test]
fn a_build_without_mutants_refuses_poly_mutant() {
    for m in ["seq-skip", "no-fence", "epoch-reuse"] {
        let out = Command::new(env!("CARGO_BIN_EXE_node"))
            .args(["--data", temp_dir("refuse").to_str().unwrap()])
            .args(["--listen", "127.0.0.1:0", "--http", "127.0.0.1:0"])
            .env("POLY_MUTANT", m)
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(2));
        let err = String::from_utf8_lossy(&out.stderr);
        assert!(err.contains("refusing to start"), "{err}");
        assert!(out.stdout.is_empty(), "started anyway");
    }
}

#[test]
fn the_stream_refuses_a_non_loopback_address() {
    let out = Command::new(env!("CARGO_BIN_EXE_node"))
        .args(["--data", temp_dir("public").to_str().unwrap()])
        .args(["--listen", "0.0.0.0:0"])
        .env_remove("POLY_MUTANT")
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("loopback"));
}

#[cfg(feature = "mutants")]
mod mutants {
    use super::*;

    /// Read `count` live events of a fresh stream and return what was seen.
    async fn run(mutant: &str, count: u64) -> Seen {
        let v = venues().await;
        let node = spawn_node(&v, mutant, &[], Some(mutant));
        wait_ready(&node).await;
        let mut seen = Seen::default();
        let (mut ws, hello) = subscribe(&node.stream, None).await;
        seen.read_snapshot(&mut ws, &hello).await;
        seen.read_live(&mut ws, hello.next, hello.next + count)
            .await;
        drop(ws);
        node.stop();
        seen
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn seq_skip_is_caught_as_a_gap() {
        // 1,100 consecutive numbers always include a multiple of 1,000.
        let seen = run("seq-skip", 1_100).await;
        assert!(seen.seq_gaps >= 1, "seq-skip went unseen");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn dup_frame_keeps_seq_contiguous_but_repeats_keys() {
        // At least two 500th frames fall inside 1,100.
        let seen = run("dup-frame", 1_100).await;
        assert_eq!(seen.seq_gaps, 0, "a fresh seq must not look like a gap");
        assert!(
            seen.duplicate_keys() > 0,
            "the repeated frame must show by key"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_unbounded_snapshot_keeps_stalled_clients_forever() {
        // Control for the stall test: without the bounds both clients are
        // still connected after the same wait.
        let (node, held, open) = stalled_clients(Some("unbounded-snapshot")).await;
        assert_eq!(open, 2, "the stall check would not catch the fault");
        drop(held);
        node.stop();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stale_resume_is_caught_as_a_resume_violation() {
        let v = venues().await;
        let node = spawn_node(&v, "stale-resume", &[], Some("stale-resume"));
        wait_ready(&node).await;
        let mut seen = Seen::default();
        let (mut ws, hello) = subscribe(&node.stream, None).await;
        seen.read_snapshot(&mut ws, &hello).await;
        let last = seen.read_live(&mut ws, hello.next, hello.next + 20).await;
        drop(ws);
        tokio::time::sleep(Duration::from_millis(300)).await;
        let resume = ResumeFrom {
            boot: node.boot,
            seq: last,
        };
        let (_ws, hello) = subscribe(&node.stream, Some(resume)).await;
        seen.check_resume(&hello, last);
        assert_eq!(seen.resume_violations, 1, "stale resume went unseen");
        node.stop();
    }
}
