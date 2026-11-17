//! HA ingest node: reads the three venues, writes minute segments to local
//! disk and serves a sequenced, resumable stream on loopback.
//!
//! cargo run --release -p app --bin node -- [--data DIR] [--node N]
//!     [--listen 127.0.0.1:9100] [--http 127.0.0.1:9101] [--keep-slots 60]
//!     [--ring-frames 200000] [--ring-bytes 67108864]
//!     [--rtds-url URL] [--binance-url URL] [--clob-url URL] [--token ID]
//!     [--store DIR | --azure-store URL] [--slot-ms 60000]
//!     [--lease-timing TTL,RENEW,POLL,HOLD,TAKE] [--azure-selftest ID]
//!
//! Every frame, in order: append to the open segment, compute its keys,
//! push it to the ring (see `ha::ring`), which numbers venue frames. Clients
//! speak `ha::wire` v1 over WebSocket on `--listen`, which must be a
//! loopback address; remote clients reach it through an SSH port forward.
//! `--http` serves `/healthz`, `/readyz`, `/metrics` and `/ha`, with no raw
//! data.
//!
//! Segments are `<data>/seg/<slot>.<boot>.prec.open`, where the slot is the
//! receive minute since the Unix epoch, flushed when a frame arrives a
//! second or more after the last flush and renamed to `.prec` when the next
//! minute's first frame arrives or at shutdown. Slots older than
//! `--keep-slots` are deleted. `--slot-ms` shortens the slot for tests. The
//! rules, write errors included, are in `app::segments`.
//!
//! The WebSocket handshake, the hello and every chunk of a snapshot are
//! bounded like a live batch: a client that takes over 5 s for one of them
//! is dropped, so a stalled reader never pins a snapshot.
//!
//! With `--store DIR` the node contends for the archive lease on that
//! directory store (see `app::archiver`), ships closed slots while it
//! leads, and reports the epoch and leadership in `hb`, `/ha` and
//! `/metrics`. `--lease-timing` sets ttl, renew, poll, holder margin and
//! takeover margin in milliseconds (default 30000,10000,5000,2000,1000).
//! With a store the node exits 2 unless a slot lasts at least as long as a
//! takeover (ttl, takeover margin and two polls).
//! `--azure-store https://<account>.blob.core.windows.net` puts the lease
//! and the archive on Blob Storage instead (`ha::azure`), signed with the
//! VM's managed identity; `POLY_AZURE_CLIENT_ID` names the identity when
//! the VM has several. With `--azure-selftest ID` the node only runs the
//! store contract suite under `selftest/ID/` in the archive container and
//! exits 0 or 1.
//!
//! On start the node prints one line, `node <id> boot <hex> stream <addr>
//! http <addr>`, with the bound addresses. On SIGINT or SIGTERM it sends
//! `bye shutdown` to every client, stops the feeds, drains the queue,
//! closes the open segment and exits 0.
//!
//! `POLY_MUTANT` selects a seeded fault for the HA harness and is honoured
//! only in a build with the `mutants` feature. Any other build refuses to
//! start when it is set.

use app::archiver::{Archiver, LeaseView};
use app::ingest::{shutdown_signal, spawn_feeds, Backlog, FeedHealth, FeedUrls};
use app::segments::{self, SegStats, Segments};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use feeds::recorder::queue;
use feeds::recording::Source;
use futures_util::{SinkExt, Stream, StreamExt};
use ha::azure::{AzureConfig, AzureStore};
use ha::caslease::{self, CasLease, LeaseMutant, Timing};
use ha::key::{frame_keys, EventKey};
use ha::ring::{Plan, Ring, RingConfig};
use ha::store::{FsStore, HeadStore, ObjectStore};
use ha::wire::{self, Boot, ByeReason, Mode, NodeMsg};
use std::future::Future;
use std::hash::{BuildHasher, Hasher};
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio_tungstenite::tungstenite::{self, Message};
use tracing::{debug, info, warn};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const HEARTBEAT: Duration = Duration::from_millis(250);
const SEND_TIMEOUT: Duration = Duration::from_secs(5);
const BATCH: usize = 256;

/// Seeded faults for the HA harness.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mutant {
    None,
    /// Leave out every ev whose seq is a multiple of 1,000.
    SeqSkip,
    /// Answer every resume from this boot at the head, not at seq + 1.
    StaleResume,
    /// Push every 500th venue frame a second time, under the next seq.
    DupFrame,
    /// Wait without a bound for the handshake, the hello and the snapshot.
    UnboundedSnapshot,
    /// A lease fault: `no-fence` sends renews and commits with no
    /// condition, `epoch-reuse` keeps the old epoch on a takeover.
    Lease(LeaseMutant),
}

fn mutant_from_env() -> Result<Mutant, String> {
    let Ok(v) = std::env::var("POLY_MUTANT") else {
        return Ok(Mutant::None);
    };
    if !cfg!(feature = "mutants") {
        return Err(format!(
            "POLY_MUTANT={v} is set but this build has no mutants feature; refusing to start"
        ));
    }
    match v.as_str() {
        "seq-skip" => Ok(Mutant::SeqSkip),
        "stale-resume" => Ok(Mutant::StaleResume),
        "dup-frame" => Ok(Mutant::DupFrame),
        "unbounded-snapshot" => Ok(Mutant::UnboundedSnapshot),
        "no-fence" => Ok(Mutant::Lease(LeaseMutant::Unconditional)),
        "epoch-reuse" => Ok(Mutant::Lease(LeaseMutant::ReuseEpoch)),
        _ => Err(format!("unknown POLY_MUTANT {v:?}")),
    }
}

struct Args {
    data: PathBuf,
    node: u32,
    listen: SocketAddr,
    http: SocketAddr,
    keep_slots: i64,
    ring: RingConfig,
    urls: FeedUrls,
    store: Option<PathBuf>,
    azure: Option<AzureConfig>,
    azure_selftest: Option<String>,
    slot_ms: i64,
    timing: Timing,
}

fn parse_timing(v: &str) -> Option<Timing> {
    let n: Vec<u64> = v
        .split(',')
        .map(|x| x.trim().parse().ok())
        .collect::<Option<_>>()?;
    let [ttl_ms, renew_ms, poll_ms, holder_margin_ms, takeover_margin_ms] = n[..] else {
        return None;
    };
    let ok = holder_margin_ms < ttl_ms && renew_ms < ttl_ms - holder_margin_ms && poll_ms > 0;
    ok.then_some(Timing {
        ttl_ms,
        renew_ms,
        poll_ms,
        holder_margin_ms,
        takeover_margin_ms,
    })
}

fn parse_args() -> Result<Args, String> {
    let mut it = std::env::args().skip(1);
    let mut a = Args {
        data: PathBuf::from("data"),
        node: 1,
        listen: "127.0.0.1:9100".parse().expect("addr"),
        http: "127.0.0.1:9101".parse().expect("addr"),
        keep_slots: 60,
        ring: RingConfig::default(),
        urls: FeedUrls::default(),
        store: None,
        azure: None,
        azure_selftest: None,
        slot_ms: 60_000,
        timing: Timing::default(),
    };
    while let Some(arg) = it.next() {
        let mut val = || it.next().ok_or(format!("{arg} needs a value"));
        let bad = |what: &str| format!("bad {what}");
        match arg.as_str() {
            "--data" => a.data = PathBuf::from(val()?),
            "--node" => a.node = val()?.parse().map_err(|_| bad("--node"))?,
            "--listen" => a.listen = val()?.parse().map_err(|_| bad("--listen"))?,
            "--http" => a.http = val()?.parse().map_err(|_| bad("--http"))?,
            "--keep-slots" => a.keep_slots = val()?.parse().map_err(|_| bad("--keep-slots"))?,
            "--ring-frames" => {
                a.ring.max_frames = val()?.parse().map_err(|_| bad("--ring-frames"))?
            }
            "--ring-bytes" => a.ring.max_bytes = val()?.parse().map_err(|_| bad("--ring-bytes"))?,
            "--rtds-url" => a.urls.rtds = val()?,
            "--binance-url" => a.urls.binance = val()?,
            "--clob-url" => a.urls.clob = val()?,
            "--token" => a.urls.token = Some(val()?),
            "--store" => a.store = Some(PathBuf::from(val()?)),
            "--azure-store" => {
                // The identity to ask a token for, when the VM has several.
                let id = std::env::var("POLY_AZURE_CLIENT_ID")
                    .ok()
                    .filter(|s| !s.is_empty());
                let cfg = AzureConfig::new(&val()?, id);
                cfg.validate()?;
                a.azure = Some(cfg);
            }
            "--azure-selftest" => a.azure_selftest = Some(val()?),
            "--slot-ms" => a.slot_ms = val()?.parse().map_err(|_| bad("--slot-ms"))?,
            "--lease-timing" => {
                a.timing = parse_timing(&val()?).ok_or_else(|| bad("--lease-timing"))?
            }
            s => return Err(format!("unknown argument {s}")),
        }
    }
    // A leader that stalls and keeps the lease has stalled for less than a
    // takeover takes, and it files what arrived meanwhile in the slot it
    // wakes in. The overlap object covers a shift of one slot, so a slot
    // must last at least as long as a takeover.
    let takeover_ms = a.timing.ttl_ms + a.timing.takeover_margin_ms + 2 * a.timing.poll_ms;
    if (a.store.is_some() || a.azure.is_some()) && (a.slot_ms as u64) < takeover_ms {
        return Err(format!(
            "--slot-ms {} is shorter than a takeover ({takeover_ms} ms: ttl, takeover margin and two polls)",
            a.slot_ms
        ));
    }
    if !a.listen.ip().is_loopback() {
        return Err(format!(
            "--listen {} is not loopback; the stream is served on loopback only",
            a.listen
        ));
    }
    if a.store.is_some() && a.azure.is_some() {
        return Err("--store and --azure-store cannot both be given".into());
    }
    if a.azure_selftest.is_some() && a.azure.is_none() {
        return Err("--azure-selftest needs --azure-store".into());
    }
    if a.keep_slots < 1 || a.ring.max_frames < 1 || a.slot_ms < 1_000 {
        return Err(
            "--keep-slots and --ring-frames must be at least 1, --slot-ms at least 1000".into(),
        );
    }
    Ok(a)
}

/// A random boot id from the standard library's per-process random keys.
fn random_boot() -> Boot {
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    );
    h.write_u32(std::process::id());
    Boot(h.finish())
}

#[derive(Debug, Default)]
struct NodeStats {
    connections: AtomicU64,
    connections_total: AtomicU64,
    lagged: AtomicU64,
    bad_requests: AtomicU64,
}

fn lock(ring: &Mutex<Ring>) -> MutexGuard<'_, Ring> {
    ring.lock().unwrap_or_else(|e| e.into_inner())
}

/// The recorder thread's per-frame state.
struct Pipeline {
    seg: Segments,
    ring: Arc<Mutex<Ring>>,
    head: watch::Sender<u64>,
    keys: Vec<EventKey>,
    mutant: Mutant,
    venue_frames: u64,
}

fn on_frame(p: &mut Pipeline, rx_micros: i64, src: Source, text: String) -> io::Result<()> {
    // The stream stays up when the disk fails; `Segments` counts the error
    // and the frames it could not write.
    if let Err(e) = p.seg.append(Instant::now(), rx_micros, src, &text) {
        warn!(error = %e, "segment write failed");
    }
    p.keys.clear();
    let kind = frame_keys(src, &text, &mut p.keys);
    let frame: Arc<str> = text.into();
    let mut ring = lock(&p.ring);
    let mut head = ring.push(src, rx_micros, frame.clone(), kind, &p.keys);
    if head.is_some() {
        p.venue_frames += 1;
        if p.mutant == Mutant::DupFrame && p.venue_frames.is_multiple_of(500) {
            head = ring.push(src, rx_micros, frame, kind, &p.keys);
        }
    }
    drop(ring);
    if let Some(h) = head {
        p.head.send_replace(h);
    }
    Ok(())
}

struct Ctx {
    node: u32,
    ring: Arc<Mutex<Ring>>,
    head: watch::Receiver<u64>,
    shutdown: watch::Receiver<bool>,
    mutant: Mutant,
    stats: Arc<NodeStats>,
    seg: Arc<SegStats>,
    health: FeedHealth,
    recorder: Arc<feeds::recorder::RecorderStats>,
    backlog: Mutex<Backlog>,
    view: Arc<LeaseView>,
}

impl Ctx {
    /// Whether the recorder queue keeps up (see [`Backlog`]).
    fn recorder_healthy(&self) -> bool {
        let r = &self.recorder;
        self.backlog
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .healthy(
                Instant::now(),
                r.dropped.load(Relaxed),
                r.queue_depth(),
                r.capacity.load(Relaxed),
            )
    }
}

async fn accept_loop(listener: TcpListener, ctx: Arc<Ctx>) {
    loop {
        let tcp = match listener.accept().await {
            Ok((tcp, _)) => tcp,
            Err(e) => {
                warn!(error = %e, "accept failed");
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };
        let _ = tcp.set_nodelay(true);
        let ctx = ctx.clone();
        tokio::spawn(async move {
            ctx.stats.connections.fetch_add(1, Relaxed);
            ctx.stats.connections_total.fetch_add(1, Relaxed);
            if let Err(e) = serve_conn(tcp, &ctx).await {
                debug!(error = %e, "stream connection ended");
            }
            ctx.stats.connections.fetch_sub(1, Relaxed);
        });
    }
}

/// The first text message, or `None` if the client closes first.
async fn first_text<S>(rx: &mut S) -> Option<String>
where
    S: Stream<Item = Result<Message, tungstenite::Error>> + Unpin,
{
    while let Some(m) = rx.next().await {
        match m.ok()? {
            Message::Text(t) => return Some(t),
            Message::Close(_) => return None,
            _ => {}
        }
    }
    None
}

async fn send_bye<S>(tx: &mut S, reason: ByeReason)
where
    S: futures_util::Sink<Message, Error = tungstenite::Error> + Unpin,
{
    let bye = wire::encode(&NodeMsg::Bye { reason });
    let _ = tokio::time::timeout(Duration::from_secs(1), async {
        tx.send(Message::Text(bye)).await?;
        tx.close().await
    })
    .await;
}

/// `f` bounded by [`SEND_TIMEOUT`], except under the `unbounded-snapshot`
/// mutant, which waits as long as the client takes.
async fn bounded<T, E>(mutant: Mutant, f: impl Future<Output = Result<T, E>>) -> Result<T, BoxError>
where
    E: Into<BoxError>,
{
    if mutant == Mutant::UnboundedSnapshot {
        return f.await.map_err(Into::into);
    }
    match tokio::time::timeout(SEND_TIMEOUT, f).await {
        Ok(r) => r.map_err(Into::into),
        Err(_) => Err("client not reading".into()),
    }
}

async fn serve_conn(tcp: TcpStream, ctx: &Ctx) -> Result<(), BoxError> {
    let ws = bounded(ctx.mutant, tokio_tungstenite::accept_async(tcp)).await?;
    let (mut tx, mut rx) = ws.split();
    let first = tokio::time::timeout(Duration::from_secs(5), first_text(&mut rx)).await;
    let sub = match first {
        Ok(Some(t)) => wire::decode_sub(&t).ok(),
        _ => None,
    };
    let Some(sub) = sub else {
        ctx.stats.bad_requests.fetch_add(1, Relaxed);
        send_bye(&mut tx, ByeReason::BadRequest).await;
        return Ok(());
    };

    let (plan, oldest, head, boot) = {
        let r = lock(&ctx.ring);
        (r.plan(sub.resume), r.oldest(), r.head(), r.boot())
    };
    let (mut mode, mut next, mut complete, mut frames) = match plan {
        Plan::Resume { next } => (Mode::Resume, next, true, Vec::new()),
        Plan::Snapshot {
            frames,
            next,
            complete,
        } => (Mode::Snapshot, next, complete, frames),
    };
    if ctx.mutant == Mutant::StaleResume && sub.resume.is_some_and(|r| r.boot == boot) {
        (mode, next, complete) = (Mode::Resume, head + 1, true);
        frames.clear();
    }
    let hello = NodeMsg::Hello {
        v: wire::VERSION,
        node: ctx.node,
        boot,
        mode,
        next,
        oldest,
        head,
        complete,
    };
    // The hello, the snapshot frames and snap_end go out in chunks of a
    // live batch, each flushed within the send timeout.
    let snapshot = mode == Mode::Snapshot;
    let mut msgs = std::iter::once(wire::encode(&hello))
        .chain(
            frames
                .iter()
                .map(|e| wire::encode_ev(e.seq, e.src, e.rx_micros, true, &e.frame)),
        )
        .chain(snapshot.then(|| wire::encode(&NodeMsg::SnapEnd { next })));
    loop {
        let chunk: Vec<String> = msgs.by_ref().take(BATCH).collect();
        if chunk.is_empty() {
            break;
        }
        let send = async {
            for m in chunk {
                tx.feed(Message::Text(m)).await?;
            }
            tx.flush().await
        };
        bounded(ctx.mutant, send).await?;
    }
    drop(msgs);
    drop(frames);

    let mut head_rx = ctx.head.clone();
    let mut shutdown = ctx.shutdown.clone();
    let mut last_hb = Instant::now();
    loop {
        if *shutdown.borrow_and_update() {
            send_bye(&mut tx, ByeReason::Shutdown).await;
            return Ok(());
        }
        // Mark the head seen before reading, so a frame pushed after the
        // read wakes the wait below.
        head_rx.mark_unchanged();
        let batch = lock(&ctx.ring).read_from(next, BATCH);
        match batch {
            Err(_) => {
                ctx.stats.lagged.fetch_add(1, Relaxed);
                send_bye(&mut tx, ByeReason::Lagged).await;
                return Ok(());
            }
            Ok(batch) if !batch.is_empty() => {
                let send = async {
                    for e in &batch {
                        next = e.seq + 1;
                        if ctx.mutant == Mutant::SeqSkip && e.seq.is_multiple_of(1000) {
                            continue;
                        }
                        let ev = wire::encode_ev(e.seq, e.src, e.rx_micros, false, &e.frame);
                        tx.feed(Message::Text(ev)).await?;
                    }
                    tx.flush().await
                };
                tokio::time::timeout(SEND_TIMEOUT, send)
                    .await
                    .map_err(|_| "client not reading")??;
            }
            Ok(_) => {
                let hb_at = tokio::time::Instant::from_std(last_hb + HEARTBEAT);
                tokio::select! {
                    r = head_rx.changed() => {
                        if r.is_err() {
                            send_bye(&mut tx, ByeReason::Shutdown).await;
                            return Ok(());
                        }
                    }
                    _ = shutdown.changed() => {}
                    _ = tokio::time::sleep_until(hb_at) => {}
                    m = rx.next() => match m {
                        None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return Ok(()),
                        Some(Ok(_)) => {}
                    },
                }
            }
        }
        if last_hb.elapsed() >= HEARTBEAT {
            let hb = NodeMsg::Hb {
                head: *head_rx.borrow(),
                epoch: ctx.view.epoch.load(Relaxed),
                leader: ctx.view.leader(),
            };
            tokio::time::timeout(SEND_TIMEOUT, tx.send(Message::Text(wire::encode(&hb))))
                .await
                .map_err(|_| "client not reading")??;
            last_hb = Instant::now();
        }
    }
}

async fn healthz() -> &'static str {
    "ok\n"
}

async fn readyz(State(ctx): State<Arc<Ctx>>) -> impl IntoResponse {
    if ctx.health.all_connected() && ctx.recorder_healthy() && !*ctx.shutdown.borrow() {
        (StatusCode::OK, "ready\n")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n")
    }
}

async fn ha_status(State(ctx): State<Arc<Ctx>>) -> impl IntoResponse {
    let (boot, head, oldest) = {
        let r = lock(&ctx.ring);
        (r.boot(), r.head(), r.oldest())
    };
    let body = serde_json::json!({
        "node": ctx.node,
        "boot": boot.to_string(),
        "head": head,
        "oldest": oldest,
        "epoch": ctx.view.epoch.load(Relaxed),
        "leader": ctx.view.leader(),
    });
    ([("content-type", "application/json")], body.to_string())
}

async fn metrics(State(ctx): State<Arc<Ctx>>) -> impl IntoResponse {
    use std::fmt::Write as _;
    let (head, oldest, frames, bytes) = {
        let r = lock(&ctx.ring);
        (r.head(), r.oldest(), r.len(), r.bytes())
    };
    let s = &ctx.stats;
    let mut out = String::new();
    let mut line = |name: &str, v: u64| {
        let _ = writeln!(out, "{name} {v}");
    };
    line("poly_node_head_seq", head);
    line("poly_node_oldest_seq", oldest);
    line("poly_node_ring_frames", frames as u64);
    line("poly_node_ring_bytes", bytes as u64);
    line("poly_node_connections", s.connections.load(Relaxed));
    line(
        "poly_node_connections_total",
        s.connections_total.load(Relaxed),
    );
    line("poly_node_lagged_total", s.lagged.load(Relaxed));
    line("poly_node_bad_requests_total", s.bad_requests.load(Relaxed));
    let g = &ctx.seg;
    line("poly_node_segment_errors_total", g.errors.load(Relaxed));
    line("poly_node_segment_lost_total", g.lost.load(Relaxed));
    line("poly_node_segments_closed_total", g.closed.load(Relaxed));
    line(
        "poly_recorder_dropped_total",
        ctx.recorder.dropped.load(Relaxed),
    );
    line("poly_recorder_queue_depth", ctx.recorder.queue_depth());
    let v = &ctx.view;
    line("poly_lease_epoch", v.epoch.load(Relaxed));
    line("poly_lease_leader", v.leader() as u64);
    line("poly_fence_rejected_total", v.fence_rejected.load(Relaxed));
    line(
        "poly_lease_unknown_outcomes_total",
        v.unknown_outcomes.load(Relaxed),
    );
    line("poly_archive_commits_total", v.commits.load(Relaxed));
    line("poly_archive_uploads_total", v.uploads.load(Relaxed));
    line(
        "poly_archive_upload_errors_total",
        v.upload_errors.load(Relaxed),
    );
    line("poly_archive_holes_total", v.holes.load(Relaxed));
    line("poly_archive_partial_total", v.partials.load(Relaxed));
    line("poly_archive_overlaps_total", v.overlaps.load(Relaxed));
    for m in ctx.health.all() {
        let snap = m.snapshot();
        let _ = writeln!(
            out,
            "poly_feed_connected{{feed=\"{}\"}} {}",
            snap.name, snap.connected as u8
        );
    }
    ([("content-type", "text/plain; version=0.0.4")], out)
}

fn ensure_dir(p: &Path) -> io::Result<()> {
    std::fs::create_dir_all(p)
}

/// Everything an archiver needs but its store.
struct ArchiveParts {
    lease: CasLease,
    data: PathBuf,
    node: u32,
    slot_micros: i64,
    view: Arc<LeaseView>,
    stop: Arc<AtomicBool>,
}

fn build_archiver<S: HeadStore + ObjectStore>(store: S, p: ArchiveParts) -> Archiver<S> {
    Archiver {
        lease: p.lease,
        store,
        seg_dir: p.data.join("seg"),
        tmp_dir: p.data.join("archive-tmp"),
        log: p.data.join("lease.jsonl"),
        node: p.node,
        slot_micros: p.slot_micros,
        lookback: 60,
        view: p.view,
        stop: p.stop,
    }
}

/// Run the store contract suite against the deployed account, with its
/// HEAD and its objects under `selftest/<id>/` in the archive container, so
/// the real HEAD is never touched. Returns the process exit code.
fn azure_selftest(mut cfg: AzureConfig, id: &str, data: &Path) -> i32 {
    let ok = !id.is_empty()
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    if !ok {
        eprintln!("node: --azure-selftest takes letters, digits, - and _");
        return 2;
    }
    let prefix = format!("selftest/{id}/");
    cfg.head_container = cfg.object_container.clone();
    cfg.head_blob = format!("{prefix}HEAD.json");
    let scratch = data.join("selftest-tmp");
    // The store makes blocking calls, so it lives on a plain thread.
    let run = std::thread::spawn(move || {
        let store = AzureStore::new(cfg)?;
        let out = ha::store::contract_suite(&store, &scratch, &prefix);
        let _ = std::fs::remove_dir_all(&scratch);
        out
    });
    match run.join() {
        Ok(Ok(())) => {
            println!("azure selftest ok: conditional writes hold under selftest/{id}/");
            0
        }
        Ok(Err(e)) => {
            eprintln!("azure selftest FAILED: {e}");
            1
        }
        Err(_) => {
            eprintln!("azure selftest FAILED: the store thread panicked");
            1
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,hyper=warn,reqwest=warn".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let mutant = match mutant_from_env() {
        Ok(m) => m,
        Err(e) => {
            eprintln!("node: {e}");
            std::process::exit(2);
        }
    };
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("node: {e}");
            std::process::exit(2);
        }
    };
    if let (Some(cfg), Some(id)) = (&args.azure, &args.azure_selftest) {
        std::process::exit(azure_selftest(cfg.clone(), id, &args.data));
    }
    if mutant != Mutant::None {
        warn!(?mutant, "running with a seeded fault");
    }
    let boot = random_boot();
    let seg_dir = args.data.join("seg");
    ensure_dir(&seg_dir)?;

    let stats = Arc::new(NodeStats::default());
    let view = Arc::new(LeaseView::default());
    let slot_micros = args.slot_ms * 1_000;
    let ring = Arc::new(Mutex::new(Ring::new(boot, args.ring)));
    let (head_tx, head_rx) = watch::channel(0u64);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let health = FeedHealth::default();

    let (rec, q) = queue(65_536);
    let seg = Segments::new(
        seg_dir,
        boot,
        args.keep_slots,
        slot_micros,
        view.clone(),
        segments::create_new(),
    );
    let seg_stats = seg.stats.clone();
    let pipeline = Pipeline {
        seg,
        ring: ring.clone(),
        head: head_tx,
        keys: Vec::with_capacity(16),
        mutant,
        venue_frames: 0,
    };
    let task = q.start_with(pipeline, on_frame, |mut p| {
        p.seg.close()?;
        Ok((p.seg.records, p.seg.bytes))
    });

    let ctx = Arc::new(Ctx {
        node: args.node,
        ring,
        head: head_rx,
        shutdown: shutdown_rx,
        mutant,
        stats: stats.clone(),
        seg: seg_stats,
        health: health.clone(),
        recorder: rec.stats.clone(),
        backlog: Mutex::new(Backlog::default()),
        view: view.clone(),
    });

    let stop_archiver = Arc::new(AtomicBool::new(false));
    let archiver = if args.store.is_some() || args.azure.is_some() {
        let mut lcfg =
            caslease::Config::new(args.node, random_boot().0 ^ boot.0).with_timing(args.timing);
        // Tenures older than three days are dropped.
        lcfg.keep_slots = (3 * 24 * 3_600_000 / args.slot_ms).max(1);
        let lease_mutant = match mutant {
            Mutant::Lease(m) => m,
            _ => LeaseMutant::None,
        };
        #[cfg(feature = "mutants")]
        let lcfg = lcfg.with_mutant(lease_mutant);
        #[cfg(not(feature = "mutants"))]
        let _ = lease_mutant;
        let parts = ArchiveParts {
            lease: CasLease::new(lcfg),
            data: args.data.clone(),
            node: args.node,
            slot_micros,
            view: view.clone(),
            stop: stop_archiver.clone(),
        };
        match (&args.store, args.azure.clone()) {
            (Some(dir), _) => {
                let a = build_archiver(FsStore::open(dir)?, parts);
                Some(std::thread::spawn(move || a.run()))
            }
            // The Blob store makes blocking calls, so it is built on the
            // archiver's own thread, never on the runtime.
            (None, Some(cfg)) => Some(std::thread::spawn(move || match AzureStore::new(cfg) {
                Ok(store) => build_archiver(store, parts).run(),
                Err(e) => {
                    eprintln!("node: {e}");
                    std::process::exit(2);
                }
            })),
            (None, None) => None,
        }
    } else {
        None
    };

    let stream = TcpListener::bind(args.listen).await?;
    let http = TcpListener::bind(args.http).await?;
    let stream_addr = stream.local_addr()?;
    let http_addr = http.local_addr()?;
    let router = Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .route("/ha", get(ha_status))
        .with_state(ctx.clone());
    let accept = tokio::spawn(accept_loop(stream, ctx.clone()));
    let mut http_shutdown = ctx.shutdown.clone();
    let http_task = tokio::spawn(async move {
        let _ = axum::serve(http, router)
            .with_graceful_shutdown(async move {
                let _ = http_shutdown.wait_for(|s| *s).await;
            })
            .await;
    });
    let feeds = spawn_feeds(&rec, &args.urls, Some(&health));
    println!(
        "node {} boot {boot} stream {stream_addr} http {http_addr}",
        args.node
    );
    info!(node = args.node, %boot, %stream_addr, %http_addr, "node up");

    let reason = shutdown_signal().await;
    info!(
        reason,
        "stopping: closing clients, cancelling feeds, draining"
    );
    shutdown_tx.send_replace(true);
    accept.abort();
    for t in &feeds {
        t.abort();
    }
    for t in feeds {
        let _ = t.await;
    }
    let dropped = rec.stats.dropped.load(Relaxed);
    drop(rec);
    let (records, bytes) = tokio::task::spawn_blocking(move || task.finish()).await??;
    stop_archiver.store(true, Relaxed);
    if let Some(a) = archiver {
        let _ = tokio::task::spawn_blocking(move || a.join()).await;
    }
    // Give connections a moment to deliver their bye.
    let deadline = Instant::now() + Duration::from_secs(2);
    while stats.connections.load(Relaxed) > 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let _ = http_task.await;
    println!(
        "node stopped: recorded {records} frames, {bytes} bytes, dropped {dropped} ({reason})"
    );
    Ok(())
}
