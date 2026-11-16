//! HA chaos harness: run two nodes against the venue simulator, kill and
//! freeze them under a hedged client, and check what the client delivered
//! against total truth.
//!
//! ```text
//! chaos local  --run DIR [--seed 1] [--rate 200] [--faults 12] [--gap-ms 4000]
//!              [--freeze-ms 2000] [--margin-s 15] [--recording FILE] [--lag-ms 0]
//!              [--mutant NAME] [--bins DIR]
//! chaos plan   [--seed 1] [--faults 12] [--gap-ms 4000] [--freeze-ms 2000]
//!              [--margin-s 15] [--mutant NAME]
//! chaos client --nodes URL,URL --seconds N --run DIR [--mutant NAME]
//! chaos verify --run DIR --truth FILE
//! ```
//!
//! `local` does the whole run on this machine: it starts `venue_sim` with a
//! manifest, two `node`s on fixed loopback ports, and the client in this
//! process; waits until both subscriptions are live; runs the plan; and
//! writes the run directory described in `app::chaos`, then the verdict.
//! Before each fault the other node's `/readyz` must have answered 200 for
//! 500 ms, and the harness waits up to 10 s for the client to be live on
//! it (a client that never rehedges still meets the fault). `kill9` is
//! SIGKILL and a respawn 1 s later; `freeze` is SIGSTOP then SIGCONT; `cut`
//! drops the client's connection in a loopback relay that sits between the
//! client and each node, while the node keeps running.
//!
//! Exit 0 pass, 1 fail (the reason is printed and in `report.json`),
//! 2 invalid: a node exited on its own, a peer never became ready, or the
//! subscriptions never all went live.
//!
//! `--mutant` picks one seeded fault and works only in a build with the
//! `mutants` feature: `no-dedup`, `single-node`, `no-reconnect` (client),
//! `seq-skip`, `stale-resume`, `dup-frame` (node, through `POLY_MUTANT`),
//! `overlap-kill` (harness).

use app::chaos::{self, ClientRecord, ConnObs, Executed, FaultKind, Harness, LogLine, Plan};
use ha::client::{self, ClientConfig, ClientMutant, Output};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{oneshot, watch};

struct Opts(HashMap<String, String>);

impl Opts {
    fn parse(args: &[String]) -> Result<Opts, String> {
        let mut m = HashMap::new();
        let mut it = args.iter();
        while let Some(a) = it.next() {
            let Some(k) = a.strip_prefix("--") else {
                return Err(format!("unexpected argument {a}"));
            };
            let v = it.next().ok_or(format!("{a} needs a value"))?;
            m.insert(k.to_string(), v.clone());
        }
        Ok(Opts(m))
    }

    fn str(&self, k: &str) -> Option<&str> {
        self.0.get(k).map(String::as_str)
    }

    fn need(&self, k: &str) -> Result<&str, String> {
        self.str(k).ok_or(format!("--{k} is required"))
    }

    fn num<T: std::str::FromStr>(&self, k: &str, default: T) -> Result<T, String> {
        match self.str(k) {
            None => Ok(default),
            Some(v) => v.parse().map_err(|_| format!("bad --{k} {v}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mutant {
    None,
    Client(ClientMutant),
    Node(&'static str),
    Harness,
}

fn parse_mutant(name: Option<&str>) -> Result<Mutant, String> {
    let Some(name) = name else {
        return Ok(Mutant::None);
    };
    if !cfg!(feature = "mutants") {
        return Err(format!(
            "--mutant {name} needs a build with the mutants feature; refusing to start"
        ));
    }
    Ok(match name {
        "no-dedup" => Mutant::Client(ClientMutant::NoDedup),
        "single-node" => Mutant::Client(ClientMutant::SingleNode),
        "no-reconnect" => Mutant::Client(ClientMutant::NoReconnect),
        "seq-skip" => Mutant::Node("seq-skip"),
        "stale-resume" => Mutant::Node("stale-resume"),
        "dup-frame" => Mutant::Node("dup-frame"),
        chaos::OVERLAP_KILL => Mutant::Harness,
        _ => return Err(format!("unknown mutant {name}")),
    })
}

fn client_config(m: Mutant) -> ClientConfig {
    let cfg = ClientConfig::new(2);
    #[cfg(feature = "mutants")]
    if let Mutant::Client(c) = m {
        return cfg.with_mutant(c);
    }
    let _ = m;
    cfg
}

fn wanted(m: Mutant) -> Vec<bool> {
    match m {
        Mutant::Client(ClientMutant::SingleNode) => vec![true, false],
        _ => vec![true, true],
    }
}

fn wall_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn wall_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

fn mono_ms(epoch: Instant, t: Instant) -> i64 {
    t.saturating_duration_since(epoch).as_millis() as i64
}

/// What the client saw, built from its outputs.
struct Rec {
    epoch: Instant,
    wanted: Vec<bool>,
    live: Vec<bool>,
    conn: Vec<ConnObs>,
    log: Vec<LogLine>,
    live_wall_ms: Option<i64>,
    live_mono_ms: Option<i64>,
    tx: watch::Sender<Vec<bool>>,
}

impl Rec {
    fn on(&mut self, o: Output) {
        let now = Instant::now();
        let m = mono_ms(self.epoch, now);
        match o {
            Output::Event(e) => chaos::log_lines(&e, wall_us(), m, &mut self.log),
            Output::Live { node } => {
                self.live[node] = true;
                self.conn.push(ConnObs {
                    node,
                    live: true,
                    wall_ms: wall_ms(),
                    mono_ms: m,
                    last_msg_mono_ms: None,
                    reason: "live".into(),
                });
                let all = self.live.iter().zip(&self.wanted).all(|(l, w)| *l || !*w);
                if all && self.live_wall_ms.is_none() {
                    self.live_wall_ms = Some(wall_ms());
                    self.live_mono_ms = Some(m);
                }
                self.tx.send_replace(self.live.clone());
            }
            Output::Down {
                node,
                reason,
                last_msg,
            } => {
                self.live[node] = false;
                self.conn.push(ConnObs {
                    node,
                    live: false,
                    wall_ms: wall_ms(),
                    mono_ms: m,
                    last_msg_mono_ms: last_msg.map(|t| mono_ms(self.epoch, t)),
                    reason: format!("{reason:?}"),
                });
                self.tx.send_replace(self.live.clone());
            }
        }
    }
}

async fn run_client(
    urls: Vec<String>,
    mutant: Mutant,
    epoch: Instant,
    stop: oneshot::Receiver<()>,
    tx: watch::Sender<Vec<bool>>,
) -> (ClientRecord, Vec<LogLine>) {
    let wanted = wanted(mutant);
    let mut rec = Rec {
        epoch,
        live: vec![false; wanted.len()],
        wanted,
        conn: Vec::new(),
        log: Vec::new(),
        live_wall_ms: None,
        live_mono_ms: None,
        tx,
    };
    let core = client::run(
        urls,
        client_config(mutant),
        async {
            let _ = stop.await;
        },
        |o| rec.on(o),
    )
    .await;
    let record = ClientRecord {
        wanted: rec.wanted,
        live_wall_ms: rec.live_wall_ms,
        live_mono_ms: rec.live_mono_ms,
        conn: rec.conn,
        counters: core.counters().into(),
        first_from: core.merger().first_from().to_vec(),
        merger: core.merger().totals().into(),
    };
    (record, rec.log)
}

fn write_client(run: &Path, rec: &ClientRecord, log: &[LogLine]) -> std::io::Result<()> {
    chaos::write_json(&run.join(chaos::CLIENT), rec)?;
    chaos::write_jsonl(&run.join(chaos::LOG), log)
}

fn verify_and_print(run: &Path, truth: &Path) -> i32 {
    let inputs = match chaos::load(run, truth) {
        Ok(i) => i,
        Err(e) => {
            eprintln!("chaos verify: {e}");
            return 2;
        }
    };
    let r = chaos::verify(&inputs);
    if let Err(e) = chaos::write_json(&run.join(chaos::REPORT), &r) {
        eprintln!("chaos verify: writing the report: {e}");
        return 2;
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&r).expect("report serializes")
    );
    println!(
        "chaos: {} reason={} missing={} duplicates={} extra={} late_lost={} faults={}/{} observed={} self_repeats={} merger_duplicates={}",
        r.verdict.result,
        r.verdict.reason.as_deref().unwrap_or("-"),
        r.missing,
        r.duplicates,
        r.extra,
        r.late.lost,
        r.faults.executed,
        r.faults.planned,
        r.faults.observed,
        r.counters.self_repeats,
        r.merger.duplicates,
    );
    r.exit_code()
}

fn plan_from(o: &Opts) -> Result<Plan, String> {
    Ok(chaos::plan(
        o.num("seed", 1u64)?,
        o.num("faults", 12usize)?,
        o.num("gap-ms", 4_000i64)?,
        o.num("freeze-ms", 2_000u64)?,
        o.num("margin-s", 15i64)? * 1_000,
        o.str("mutant"),
    ))
}

fn cmd_plan(o: &Opts) -> Result<i32, String> {
    parse_mutant(o.str("mutant"))?;
    let p = plan_from(o)?;
    println!("{}", serde_json::to_string_pretty(&p).expect("plan"));
    Ok(0)
}

async fn cmd_client(o: &Opts) -> Result<i32, String> {
    let mutant = parse_mutant(o.str("mutant"))?;
    let urls: Vec<String> = o.need("nodes")?.split(',').map(str::to_string).collect();
    if urls.len() != 2 {
        return Err("--nodes takes two urls".into());
    }
    let seconds: u64 = o.num("seconds", 60)?;
    let run = PathBuf::from(o.need("run")?);
    std::fs::create_dir_all(&run).map_err(|e| e.to_string())?;
    let (stop_tx, stop_rx) = oneshot::channel();
    let (tx, _rx) = watch::channel(vec![false; 2]);
    let task = tokio::spawn(run_client(urls, mutant, Instant::now(), stop_rx, tx));
    tokio::time::sleep(Duration::from_secs(seconds)).await;
    let _ = stop_tx.send(());
    let (rec, log) = task.await.map_err(|e| e.to_string())?;
    write_client(&run, &rec, &log).map_err(|e| e.to_string())?;
    println!(
        "chaos client: {} log lines, counters {:?}",
        log.len(),
        rec.counters
    );
    Ok(0)
}

fn cmd_verify(o: &Opts) -> Result<i32, String> {
    Ok(verify_and_print(
        Path::new(o.need("run")?),
        Path::new(o.need("truth")?),
    ))
}

fn free_port() -> std::io::Result<u16> {
    Ok(std::net::TcpListener::bind("127.0.0.1:0")?
        .local_addr()?
        .port())
}

/// Read the first stdout line, then keep draining so a later print never
/// meets a closed pipe.
fn first_line(child: &mut Child) -> std::io::Result<String> {
    let out = child
        .stdout
        .take()
        .ok_or_else(|| std::io::Error::other("no stdout"))?;
    let mut r = BufReader::new(out);
    let mut line = String::new();
    if r.read_line(&mut line)? == 0 {
        return Err(std::io::Error::other("exited before its start line"));
    }
    std::thread::spawn(move || {
        let mut sink = String::new();
        while r.read_line(&mut sink).unwrap_or(0) > 0 {
            sink.clear();
        }
    });
    Ok(line)
}

fn append_log(path: &Path) -> std::io::Result<File> {
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
}

fn signal(pid: u32, sig: &str) -> Result<(), String> {
    let ok = Command::new("kill")
        .args([format!("-{sig}"), pid.to_string()])
        .status()
        .map_err(|e| e.to_string())?
        .success();
    if ok {
        Ok(())
    } else {
        Err(format!("kill -{sig} {pid} failed"))
    }
}

/// Stop with SIGTERM and wait up to 15 s, then SIGKILL.
fn stop_child(child: &mut Child) {
    if child.try_wait().ok().flatten().is_some() {
        return;
    }
    let _ = signal(child.id(), "TERM");
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(15) {
        if child.try_wait().ok().flatten().is_some() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let _ = child.kill();
    let _ = child.wait();
}

struct NodeProc {
    proxy: Arc<Proxy>,
    bin: PathBuf,
    args: Vec<String>,
    mutant: Option<&'static str>,
    http: SocketAddr,
    log: PathBuf,
    child: Option<Child>,
}

impl NodeProc {
    fn start(&mut self) -> Result<(), String> {
        let mut cmd = Command::new(&self.bin);
        cmd.args(&self.args)
            .env("RUST_LOG", "warn")
            .env_remove("POLY_MUTANT")
            .stdout(Stdio::piped())
            .stderr(append_log(&self.log).map_err(|e| e.to_string())?);
        if let Some(m) = self.mutant {
            cmd.env("POLY_MUTANT", m);
        }
        let mut child = cmd.spawn().map_err(|e| format!("{:?}: {e}", self.bin))?;
        let line = first_line(&mut child).map_err(|e| format!("node: {e}"))?;
        if !line.starts_with("node ") {
            return Err(format!("node start line {line:?}"));
        }
        self.child = Some(child);
        Ok(())
    }

    /// True if the process is gone without the harness stopping it.
    fn exited(&mut self) -> bool {
        match &mut self.child {
            Some(c) => c.try_wait().ok().flatten().is_some(),
            None => true,
        }
    }

    fn pid(&self) -> Result<u32, String> {
        self.child
            .as_ref()
            .map(Child::id)
            .ok_or("node not running".into())
    }

    fn kill9(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    fn stop(&mut self) {
        if let Some(c) = &mut self.child {
            stop_child(c);
        }
        self.child = None;
    }
}

impl Drop for NodeProc {
    fn drop(&mut self) {
        self.kill9();
    }
}

async fn http_get(addr: SocketAddr, path: &str) -> Option<u16> {
    let io = async {
        let mut s = tokio::net::TcpStream::connect(addr).await.ok()?;
        let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        s.write_all(req.as_bytes()).await.ok()?;
        let mut buf = String::new();
        s.read_to_string(&mut buf).await.ok()?;
        buf.split_whitespace().nth(1)?.parse().ok()
    };
    tokio::time::timeout(Duration::from_secs(1), io)
        .await
        .ok()
        .flatten()
}

/// Wait until `/readyz` is 200, up to `limit`.
async fn wait_ready(http: SocketAddr, limit: Duration) -> bool {
    let started = Instant::now();
    while started.elapsed() < limit {
        if http_get(http, "/readyz").await == Some(200) {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    false
}

/// Wait until `/readyz` has answered 200 continuously for `hold`.
async fn ready_for(http: SocketAddr, hold: Duration, limit: Duration) -> bool {
    let started = Instant::now();
    let mut since: Option<Instant> = None;
    while started.elapsed() < limit {
        if http_get(http, "/readyz").await == Some(200) {
            let s = *since.get_or_insert_with(Instant::now);
            if s.elapsed() >= hold {
                return true;
            }
        } else {
            since = None;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

type Shared = Arc<tokio::sync::Mutex<NodeProc>>;

/// A loopback TCP relay between the client and one node, so the harness
/// can cut the path between them while the node keeps running.
#[derive(Default)]
struct Proxy {
    conns: std::sync::Mutex<Vec<tokio::task::AbortHandle>>,
}

impl Proxy {
    async fn start(target: SocketAddr) -> std::io::Result<(Arc<Proxy>, u16)> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let proxy = Arc::new(Proxy::default());
        let p = proxy.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut down, _)) = listener.accept().await else {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    continue;
                };
                let _ = down.set_nodelay(true);
                let relay = tokio::spawn(async move {
                    if let Ok(mut up) = tokio::net::TcpStream::connect(target).await {
                        let _ = up.set_nodelay(true);
                        let _ = tokio::io::copy_bidirectional(&mut down, &mut up).await;
                    }
                });
                let mut conns = p.conns.lock().unwrap_or_else(|e| e.into_inner());
                conns.retain(|h| !h.is_finished());
                conns.push(relay.abort_handle());
            }
        });
        Ok((proxy, port))
    }

    /// Drop every relayed connection. Returns how many there were.
    fn cut(&self) -> usize {
        let conns = std::mem::take(&mut *self.conns.lock().unwrap_or_else(|e| e.into_inner()));
        for h in &conns {
            h.abort();
        }
        conns.len()
    }
}

async fn execute(
    node: Shared,
    idx: usize,
    kind: FaultKind,
    freeze_ms: u64,
    epoch: Instant,
    mut live: watch::Receiver<Vec<bool>>,
) -> Result<Executed, String> {
    let mut n = node.lock().await;
    let at_wall_ms = wall_ms();
    let at_mono_ms = mono_ms(epoch, Instant::now());
    if kind == FaultKind::Cut {
        n.proxy.cut();
        drop(n);
        // Recovered once the client is live on the node again, or after
        // 10 s for a client that does not come back.
        let _ = tokio::time::timeout(Duration::from_secs(2), live.wait_for(|l| !l[idx])).await;
        let _ = tokio::time::timeout(Duration::from_secs(10), live.wait_for(|l| l[idx])).await;
        return Ok(Executed {
            node: idx,
            kind,
            at_wall_ms,
            at_mono_ms,
            recovered_wall_ms: wall_ms(),
            recovered_mono_ms: mono_ms(epoch, Instant::now()),
        });
    }
    match kind {
        FaultKind::Kill9 => {
            n.kill9();
            tokio::time::sleep(Duration::from_secs(1)).await;
            n.start()?;
        }
        FaultKind::Freeze => {
            let pid = n.pid()?;
            signal(pid, "STOP")?;
            tokio::time::sleep(Duration::from_millis(freeze_ms)).await;
            signal(pid, "CONT")?;
        }
        FaultKind::Cut => {}
    }
    let http = n.http;
    drop(n);
    if !wait_ready(http, Duration::from_secs(20)).await {
        return Err(format!("node {idx} not ready 20 s after its fault"));
    }
    Ok(Executed {
        node: idx,
        kind,
        at_wall_ms,
        at_mono_ms,
        recovered_wall_ms: wall_ms(),
        recovered_mono_ms: mono_ms(epoch, Instant::now()),
    })
}

async fn check_alive(nodes: &[Shared]) -> Result<(), String> {
    for (i, n) in nodes.iter().enumerate() {
        if n.lock().await.exited() {
            return Err(format!("node {i} exited on its own"));
        }
    }
    Ok(())
}

/// Run the plan. Executed faults are pushed as they finish.
async fn drive(
    plan: &Plan,
    nodes: &[Shared],
    mut live: watch::Receiver<Vec<bool>>,
    wanted: &[bool],
    epoch: Instant,
    executed: &mut Vec<Executed>,
) -> Result<(), String> {
    let all_live = |l: &Vec<bool>| l.iter().zip(wanted).all(|(l, w)| *l || !*w);
    tokio::time::timeout(Duration::from_secs(30), live.wait_for(all_live))
        .await
        .map_err(|_| "subscriptions never all live".to_string())?
        .map_err(|_| "client stopped".to_string())?;
    let margin = Duration::from_millis(plan.margin_ms as u64);
    let t0 = tokio::time::Instant::now() + margin;
    let due = |at_ms: i64| t0 + Duration::from_millis(at_ms.max(0) as u64);
    if plan.overlap {
        let mut tasks = Vec::new();
        for f in &plan.faults {
            tokio::time::sleep_until(due(f.at_ms)).await;
            tasks.push(tokio::spawn(execute(
                nodes[f.node].clone(),
                f.node,
                f.kind,
                f.freeze_ms,
                epoch,
                live.clone(),
            )));
        }
        for t in tasks {
            executed.push(t.await.map_err(|e| e.to_string())??);
        }
    } else {
        for f in &plan.faults {
            tokio::time::sleep_until(due(f.at_ms)).await;
            check_alive(nodes).await?;
            let other = 1 - f.node;
            let http = nodes[other].lock().await.http;
            if !ready_for(http, Duration::from_millis(500), Duration::from_secs(20)).await {
                return Err(format!(
                    "node {other} not ready before a fault on node {}",
                    f.node
                ));
            }
            if wanted[other] {
                let _ = tokio::time::timeout(Duration::from_secs(10), live.wait_for(|l| l[other]))
                    .await;
            }
            let node = nodes[f.node].clone();
            let done = execute(node, f.node, f.kind, f.freeze_ms, epoch, live.clone()).await?;
            executed.push(done);
        }
    }
    let last = executed
        .iter()
        .map(|e| epoch + Duration::from_millis(e.recovered_mono_ms as u64))
        .max()
        .map(tokio::time::Instant::from_std)
        .unwrap_or(t0);
    // T1 is a margin after the last recovery; the client runs a margin
    // past T1.
    tokio::time::sleep_until(last.max(t0) + 2 * margin).await;
    check_alive(nodes).await
}

fn recording_token(path: &Path) -> Result<String, String> {
    let (recs, _) = feeds::recording::read_all(path).map_err(|e| format!("{path:?}: {e:?}"))?;
    recs.iter()
        .filter(|r| r.source == feeds::recording::Source::Control)
        .find_map(|r| {
            serde_json::from_str::<serde_json::Value>(&r.frame)
                .ok()?
                .get("subscribe")?
                .as_str()
                .map(str::to_string)
        })
        .ok_or("the recording has no subscribe record".into())
}

struct Sim {
    child: Child,
    urls: Vec<String>,
}

impl Drop for Sim {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_sim(bin: &Path, args: &[String], log: &Path) -> Result<Sim, String> {
    let mut child = Command::new(bin)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(append_log(log).map_err(|e| e.to_string())?)
        .spawn()
        .map_err(|e| format!("{bin:?}: {e}"))?;
    let line = first_line(&mut child).map_err(|e| format!("venue_sim: {e}"))?;
    // venue_sim rtds <url> binance <url> clob <url>
    let w: Vec<&str> = line.split_whitespace().collect();
    if w.len() != 7 || (w[0], w[1], w[3], w[5]) != ("venue_sim", "rtds", "binance", "clob") {
        let _ = child.kill();
        return Err(format!("venue_sim start line {line:?}"));
    }
    Ok(Sim {
        urls: vec![w[2].into(), w[4].into(), w[6].into()],
        child,
    })
}

async fn cmd_local(o: &Opts) -> Result<i32, String> {
    let mutant = parse_mutant(o.str("mutant"))?;
    let run = PathBuf::from(o.need("run")?);
    std::fs::create_dir_all(&run).map_err(|e| e.to_string())?;
    let plan = plan_from(o)?;
    chaos::write_json(&run.join(chaos::PLAN), &plan).map_err(|e| e.to_string())?;
    let bins = match o.str("bins") {
        Some(b) => PathBuf::from(b),
        None => std::env::current_exe()
            .map_err(|e| e.to_string())?
            .parent()
            .ok_or("no bin dir")?
            .to_path_buf(),
    };
    let recording = o.str("recording").map(PathBuf::from);
    let token = match &recording {
        Some(p) => recording_token(p)?,
        None => app::sim::ASSETS[0].to_string(),
    };
    let truth = run.join("truth.prec");

    let mut sim_args = vec![
        "--seed".to_string(),
        plan.seed.to_string(),
        "--rate".into(),
        o.num("rate", 200.0f64)?.to_string(),
        "--manifest".into(),
        truth.to_string_lossy().into_owned(),
        "--lag-ms".into(),
        o.num("lag-ms", 0u64)?.to_string(),
        "--start-after".into(),
        "2".into(),
    ];
    if let Some(r) = &recording {
        sim_args.extend(["--recording".into(), r.to_string_lossy().into_owned()]);
    }
    let mut sim = spawn_sim(&bins.join("venue_sim"), &sim_args, &run.join("sim.log"))?;

    let node_mutant = match mutant {
        Mutant::Node(m) => Some(m),
        _ => None,
    };
    let mut nodes: Vec<Shared> = Vec::new();
    let mut urls = Vec::new();
    for (i, name) in ["a", "b"].iter().enumerate() {
        let stream = free_port().map_err(|e| e.to_string())?;
        let http = free_port().map_err(|e| e.to_string())?;
        let data = run.join(format!("node-{name}-data"));
        let args = vec![
            "--data".to_string(),
            data.to_string_lossy().into_owned(),
            "--node".into(),
            (i + 1).to_string(),
            "--listen".into(),
            format!("127.0.0.1:{stream}"),
            "--http".into(),
            format!("127.0.0.1:{http}"),
            "--rtds-url".into(),
            sim.urls[0].clone(),
            "--binance-url".into(),
            sim.urls[1].clone(),
            "--clob-url".into(),
            sim.urls[2].clone(),
            "--token".into(),
            token.clone(),
        ];
        let target: SocketAddr = format!("127.0.0.1:{stream}").parse().expect("addr");
        let (proxy, relay) = Proxy::start(target).await.map_err(|e| e.to_string())?;
        let mut n = NodeProc {
            proxy,
            bin: bins.join("node"),
            args,
            mutant: node_mutant,
            http: format!("127.0.0.1:{http}").parse().expect("addr"),
            log: run.join(format!("node-{name}.log")),
            child: None,
        };
        n.start()?;
        // The client reaches each node through its relay.
        urls.push(format!("ws://127.0.0.1:{relay}"));
        nodes.push(Arc::new(tokio::sync::Mutex::new(n)));
    }

    let epoch = Instant::now();
    let (stop_tx, stop_rx) = oneshot::channel();
    let (live_tx, live_rx) = watch::channel(vec![false; 2]);
    let client = tokio::spawn(run_client(urls, mutant, epoch, stop_rx, live_tx));
    let want = wanted(mutant);
    let mut executed = Vec::new();
    let outcome = drive(&plan, &nodes, live_rx, &want, epoch, &mut executed).await;
    if let Err(e) = &outcome {
        eprintln!("chaos local: invalid run: {e}");
    }

    let _ = stop_tx.send(());
    let (rec, log) = client.await.map_err(|e| e.to_string())?;
    for n in &nodes {
        n.lock().await.stop();
    }
    stop_child(&mut sim.child);
    chaos::write_jsonl(&run.join(chaos::FAULTS), &executed).map_err(|e| e.to_string())?;
    let harness = Harness {
        invalid: outcome.err(),
    };
    chaos::write_json(&run.join(chaos::HARNESS), &harness).map_err(|e| e.to_string())?;
    write_client(&run, &rec, &log).map_err(|e| e.to_string())?;
    Ok(verify_and_print(&run, &truth))
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (cmd, rest) = match args.split_first() {
        Some((c, r)) => (c.as_str(), r),
        None => ("", &[][..]),
    };
    let opts = match Opts::parse(rest) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("chaos: {e}");
            std::process::exit(2);
        }
    };
    let res = match cmd {
        "plan" => cmd_plan(&opts),
        "client" => cmd_client(&opts).await,
        "verify" => cmd_verify(&opts),
        "local" => cmd_local(&opts).await,
        _ => Err("usage: chaos local|plan|client|verify [options]".into()),
    };
    match res {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            eprintln!("chaos: {e}");
            std::process::exit(2);
        }
    }
}
