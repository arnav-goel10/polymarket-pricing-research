//! Venue simulator for the HA harness: three WebSocket venues (RTDS,
//! Binance, CLOB order book) fed by one generator, with total truth.
//!
//! cargo run --release -p app --bin venue_sim -- [--seed 1] [--rate 200]
//!     [--manifest truth.prec] [--recording fixtures/live-sample.prec]
//!     [--lag-ms 0] [--start-after 0]
//!
//! Synthetic mode makes `--rate` Binance and CLOB frames a second (half
//! each) plus 5 RTDS frames a second, deterministic from `--seed` (see
//! `app::sim`). With `--recording` it plays the venue frames of a recording
//! once, `--rate` a second, then stays connected and silent.
//!
//! Every broadcast frame is written to `--manifest` with its broadcast time,
//! so the truth is a `.prec` file and the CI check and a live check read
//! truth the same way. Greetings are not written: they repeat frames that
//! are already there. Each new RTDS connection is greeted with the last 60
//! points of each price series and each new CLOB connection with the latest
//! `book` of each asset, as the real venues do. RTDS answers `PING` with
//! `PONG`.
//!
//! `--lag-ms K` delays every frame to every second connection on each
//! venue (the 2nd, 4th, ...). `--start-after N` holds the generator until
//! every venue has had N connections.
//!
//! Prints `venue_sim rtds <url> binance <url> clob <url>` once listening.
//! On SIGINT or SIGTERM it closes the manifest and exits 0.

use app::ingest::shutdown_signal;
use app::sim::{Greeter, Synth, RTDS_TICK_MS};
use feeds::recording::{read_all, RecordWriter, Source};
use futures_util::{SinkExt, StreamExt};
use std::collections::VecDeque;
use std::io::Write as _;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Message;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

const CHANNEL: usize = 1 << 16;
const SOURCES: [Source; 3] = [Source::Rtds, Source::Binance, Source::ClobBook];

struct Args {
    seed: u64,
    rate: f64,
    manifest: Option<PathBuf>,
    recording: Option<PathBuf>,
    lag: Duration,
    start_after: u64,
}

fn parse_args() -> Result<Args, String> {
    let mut a = Args {
        seed: 1,
        rate: 200.0,
        manifest: None,
        recording: None,
        lag: Duration::ZERO,
        start_after: 0,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut val = || it.next().ok_or(format!("{arg} needs a value"));
        let bad = |what: &str| format!("bad {what}");
        match arg.as_str() {
            "--seed" => a.seed = val()?.parse().map_err(|_| bad("--seed"))?,
            "--rate" => a.rate = val()?.parse().map_err(|_| bad("--rate"))?,
            "--manifest" => a.manifest = Some(PathBuf::from(val()?)),
            "--recording" => a.recording = Some(PathBuf::from(val()?)),
            "--lag-ms" => {
                a.lag = Duration::from_millis(val()?.parse().map_err(|_| bad("--lag-ms"))?)
            }
            "--start-after" => a.start_after = val()?.parse().map_err(|_| bad("--start-after"))?,
            s => return Err(format!("unknown argument {s}")),
        }
    }
    if a.rate.is_nan() || a.rate <= 0.0 {
        return Err("--rate must be above 0".into());
    }
    Ok(a)
}

fn wall_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

struct Venue {
    src: Source,
    tx: broadcast::Sender<Arc<str>>,
    conns: AtomicU64,
}

struct Shared {
    venues: [Venue; 3],
    greeter: Mutex<Greeter>,
    lag: Duration,
}

fn venue(shared: &Shared, src: Source) -> &Venue {
    &shared.venues[SOURCES.iter().position(|s| *s == src).expect("venue")]
}

async fn accept_loop(listener: TcpListener, shared: Arc<Shared>, src: Source) {
    loop {
        let Ok((tcp, _)) = listener.accept().await else {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        };
        let _ = tcp.set_nodelay(true);
        let v = venue(&shared, src);
        let n = v.conns.fetch_add(1, Relaxed);
        // Subscribe before taking the greeting, so no frame falls between.
        let live = v.tx.subscribe();
        let greet = shared
            .greeter
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .greeting(src);
        let lag = if n % 2 == 1 {
            shared.lag
        } else {
            Duration::ZERO
        };
        tokio::spawn(async move {
            let _ = serve_conn(tcp, live, greet, lag, src == Source::Rtds).await;
        });
    }
}

async fn serve_conn(
    tcp: tokio::net::TcpStream,
    mut live: broadcast::Receiver<Arc<str>>,
    greet: Vec<String>,
    lag: Duration,
    pong: bool,
) -> Result<(), BoxError> {
    let ws = tokio_tungstenite::accept_async(tcp).await?;
    let (mut tx, mut rx) = ws.split();
    for g in greet {
        tx.send(Message::Text(g)).await?;
    }
    let mut delayed: VecDeque<(tokio::time::Instant, Arc<str>)> = VecDeque::new();
    loop {
        let due = delayed.front().map(|d| d.0);
        tokio::select! {
            r = live.recv() => match r {
                Ok(f) if lag.is_zero() => tx.send(Message::Text(f.to_string())).await?,
                Ok(f) => delayed.push_back((tokio::time::Instant::now() + lag, f)),
                // A reader that fell a whole channel behind is dropped, as a
                // venue drops a slow subscriber.
                Err(_) => return Ok(()),
            },
            _ = async { tokio::time::sleep_until(due.expect("due")).await }, if due.is_some() => {
                if let Some((_, f)) = delayed.pop_front() {
                    tx.send(Message::Text(f.to_string())).await?;
                }
            }
            m = rx.next() => match m {
                Some(Ok(Message::Text(t))) if pong && t == "PING" => {
                    tx.send(Message::Text("PONG".into())).await?
                }
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return Ok(()),
                Some(Ok(_)) => {}
            },
        }
    }
}

/// Broadcasts frames and writes them to the manifest.
struct Publisher {
    shared: Arc<Shared>,
    manifest: Option<RecordWriter<std::fs::File>>,
    published: u64,
}

impl Publisher {
    fn publish(&mut self, src: Source, frame: String) -> std::io::Result<()> {
        if let Some(m) = &mut self.manifest {
            m.append(wall_micros(), src, &frame)?;
        }
        self.shared
            .greeter
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .observe(src, &frame);
        let _ = venue(&self.shared, src).tx.send(frame.into());
        self.published += 1;
        Ok(())
    }
}

fn wall_millis() -> i64 {
    wall_micros() / 1_000
}

async fn generate(
    mut p: Publisher,
    args: Args,
    stop: Arc<AtomicBool>,
) -> Result<Publisher, BoxError> {
    while SOURCES
        .iter()
        .any(|s| venue(&p.shared, *s).conns.load(Relaxed) < args.start_after)
    {
        if stop.load(Relaxed) {
            return Ok(p);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut recorded = match &args.recording {
        Some(path) => {
            let (recs, _) = read_all(path).map_err(|e| format!("{path:?}: {e:?}"))?;
            Some(
                recs.into_iter()
                    .filter(|r| r.source != Source::Control)
                    .collect::<VecDeque<_>>(),
            )
        }
        None => None,
    };
    let mut synth = Synth::new(args.seed);
    let start = Instant::now();
    let mut sent: u64 = 0;
    let mut next_rtds = 0i64;
    while !stop.load(Relaxed) {
        let due = (start.elapsed().as_secs_f64() * args.rate) as u64;
        while sent < due {
            sent += 1;
            match &mut recorded {
                Some(q) => match q.pop_front() {
                    Some(r) => p.publish(r.source, r.frame)?,
                    None => break,
                },
                None => {
                    let now = wall_millis();
                    if sent.is_multiple_of(2) {
                        p.publish(Source::Binance, synth.binance(now))?;
                    } else {
                        p.publish(Source::ClobBook, synth.clob(now))?;
                    }
                }
            }
        }
        if recorded.is_none() {
            let elapsed = start.elapsed().as_millis() as i64;
            while next_rtds <= elapsed {
                next_rtds += RTDS_TICK_MS;
                p.publish(Source::Rtds, synth.rtds(wall_millis()))?;
            }
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    Ok(p)
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("venue_sim: {e}");
            std::process::exit(2);
        }
    };
    let shared = Arc::new(Shared {
        venues: SOURCES.map(|src| Venue {
            src,
            tx: broadcast::channel(CHANNEL).0,
            conns: AtomicU64::new(0),
        }),
        greeter: Mutex::new(Greeter::default()),
        lag: args.lag,
    });
    let mut urls = Vec::new();
    for v in &shared.venues {
        let l = TcpListener::bind("127.0.0.1:0").await?;
        urls.push(format!("ws://{}", l.local_addr()?));
        tokio::spawn(accept_loop(l, shared.clone(), v.src));
    }
    let manifest = match &args.manifest {
        Some(p) => Some(RecordWriter::create(p)?),
        None => None,
    };
    let publisher = Publisher {
        shared: shared.clone(),
        manifest,
        published: 0,
    };
    println!(
        "venue_sim rtds {} binance {} clob {}",
        urls[0], urls[1], urls[2]
    );
    std::io::stdout().flush()?;

    let stop = Arc::new(AtomicBool::new(false));
    let gen = tokio::spawn(generate(publisher, args, stop.clone()));
    let reason = shutdown_signal().await;
    stop.store(true, Relaxed);
    let p = gen.await??;
    let published = p.published;
    if let Some(m) = p.manifest {
        m.finish()?;
    }
    println!("venue_sim stopped: broadcast {published} frames ({reason})");
    Ok(())
}
