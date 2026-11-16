//! A scripted local WebSocket server for fault-injection tests.
//!
//! Each accepted connection plays one script: send frames, go silent, or
//! drop the TCP connection without a close handshake. Test-only helper, but
//! public so the `market` crate's stream tests can reuse it.

use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

/// One step of a connection script.
#[derive(Debug, Clone)]
pub enum Step {
    /// Send a text frame.
    Text(String),
    /// Wait without sending anything (simulates a stale feed).
    Silent(Duration),
    /// Drop the TCP connection abruptly (no WebSocket close frame).
    Drop,
    /// Send a WebSocket close frame.
    Close,
}

pub fn text(s: impl Into<String>) -> Step {
    Step::Text(s.into())
}

/// Start a server that plays `scripts[i]` to the i-th connection. Returns the
/// `ws://` URL. Connections beyond the scripts list are held open silently.
pub async fn serve(scripts: Vec<Vec<Step>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    tokio::spawn(async move {
        let mut scripts = scripts.into_iter();
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            let script = scripts
                .next()
                .unwrap_or_else(|| vec![Step::Silent(Duration::from_secs(3600))]);
            tokio::spawn(async move {
                let Ok(ws) = tokio_tungstenite::accept_async(tcp).await else {
                    return;
                };
                let (mut tx, mut rx) = ws.split();
                // Drain client frames (subscribe, PING) so writes never block.
                let drain = tokio::spawn(async move { while rx.next().await.is_some() {} });
                for step in script {
                    match step {
                        Step::Text(t) => {
                            if tx.send(Message::Text(t)).await.is_err() {
                                break;
                            }
                        }
                        Step::Silent(d) => tokio::time::sleep(d).await,
                        Step::Drop => break,
                        Step::Close => {
                            let _ = tx.send(Message::Close(None)).await;
                            break;
                        }
                    }
                }
                drain.abort();
                // Dropping both halves closes the TCP socket.
            });
        }
    });
    format!("ws://{addr}")
}

/// Start a server that sends every frame published on `frames` to every
/// connection open at that moment, the way a venue fans one feed out to
/// all subscribers. Each new connection first gets the frames `greet`
/// returns (a venue's snapshot on subscribe). A connection that falls more
/// than the channel's capacity behind is dropped, like a venue drops a slow
/// reader. Client frames are read and ignored. Returns the `ws://` URL.
pub async fn serve_broadcast<G>(frames: tokio::sync::broadcast::Sender<String>, greet: G) -> String
where
    G: Fn() -> Vec<String> + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let greet = std::sync::Arc::new(greet);
    tokio::spawn(async move {
        loop {
            let Ok((tcp, _)) = listener.accept().await else {
                return;
            };
            // Subscribe before the handshake so no frame published after
            // the accept is missed.
            let mut live = frames.subscribe();
            let greet = greet.clone();
            tokio::spawn(async move {
                let Ok(ws) = tokio_tungstenite::accept_async(tcp).await else {
                    return;
                };
                let (mut tx, mut rx) = ws.split();
                let drain = tokio::spawn(async move { while rx.next().await.is_some() {} });
                for t in greet() {
                    if tx.send(Message::Text(t)).await.is_err() {
                        drain.abort();
                        return;
                    }
                }
                // A lagged or closed channel ends the connection.
                while let Ok(t) = live.recv().await {
                    if tx.send(Message::Text(t)).await.is_err() {
                        break;
                    }
                }
                drain.abort();
            });
        }
    });
    format!("ws://{addr}")
}
