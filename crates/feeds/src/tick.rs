use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FeedSource {
    Chainlink,
    Binance,
    Coinbase,
}

/// A normalized price observation from any venue.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Tick {
    pub source: FeedSource,
    pub price: f64,
    pub ts_millis: i64,
}

/// A signed trade print used for order-flow signals.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Trade {
    pub price: f64,
    pub qty: f64,
    pub buyer_is_maker: bool, // true => aggressor is the SELLER
    pub ts_millis: i64,
}

/// Best bid/ask snapshot for queue-imbalance signals.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct BookUpdate {
    pub bid: f64,
    pub bid_qty: f64,
    pub ask: f64,
    pub ask_qty: f64,
    pub ts_millis: i64,
}

/// Multi-producer broadcast bus for normalized ticks.
#[derive(Clone)]
pub struct TickBus {
    tx: broadcast::Sender<Tick>,
}

impl TickBus {
    pub fn new(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity);
        TickBus { tx }
    }
    pub fn subscribe(&self) -> broadcast::Receiver<Tick> {
        self.tx.subscribe()
    }
    /// Publish a tick. Returns the number of active receivers (0 is fine).
    pub fn publish(&self, tick: Tick) -> usize {
        self.tx.send(tick).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_carries_source_price_and_time() {
        let t = Tick {
            source: FeedSource::Chainlink,
            price: 60_000.0,
            ts_millis: 1_700_000_000_000,
        };
        assert_eq!(t.source, FeedSource::Chainlink);
        assert_eq!(t.price, 60_000.0);
    }

    #[tokio::test]
    async fn tick_bus_broadcasts_to_subscribers() {
        let bus = TickBus::new(16);
        let mut rx = bus.subscribe();
        bus.publish(Tick {
            source: FeedSource::Binance,
            price: 1.0,
            ts_millis: 1,
        });
        let got = rx.recv().await.unwrap();
        assert_eq!(got.price, 1.0);
    }
}
