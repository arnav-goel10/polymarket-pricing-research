// Run (no credentials needed): cargo run -p feeds --example rtds_stream
// Streams BTC ticks from Polymarket RTDS — both Binance and Chainlink sources.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!(
        "connecting to Polymarket RTDS ({}) ...",
        feeds::rtds::RTDS_URL
    );
    feeds::rtds::run(feeds::rtds::RTDS_URL, |t| {
        println!("{:?} btc={} ts={}", t.source, t.price, t.ts_millis);
    })
    .await
}
