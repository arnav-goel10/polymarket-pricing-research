// Run: cargo run -p feeds --example binance_stream
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    feeds::binance::run(feeds::binance::default_url(), |ev| println!("{ev:?}")).await
}
