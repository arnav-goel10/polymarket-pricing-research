// Run: CHAINLINK_STREAMS_API_KEY=... CHAINLINK_STREAMS_API_SECRET=... \
//      BTC_FEED_ID=0x... cargo run -p feeds --example chainlink_latest
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let creds = feeds::load_creds()?;
    let feed = std::env::var("BTC_FEED_ID").expect("BTC_FEED_ID");
    let ep = feeds::Endpoints::mainnet();
    let r = feeds::chainlink::fetch_latest(&ep.rest_base, &creds, &feed).await?;
    println!(
        "BTC/USD price={} bid={} ask={} ts={}",
        r.price, r.bid, r.ask, r.observations_timestamp
    );
    Ok(())
}
