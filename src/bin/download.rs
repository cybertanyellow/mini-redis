//use flatbread::{client, DEFAULT_PORT};

use tracing::{debug, info};
use structopt::StructOpt;
use flatbread::fb161::Fb161Downloader;
use chrono::NaiveDateTime;

#[derive(StructOpt, Debug)]
#[structopt(name = "flatbread-download", version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = "Issue download commands")]
struct Cli {
    #[structopt(name = "database-url", short = "u", long = "--db-url")]
    db_url: Option<String>,

    #[structopt(name = "start-time", short = "s", long = "--start")]
    start_time: Option<NaiveDateTime>,

    #[structopt(name = "destination", short = "d", long = "--dst")]
    dst: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> flatbread::Result<()> {
    // Enable logging
    tracing_subscriber::fmt::try_init()?;

    // Parse command line arguments
    let cli = Cli::from_args();

    debug!("{:#?} {:#?} {}", &cli.db_url, &cli.start_time, &cli.dst);

    //let ts = NaiveDateTime::parse_from_str(&cli.start_time, "%Y-%m-%d %H:%M:%S")?;

    let mut downloader = Fb161Downloader::new(cli.db_url, cli.start_time, cli.dst).await;
    downloader.download().await?;

    info!("Download complete");

    Ok(())
}
