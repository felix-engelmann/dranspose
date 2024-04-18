mod control_plane;
mod data_plane;
mod dranspose;

use async_zmq::{Message, Result};
use clap::Parser;
use std::time::Instant;

use log::info;
use signal_hook::consts::signal::*;
use signal_hook_async_std::Signals;

use crate::control_plane::register;

use env_logger::Env;


struct TimedMultipart {
    multipart: Vec<Message>,
    received: Instant,
}

#[derive(Parser, Clone)]
struct Cli {
    #[clap(short, long, default_value_t = String::from("eiger"))]
    stream: String,
    #[clap(short, long, default_value_t = String::from("tcp://localhost:9999"))]
    upstream_url: String,
    #[clap(short, long, default_value_t = String::from("tcp://localhost:10000"))]
    ingester_url: String,
    #[clap(short, long, default_value_t = String::from("redis://127.0.0.1/"))]
    redis_url: String,
}

#[async_std::main]
async fn main() -> Result<()> {
    let args = Cli::parse();

    env_logger::Builder::from_env(Env::default().default_filter_or("debug")).init();

    let signals =
        Signals::new(&[SIGHUP, SIGTERM, SIGINT, SIGQUIT]).expect("unable to register signals");
    let handle = signals.handle();

    info!(
        "stream: {:?}, ingester_url: {:?}, upstream_url: {:?}",
        args.stream, args.ingester_url, args.upstream_url
    );

    let client = redis::Client::open(args.redis_url.clone()).unwrap();
    let con = client.get_multiplexed_async_connection().await.unwrap();

    /*info!("setting key");
    let _: () = con.set_ex("testkey", "balub value", 10).await.expect("set key");
    info!("key set");
    let _:() = con.del("testkey").await.expect("delkey");
    info!("key deleted");*/
    register(con, args, signals).await.expect("main task");

    handle.close();

    Ok(())
}
