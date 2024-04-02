mod data_plane;
mod dranspose;
mod control_plane;

use async_zmq::{Message, Result};
use serde::{Deserialize, Serialize};
use std::time::{Instant};
use clap::Parser;

use log::{info};
use signal_hook::consts::signal::*;
use signal_hook_async_std::Signals;

use crate::control_plane::register;




use env_logger::Env;


#[derive(Serialize, Deserialize)]
struct Stream1Packet {
    htype: String,
    msg_number: u64
}


struct TimedMultipart {
    multipart: Vec<Message>,
    received: Instant,
}




#[derive(Parser)]
struct Cli {
    #[clap(default_value_t = String::from("eiger"))]
    stream: String,
    #[clap(default_value_t = String::from("tcp://localhost:9999"))]
    upstream_url: String,
    #[clap(default_value_t = String::from("tcp://localhost:10000"))]
    ingester_url: String,
}




#[async_std::main]
async fn main() -> Result<()> {

    let args = Cli::parse();

    env_logger::Builder::from_env(Env::default().default_filter_or("debug")).init();


    let signals = Signals::new(&[SIGHUP, SIGTERM, SIGINT, SIGQUIT]).expect("unable to register signals");
    let handle = signals.handle();


    info!("stream: {:?}, ingester_url: {:?}, upstream_url: {:?}", args.stream, args.ingester_url, args.upstream_url);

    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let con = client.get_multiplexed_async_connection().await.unwrap();

    register(con, args, signals).await.expect("main task");

    handle.close();

    Ok(())

}
