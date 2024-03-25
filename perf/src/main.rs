use async_zmq::{Result, StreamExt};
use redis::aio::MultiplexedConnection;
use redis::streams::{StreamReadOptions, StreamReadReply, StreamRangeReply};
use serde::{Deserialize, Serialize};
use serde_json;
use redis::{from_redis_value, AsyncCommands};
use redis::Commands;
use std::collections::HashMap;
use uuid::Uuid;
use std::time::Duration;

use async_std::task;

#[derive(Serialize, Deserialize)]
struct Stream1Packet {
    htype: String,
    msg_number: u64
}


fn parse_stream1(data: &str) -> serde_json::Result<Stream1Packet> {
    // Some JSON input data as a &str. Maybe this comes from the user.

    // Parse the string of data into a Person object. This is exactly the
    // same function as the one that produced serde_json::Value above, but
    // now we are asking it for a Person as output.
    let p: Stream1Packet = serde_json::from_str(data)?;

    // Do things just like with any other Rust data structure.
    // println!("Please call {} at the number {}", p.htype, p.msg_number);

    Ok(p)
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct ConnectedWorker {
    name: String,
    service_uuid: Uuid,
    last_seen: f64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct IngesterState {
    service_uuid: Uuid,
    mapping_uuid: Option<Uuid>,
    parameters_hash: Option<u8>,
    processed_events: u64,
    event_rate: f32,
    name: String,
    url: String,
    #[serde(flatten)]
    connected_workers: HashMap<String, ConnectedWorker>,
    streams: Vec<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct ControllerUpdate {
    mapping_uuid: Uuid,
    finished: bool,
}


async fn register(mut con: MultiplexedConnection) -> redis::RedisResult<()> {
    let mut state = IngesterState{ service_uuid: Uuid::new_v4(),
        name: "rust-single-ingester".to_string(),
        url: "tcp://localhost:10000".to_string(),
        streams: vec!["eiger".to_string()],
        ..IngesterState::default()};

    //let latest: StreamRangeReply = con.xrevrange_count("dranspose:controller:updates", "+", "-", 1).await.unwrap();

    //println!("latest value is {:?}", latest);

    let mut lastid: String = "0".to_string();

    while true {

        let config = serde_json::to_string(&state).unwrap();
        println!("{}", &config);
        let _ : () = con.set_ex("dranspose:ingester:rust-single-ingester:config", &config, 10).await.unwrap();


        let opts = StreamReadOptions::default().block(6000);

        let update_msgs: StreamReadReply = con.xread_options(&["dranspose:controller:updates"], &[lastid.clone()], &opts ).await.unwrap();

        for key in update_msgs.keys.iter().filter(|&x| x.key == "dranspose:controller:updates") {
            lastid = key.ids.last().unwrap().id.clone();
            let data = &key.ids.last().unwrap().map;

            let update_str: String = from_redis_value(data.get("data").unwrap()).expect("msg");
            let update: ControllerUpdate = serde_json::from_str(&update_str).expect("msg");
            println!("got update {:?}", update);
            state.mapping_uuid = Some(update.mapping_uuid);
        }


        //println!("new responses: {:?}", update_msgs);
    }

    Ok(())
}



#[async_std::main]
async fn main() -> Result<()> {


    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let con = client.get_multiplexed_async_connection().await.unwrap();

    let register_task = task::spawn(async {
        register(con).await
    });
    println!("Started task!");

    let mut zmq = async_zmq::pull("tcp://127.0.0.1:9999")?.connect()?;

    while let Some(msg) = zmq.next().await {
        // Received message is a type of Result<MessageBuf>
        let msg = msg?;

        println!("{}", msg[0].as_str().unwrap());
        let packet = parse_stream1(msg[0].as_str().unwrap()).unwrap();
        if packet.htype == "series_end" {
            break;
        }
    }

    Ok(())

}