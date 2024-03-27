use async_std::stream::StreamExt;
use async_zmq::{AsRawSocket, Message, Result, Router, SinkExt};
use redis::aio::MultiplexedConnection;
use redis::streams::{StreamReadOptions, StreamReadReply, StreamRangeReply, StreamId};
use serde::{Deserialize, Serialize};
use serde_json;
use redis::{from_redis_value, AsyncCommands, RedisResult};
use redis::Commands;
use std::collections::HashMap;
use uuid::Uuid;
use std::time::Duration;
use futures::channel::mpsc;
use std::vec::IntoIter;
use std::sync::Arc;

use futures::{
    future::FutureExt, // for `.fuse()`
    pin_mut,
    future,
    select,
};
use std::time::{SystemTime, UNIX_EPOCH};
use std::ops::{Deref};


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
    connected_workers: HashMap<Uuid, ConnectedWorker>,
    streams: Vec<String>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct ControllerUpdate {
    mapping_uuid: Uuid,
    finished: bool,
}

async fn work() -> Result<()> {
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

async fn inner_manager() ->Result<()> {
    let mut router: Router<IntoIter<Message>, Message> = async_zmq::router("tcp://*:10000")?.bind()?;

    loop {
        let item = select! {
            x = router.next().fuse() => x,
            //x = s2.next() => x,
            complete => break,
        };
        if let Some(next_num) = item {
            println!("res {:?}", next_num);
        }
    }

    println!("futures are over");

    Ok(())
}

#[derive(Debug)]
enum Event {
    WorkAssignment {
        event_number: String,
        workers: Vec<String>,
    },
    Control {
        restart: bool
    },
}


async fn forwarder(mut events: mpsc::Receiver<Event>, mut workers: mpsc::Sender<ConnectedWorker>) ->Result<()> {
    let mut router: Router<IntoIter<Message>, Message> = async_zmq::router("tcp://*:10000")?.bind()?;

    let mut pull = async_zmq::pull("tcp://127.0.0.1:9999")?.connect()?;

    loop {
        select! {
            msg = pull.next().fuse() => {
                if let Some(data) = msg {
                    let data = data?;
                    println!("got from pull {:?}", data[0].as_str().unwrap());
                }
            },
            msg = router.next().fuse() => {
                if let Some(msg) = msg {
                    let data = msg?;
                    let start = SystemTime::now();
                    let since_the_epoch = start
                        .duration_since(UNIX_EPOCH)
                        .expect("Time went backwards");

                    let cw = ConnectedWorker{
                        name: data[0].as_str().unwrap().to_string(),
                        service_uuid: Uuid::from_bytes({
                            let mut array = [0; 16];
                            array[..16].copy_from_slice(data[1].deref());
                            array
                        }
                        ),
                        last_seen: since_the_epoch.as_millis() as f64/1000f64
                    };
                    print!("got connected worker {:?}", cw);
                    workers.send(cw).await.expect("could not send message");
                }

            },
            complete => break,
        };
    }

    Ok(())
}

async fn register(mut con: MultiplexedConnection) -> redis::RedisResult<()> {
    let mut state = IngesterState{ service_uuid: Uuid::new_v4(),
        name: "rust-single-ingester".to_string(),
        url: "tcp://localhost:10000".to_string(),
        streams: vec!["eiger".to_string()],
        ..IngesterState::default()};

    //let mut router = async_zmq::router("tcp://*:10000").expect("no router socket");
    //let socket = router
    //let sock: Router<IntoIter<Message>, Message> = router.bind().expect("unable to bind");
    //sock.as_raw_socket().set_router_mandatory(true);
    //let latest: StreamRangeReply = con.xrevrange_count("dranspose:controller:updates", "+", "-", 1).await.unwrap();
    //let arcsock = Arc::new(sock);

    //sock.recv_multipart()

    let (ev_sender, ev_receiver) = mpsc::channel(1000);
    let (wo_sender, wo_receiver) = mpsc::channel(1000);

    let forwarder_task = task::spawn(forwarder(ev_receiver, wo_sender));
    //println!("latest value is {:?}", latest);


    let mut lastid: String = "0".to_string();

    loop {

        let config = serde_json::to_string(&state).unwrap();
        println!("{}", &config);
        let _ : () = con.set_ex("dranspose:ingester:rust-single-ingester:config", &config, 10).await.unwrap();


        let opts = StreamReadOptions::default().block(6000);
        let lastidcopy = lastid.clone();
        select! {
            update_msgs = con.xread_options::<&str, std::string::String, StreamReadReply>(&["dranspose:controller:updates"], &[lastidcopy], &opts ).fuse() => {
                if let Ok(update_msgs) = update_msgs {
                    for key in update_msgs.keys.iter().filter(|&x| x.key == "dranspose:controller:updates") {
                        lastid = key.ids.last().unwrap().id.clone();
                        let data = &key.ids.last().unwrap().map;

                        let update_str: String = from_redis_value(data.get("data").unwrap()).expect("msg");
                        let update: ControllerUpdate = serde_json::from_str(&update_str).expect("msg");
                        println!("got update {:?}", update);
                        if Some(update.mapping_uuid) != state.mapping_uuid {
                            println!("resetting config to {}", update.mapping_uuid);
                            //await self.restart_work(newuuid)
                        }
                        state.mapping_uuid = Some(update.mapping_uuid);
                    }
                }

            }
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


    register_task.await;


    Ok(())

}