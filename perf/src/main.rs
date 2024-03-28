use async_std::stream::StreamExt;
use async_zmq::{AsRawSocket, Message, MultipartIter, Result, Router, SinkExt};
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
use std::os::linux::raw::ino_t;


use async_std::task;
use serde_json::json;

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
    //#[serde(flatten)]
    connected_workers: HashMap<Uuid, ConnectedWorker>,
    streams: Vec<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct ControllerUpdate {
    mapping_uuid: Uuid,
    finished: bool,
}

#[derive(Debug)]
enum Event {
    Start {
        mapping_uuid: Uuid,
    },
    ControllerUpdate
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



async fn forwarder(mut updates_r: mpsc::Receiver<Event>, mut connectedworker_s: mpsc::Sender<ConnectedWorker>, mut assignment_r: mpsc::Receiver<QueueWorkAssignment>) ->Result<()> {
    let mut router: Router<IntoIter<Message>, Message> = async_zmq::router("tcp://*:10000")?.bind()?;

    let mut pull = async_zmq::pull("tcp://127.0.0.1:9999")?.connect()?;

    loop {
        select! {
            control = updates_r.next().fuse() => {
                println!("forwarder got control message {:?}", control);
                match control {
                    Some(Event::Start{mapping_uuid}) => {
                        println!("got start {:?}", mapping_uuid);

                    }
                    _ => {}
                };

            },
            msg = pull.next().fuse() => {
                if let Some(data) = msg {
                    let stins: Vec<Message> = data?;
                    println!("got from pull {:?}", stins[0].as_str().unwrap());
                    let assignment = assignment_r.next().await.unwrap();
                    println!("send message to assignment {:?}", assignment);
                    /*{'w3': InternalWorkerMessage(event_number=9,
                        streams={'orca': StreamData(typ='STINS',
                            frames=[<zmq.sugar.frame.Frame object at 0x7f1d2ccd8410>, <zmq.sugar.frame.Frame object at 0x7f1d2ccdbf50>], length=2)})}
                    */
                    if let QueueWorkAssignment::WorkAssignment{assignment} = assignment {
                        let header = json!({"event_number":
                            assignment.event_number,"streams":{"eiger":{"typ":"STINS","length":stins.len()}}});

                        println!("header is {}", header.to_string());
                        let mymsg = stins;
                        if let w = assignment.assignments.get("eiger").unwrap().first().unwrap() {
                            println!("send event data to worker {:?}", w);
                            let mut payload = vec![Message::from("w1"),
                               Message::from(&header.to_string()) ];
                            payload.extend(mymsg);

                            router.send(MultipartIter::from(payload)
                            ).await.expect("unable to send");
                        }
                        else{
                            //needs copying
                        }
                        //for w in assignment.assignments.get("eiger").unwrap().iter() {

                    }
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
                    connectedworker_s.send(cw).await.expect("could not send message");
                }

            },
            complete => break,
        };
    }

    println!("forwarder terminated");

    Ok(())
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct WorkAssignment{
    event_number: u64,
    assignments: HashMap<String, Vec<String>>
}

#[derive(Debug)]
enum QueueWorkAssignment {
    Start {
        mapping_uuid: Uuid,
    },
    WorkAssignment{
        assignment: WorkAssignment
    }
}



async fn register(mut con: MultiplexedConnection, mut con_assign: MultiplexedConnection) -> redis::RedisResult<()> {
    let mut state = IngesterState{ service_uuid: Uuid::new_v4(),
        name: "rust-single-ingester".to_string(),
        url: "tcp://localhost:10000".to_string(),
        streams: vec!["eiger".to_string()],
        ..IngesterState::default()};

    //let mut router = async_zmq::router("tcp://*:10000").expect("no router socket");
    //let socket = router
    //let sock: Router<IntoIter<Message>, Message> = router.bind().expect("unable to bind");
    //sock.as_raw_socket().set_router_mandatory(true);
    //sock.recv_multipart()


    let (mut reg_fwd_updates_s, reg_fwd_updates_r) = mpsc::channel(1000);
    //let (reg_ass_updates_s, reg_ass_updates_r) = mpsc::channel(1000);
    let (fwd_reg_connectedworker_s, mut fwd_reg_connectedworker_r) = mpsc::channel(1000);
    let (mut reg_fwd_assignment_s, reg_fwd_assignment_r) = mpsc::channel(1000);


    let forwarder_task = task::spawn(forwarder(reg_fwd_updates_r, fwd_reg_connectedworker_s, reg_fwd_assignment_r));
    //let assign_task = task::spawn(get_assignments(con, reg_ass_updates_r, ass_fwd_assignment_s));
    //println!("latest value is {:?}", latest);


    let mut lastid: String = "0".to_string();
    let mut lastev: String = "0".to_string();

    // check if we joined late and discard a lot
    let latest: StreamRangeReply = con.xrevrange_count("dranspose:controller:updates", "+", "-", 1).await.unwrap();
    println!("latest update{:?}", latest);
    if let Some(firstelem) = latest.ids.first() {
        lastid = firstelem.id.clone();
    }



    loop {

        let config = serde_json::to_string(&state).unwrap();
        //println!("{}", &config);
        let _ : () = con.set_ex("dranspose:ingester:rust-single-ingester:config", &config, 10).await.unwrap();

        let opts = StreamReadOptions::default().block(6000);
        let optslong = StreamReadOptions::default().block(10);
        let idcopy = vec![lastid.clone()];
        let evcopy = vec![lastev.clone()];

        let assignedkey = format!("dranspose:assigned:{}", state.mapping_uuid.unwrap_or(Uuid::default()).to_string());

        //println!("assigned key is {:?}", assignedkey);

        let assignedkeylist = vec![assignedkey.as_str()];

        select! {
            update_msgs = con.xread::<&str, String, StreamReadReply>(&["dranspose:controller:updates"], &idcopy ).fuse() => {
                if let Ok(update_msgs) = update_msgs {
                    for key in update_msgs.keys.iter().filter(|&x| x.key == "dranspose:controller:updates") {
                        lastid = key.ids.last().unwrap().id.clone();
                        let data = &key.ids.last().unwrap().map;
                        let update_str: String = from_redis_value(data.get("data").unwrap()).expect("msg");
                        let update: ControllerUpdate = serde_json::from_str(&update_str).expect("msg");
                        println!("got update {:?}", update);
                        if Some(update.mapping_uuid) != state.mapping_uuid {
                            state.mapping_uuid = Some(update.mapping_uuid);
                            println!("resetting config to {}", update.mapping_uuid);
                            reg_fwd_assignment_s.send(QueueWorkAssignment::Start {mapping_uuid: update.mapping_uuid});
                            reg_fwd_updates_s.send(Event::Start {mapping_uuid: update.mapping_uuid}).await.expect("cannot send");
                            //await self.restart_work(newuuid)
                        }
                        if update.finished == true {
                            let finished = json!({"state":"finished", "source": "ingester", "ingester":"rust-single-ingester"});
                            let _ : () = con.xadd(format!("dranspose:ready:{}", update.mapping_uuid.to_string()),
                            "*", &vec![("data",finished.to_string() )]).await?;
                            //let _ : () = con.xadd(,
                            //    "data":finished.to_string()).await.unwrap();
                        }
                    }
                }
            },
            cw = fwd_reg_connectedworker_r.next().fuse() => {
                if let Some(cw) = cw {
                    println!("update connected worker {:?}", cw);
                    state.connected_workers.insert(cw.service_uuid, cw);
                }
            },
            assignment_msgs = con_assign.xread_options::<&str, String, StreamReadReply>(&assignedkeylist, &evcopy, &optslong ).fuse() => {
                println!("got assignmentmsg {:?}",assignment_msgs );
                if let Ok(assignment_msgs) = assignment_msgs {
                    for key in assignment_msgs.keys.iter().filter(|&x| x.key == assignedkey) {
                        lastev = key.ids.last().unwrap().id.clone();
                        let data = &key.ids.last().unwrap().map;
                        let update_str: String = from_redis_value(data.get("data").unwrap()).expect("msg");
                        let assignments: Vec<WorkAssignment> = serde_json::from_str(&update_str).expect("marshall not work");
                        println!("got assignment {:?}", assignments);
                        for assignment in assignments.iter() {
                            reg_fwd_assignment_s.send(QueueWorkAssignment::WorkAssignment{assignment: assignment.clone()}).await.expect("cannot send");
                        }
                    }
                }
            }
        };


        //println!("new responses: {:?}", update_msgs);
    }

    Ok(())
}



#[async_std::main]
async fn main() -> Result<()> {


    let client = redis::Client::open("redis://127.0.0.1/").unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();
    let client_assign = redis::Client::open("redis://127.0.0.1/").unwrap();
    let con_assign = client_assign.get_multiplexed_async_connection().await.unwrap();

    let register_task = task::spawn(async {
        register(con, con_assign).await
    });

    println!("Started task!");


    register_task.await;



    Ok(())

}