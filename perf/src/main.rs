use async_std::stream::StreamExt;
use async_zmq::{AsRawSocket, Message, MultipartIter, Result, Router, SinkExt};
use redis::aio::MultiplexedConnection;
use redis::streams::{StreamReadOptions, StreamReadReply, StreamRangeReply, StreamId};
use serde::{Deserialize, Serialize};
use serde_json;
use redis::{from_redis_value, AsyncCommands, RedisResult};
use redis::Commands;
use std::collections::{HashMap, VecDeque};
use uuid::Uuid;
use std::time::{Duration, Instant};
use futures::channel::mpsc;
use std::vec::IntoIter;
use std::sync::Arc;
use clap::Parser;

use log::{debug, info, error};
use url::Url;
use signal_hook::consts::signal::*;
use signal_hook_async_std::Signals;



use futures::{
    future::FutureExt, // for `.fuse()`
    pin_mut,
    future,
    select,
};
use std::time::{SystemTime, UNIX_EPOCH};
use std::ops::{Deref};
use std::os::linux::raw::ino_t;
use std::string::ToString;


use async_std::task;
use env_logger::Env;
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

struct TimedMultipart {
    multipart: Vec<Message>,
    received: Instant,
}



async fn forwarder(args: Cli, mut connectedworker_s: mpsc::Sender<ForwarderEvent>, mut assignment_r: mpsc::Receiver<QueueWorkAssignment>) ->Result<()> {
    let url = Url::parse(&args.ingester_url).expect("unparsable url");

    let listenurl = format!("tcp://*:{}", url.port().unwrap());

    let mut router: Router<IntoIter<Message>, Message> = async_zmq::router(&listenurl)?.bind()?;

    let mut pull = async_zmq::pull(&args.upstream_url)?.connect()?;

    let mut asbuf: VecDeque<WorkAssignment> = VecDeque::new();
    let mut pkbuf: VecDeque<TimedMultipart> = VecDeque::new();

    let mut no_events = 0;
    let mut starttime = Instant::now();
    loop {
        debug!("forwarder looped");


        while asbuf.len() > 0 && pkbuf.len() > 0 {

            let assignment = asbuf.pop_back().unwrap();
            let timed = pkbuf.pop_back().unwrap();
            let stins = timed.multipart;

            debug!("send message to assignment {:?}", assignment);
            /*{'w3': InternalWorkerMessage(event_number=9,
                streams={'orca': StreamData(typ='STINS',
                    frames=[<zmq.sugar.frame.Frame object at 0x7f1d2ccd8410>, <zmq.sugar.frame.Frame object at 0x7f1d2ccdbf50>], length=2)})}
            */
            let header = json!({"event_number":
                            assignment.event_number,"streams":{args.stream.clone():{"typ":"STINS","length":stins.len()}}});

            //println!("header is {}", header.to_string());
            let mymsg = stins;
            let workerlist = assignment.assignments.get(&args.stream).unwrap();
            if workerlist.len() == 1 {
                let w = workerlist.first().unwrap();
                //println!("send event data to worker {:?}", w);
                let mut payload = vec![Message::from(w),
                                       Message::from(&header.to_string()) ];
                payload.extend(mymsg);

                router.send(MultipartIter::from(payload)
                ).await.expect("unable to send");
                //println!("sending took {} microsec", timed.received.elapsed().as_micros());
            }
            else{
                //needs copyingg
                for w in workerlist {
                    //println!("copy message to worker {:?}", w);
                    let msgcopy: Vec<Message> = mymsg.iter().map(|m| Message::from(m.to_vec().clone())).collect();
                    let mut payload = vec![Message::from(w),
                                           Message::from(&header.to_string())];
                    payload.extend(msgcopy);

                    router.send(MultipartIter::from(payload)
                    ).await.expect("unable to send");
                }
            }
            no_events+=1;
            if no_events%1000==999 {
                let micros = starttime.elapsed().as_micros();
                println!("{} to {} packets in {} microsecs = {} p/s", no_events-1000, no_events, micros, 1000000000/micros);
                starttime = Instant::now();
            }
            //println!("finished sending out assignment to workers {:?}", assignment);
        }
        /*if pkbuf.len() > 0 {
            println!("waiting for assignments");
        }*/
        //println!("start forwarder select");
        select! {
            control = assignment_r.next().fuse() => {
                //println!("forwarder got control message {:?}", control);
                match control {
                    Some(QueueWorkAssignment::Start{mapping_uuid}) => {
                        println!("got start {:?}", mapping_uuid);
                        asbuf = VecDeque::new();
                        pkbuf = VecDeque::new();
                        connectedworker_s.send(ForwarderEvent::Ready{mapping_uuid}).await.expect("must be able to ready");
                        println!("ready sent from fwd");
                    },
                    Some(QueueWorkAssignment::WorkAssignment {assignment}) => {
                        asbuf.push_front(assignment);
                    },
                    None => {
                        println!("control message was none");
                        break;}
                };
            },
            msg = pull.next().fuse() => {
                if let Some(data) = msg {
                    let stins: Vec<Message> = data?;
                    //println!("got from pull {:?}", stins[0].as_str().unwrap());
                    let now = Instant::now();
                    pkbuf.push_front(TimedMultipart{multipart: stins, received:now});
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
                    debug!("got connected worker {:?}", cw);
                    connectedworker_s.send(ForwarderEvent::ConnectedWorker {connected_worker:cw}).await.expect("could not send message");
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

#[derive(Debug)]
enum ForwarderEvent {
    ConnectedWorker {
        connected_worker: ConnectedWorker,
    },
    Ready{
        mapping_uuid: Uuid
    }
}



async fn register(mut con: MultiplexedConnection, args: Cli, mut signals: Signals) -> redis::RedisResult<()> {
    let name = format!("rust-{}-ingester", args.stream);
    let mut state = IngesterState{ service_uuid: Uuid::new_v4(),
        name: name.clone(),
        url: args.ingester_url.clone(),
        streams: vec![args.stream.clone()],
        ..IngesterState::default()};

    //let mut router = async_zmq::router("tcp://*:10000").expect("no router socket");
    //let socket = router
    //let sock: Router<IntoIter<Message>, Message> = router.bind().expect("unable to bind");
    //sock.as_raw_socket().set_router_mandatory(true);
    //sock.recv_multipart()


    let (fwd_reg_connectedworker_s, mut fwd_reg_connectedworker_r) = mpsc::channel(1000);
    let (mut reg_fwd_assignment_s, reg_fwd_assignment_r) = mpsc::channel(1000);


    let forwarder_task = task::spawn(forwarder(args, fwd_reg_connectedworker_s, reg_fwd_assignment_r));


    let mut lastid: String = "0".to_string();
    let mut lastev: String = "0".to_string();

    // check if we joined late and discard a lot
    let latest: StreamRangeReply = con.xrevrange_count("dranspose:controller:updates", "+", "-", 1).await.unwrap();
    println!("latest update{:?}", latest);
    if let Some(firstelem) = latest.ids.first() {
        lastid = firstelem.id.clone();
    }

    let mut pending_uuid: Option<Uuid> = None;

    loop {

        let config = serde_json::to_string(&state).unwrap();
        debug!("{}", &config);
        let configkey = format!("dranspose:ingester:{}:config", name);
        let _ : () = con.set_ex(&configkey, &config, 10).await.unwrap();

        let opts = StreamReadOptions::default().block(6000);
        let ids = vec![lastid.clone(), lastev.clone()];

        let assignedkey = format!("dranspose:assigned:{}", state.mapping_uuid.unwrap_or(Uuid::default()).to_string());

        let keylist = vec!["dranspose:controller:updates", assignedkey.as_str()];
        debug!("register select");
        select! {
            update_msgs = con.xread_options::<&str, String, StreamReadReply>(&keylist, &ids, &opts).fuse() => {
                debug!("raw redis message {:?}", update_msgs);
                if let Ok(update_msgs) = update_msgs {
                    if let Some(key) = update_msgs.keys.iter().find(|&x| x.key == "dranspose:controller:updates") {
                        lastid = key.ids.last().unwrap().id.clone();
                        let data = &key.ids.last().unwrap().map;
                        let update_str: String = from_redis_value(data.get("data").unwrap()).expect("msg");
                        let update: ControllerUpdate = serde_json::from_str(&update_str).expect("msg");
                        println!("got update {:?}", update);
                        if Some(update.mapping_uuid) != state.mapping_uuid {
                            //state.mapping_uuid = Some(update.mapping_uuid);
                            println!("resetting config to {}", update.mapping_uuid);
                            reg_fwd_assignment_s.send(QueueWorkAssignment::Start {mapping_uuid: update.mapping_uuid}).await.expect("work");
                            println!("wait for forwarder ready");
                            pending_uuid = Some( update.mapping_uuid);
                        }
                        if update.finished == true {
                            let finished = json!({"state":"finished", "source": "ingester", "ingester":name});
                            let _ : () = con.xadd(format!("dranspose:ready:{}", update.mapping_uuid.to_string()),
                            "*", &vec![("data",finished.to_string() )]).await?;
                            //let _ : () = con.xadd(,
                            //    "data":finished.to_string()).await.unwrap();
                        }
                    }
                    if let Some(key) = update_msgs.keys.iter().find(|&x| x.key == assignedkey) {
                        debug!("got raw assignments {:?}", key);
                        for upd in key.ids.iter() {
                            let data = &upd.map;
                            let update_str: String = from_redis_value(data.get("data").unwrap()).expect("msg");
                            let assignments: Vec<WorkAssignment> = serde_json::from_str(&update_str).expect("marshall not work");
                            debug!("got assignments {:?}", assignments);
                            for assignment in assignments.iter() {
                                debug!("send assign");
                                reg_fwd_assignment_s.send(QueueWorkAssignment::WorkAssignment{assignment: assignment.clone()}).await.expect("cannot send");
                                debug!("sent assign")
                            }
                            lastev = upd.id.clone();
                        }


                    }
                }
            },
            cw = fwd_reg_connectedworker_r.next().fuse() => {
                if let Some(cw) = cw {
                    debug!("update connected worker {:?}", cw);
                    match cw {
                        ForwarderEvent::ConnectedWorker{connected_worker} => {
                            state.connected_workers.insert(connected_worker.service_uuid, connected_worker);
                        },
                        ForwarderEvent::Ready {mapping_uuid}  => {println!("forwarder is ready, change state");
                            if let Some(new_uuid) = pending_uuid {
                                state.mapping_uuid = Some(new_uuid);
                                pending_uuid = None;
                            }

                        }
                    };

                }
            },
            sig = signals.next().fuse() => {
                if let Some(signal) = sig {
                    match signal {
                        SIGTERM | SIGINT | SIGQUIT => {
                            // Shutdown the system;
                            println!("signal received");
                            let _: () = con.del(&configkey).await.expect("cannot delete config");
                            println!("deleted config, terminate");
                            break;
                        },
                        _ => unreachable!(),
                    }
                }
            },
        };


    }

    Ok(())
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
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    register(con, args, signals).await.expect("main task");

    handle.close();

    Ok(())

}
