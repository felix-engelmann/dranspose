use std::time::Duration;
use async_std::task;
use async_zmq::SinkExt;
use futures::channel::mpsc;
use futures::{select, StreamExt,future::FutureExt};
use log::{debug, info};
use redis::aio::MultiplexedConnection;
use redis::{AsyncCommands, from_redis_value};
use redis::streams::{StreamRangeReply, StreamReadOptions, StreamReadReply};
use signal_hook_async_std::Signals;
use uuid::Uuid;
use crate::{Cli};
use crate::data_plane::forwarder;
use crate::dranspose::{ConnectedWorker, ControllerUpdate, IngesterState, WorkAssignment};
use serde_json::json;
use signal_hook::consts::signal::*;


#[derive(Debug)]
pub(crate) enum ForwarderEvent {
    ConnectedWorker {
        connected_worker: ConnectedWorker,
    },
    Ready{
        mapping_uuid: Uuid
    }
}


#[derive(Debug)]
pub(crate) enum QueueWorkAssignment {
    Start {
        mapping_uuid: Uuid,
    },
    WorkAssignment{
        assignment: WorkAssignment
    },
    Terminate {},
}



pub(crate) async fn register(mut con: MultiplexedConnection, args: Cli, mut signals: Signals) -> redis::RedisResult<()> {
    let name = format!("rust-{}-ingester", args.stream);
    let mut state = IngesterState{ service_uuid: Uuid::new_v4(),
        name: name.clone(),
        url: args.ingester_url.clone(),
        streams: vec![args.stream.clone()],
        ..IngesterState::default()};


    let (fwd_reg_connectedworker_s, mut fwd_reg_connectedworker_r) = mpsc::channel(1000);
    let (mut reg_fwd_assignment_s, reg_fwd_assignment_r) = mpsc::channel(1000);

    let forwarder_task = task::spawn(forwarder(args, fwd_reg_connectedworker_s, reg_fwd_assignment_r));

    info!("letting forwarder task start");
    task::sleep(Duration::from_micros(1000_000)).await;
    info!("resume registerer");

    let mut lastid: String = "0".to_string();
    let mut lastev: String = "0".to_string();

    // check if we joined late and discard a lot
    let latest: StreamRangeReply = con.xrevrange_count("dranspose:controller:updates", "+", "-", 1).await.unwrap();
    info!("latest update, starting from: {:?}", latest);
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
                        info!("got update {:?}", update);
                        if Some(update.mapping_uuid) != state.mapping_uuid {
                            //state.mapping_uuid = Some(update.mapping_uuid);
                            info!("resetting config to {}", update.mapping_uuid);
                            reg_fwd_assignment_s.send(QueueWorkAssignment::Start {mapping_uuid: update.mapping_uuid}).await.expect("work");
                            info!("wait for forwarder ready");
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
                debug!("update connected worker {:?}", cw);
                if let Some(cw) = cw {
                    match cw {
                        ForwarderEvent::ConnectedWorker{connected_worker} => {
                            state.connected_workers.insert(connected_worker.service_uuid, connected_worker);
                        },
                        ForwarderEvent::Ready {mapping_uuid}  => {
                            info!("forwarder is ready, change state");
                            if let Some(new_uuid) = pending_uuid {
                                assert_eq!(new_uuid, mapping_uuid) ;
                                state.mapping_uuid = Some(new_uuid);
                                pending_uuid = None;
                            }

                        }
                    };
                }
                else {
                    task::sleep(Duration::from_micros(100_000)).await;
                }
            },
            sig = signals.next().fuse() => {
                debug!("raw signal is {:?}", sig);
                if let Some(signal) = sig {
                    match signal {
                        SIGTERM | SIGINT | SIGQUIT => {
                            // Shutdown the system;
                            info!("signal received");
                            let _: () = con.del(&configkey).await.expect("cannot delete config");
                            info!("deleted config, terminate");
                            reg_fwd_assignment_s.send(QueueWorkAssignment::Terminate {}).await.expect("cannot terminate");
                            break;
                        },
                        _ => unreachable!(),
                    }
                }
            },
        };

        //


    }

    forwarder_task.await.expect("forwarder task did not terminate");

    info!("register terminated");

    Ok(())
}