use crate::control_plane::{ForwarderEvent, QueueWorkAssignment};
use crate::dranspose::{ConnectedWorker, WorkAssignment};
use crate::{Cli, TimedMultipart};
use async_zmq::{Context, Message, MultipartIter, Router, StreamExt};
use futures::channel::mpsc;
use futures::select;
use futures_util::sink::SinkExt;
use log::{debug, info};
use serde_json::json;
use std::collections::VecDeque;
use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};
use std::vec::IntoIter;
use url::Url;
use uuid::Uuid;

use futures::{
    future::FutureExt, // for `.fuse()`
};
use std::ops::Deref;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq)]
enum ScanState {
    Discard,
    Armed,
    Running,
}

#[derive(Serialize, Deserialize)]
struct Stream1Packet {
    htype: String,
    msg_number: u64,
}

pub(crate) async fn forwarder(
    args: Cli,
    mut forwarder_events_s: mpsc::Sender<ForwarderEvent>,
    mut assignment_r: mpsc::Receiver<QueueWorkAssignment>,
) -> async_zmq::Result<()> {
    info!("started forwarder");
    let url = Url::parse(&args.ingester_url).expect("unparsable ingester url");

    let listenurl = format!("tcp://*:{}", url.port().unwrap());

    let context = Context::new();

    let mut routersock: Router<IntoIter<Message>, Message> = async_zmq::router(&listenurl)?
        .with_context(&context)
        .bind()?;
    routersock
        .as_raw_socket()
        .set_router_mandatory(true)
        .expect("cannot set mandatory");
    info!("connected router socket to {}", &listenurl);

    let mut pullsock = async_zmq::pull(&args.upstream_url)?
        .with_context(&context)
        .connect()?;
    pullsock.as_raw_socket().set_linger(0).expect("no linger");
    info!("connected pull socket to {}", &args.upstream_url);

    let mut asbuf: VecDeque<WorkAssignment> = VecDeque::new();
    let mut pkbuf: VecDeque<TimedMultipart> = VecDeque::new();

    let mut no_events = 0;
    let mut starttime = Instant::now();

    let mut instate = ScanState::Discard;

    forwarder_events_s
        .send(ForwarderEvent::Started {})
        .await
        .expect("cannot send started message");

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

            debug!("header is {}", header.to_string());
            let mymsg = stins;
            debug!("workerlist is created from assignment {:?}", assignment);
            let workerlist = assignment.assignments.get(&args.stream).unwrap();

            if workerlist.len() == 1 {
                let w = workerlist.first().unwrap();
                debug!("send event data to worker {:?}", w);
                let mut payload = vec![Message::from(w), Message::from(&header.to_string())];
                payload.extend(mymsg);

                routersock
                    .send(MultipartIter::from(payload))
                    .await
                    .expect("unable to send");
                debug!(
                    "sending took {} microsec",
                    timed.received.elapsed().as_micros()
                );
            } else {
                //needs copyingg
                for w in workerlist {
                    debug!("copy message to worker {:?}", w);
                    let msgcopy: Vec<Message> = mymsg
                        .iter()
                        .map(|m| Message::from(m.to_vec().clone()))
                        .collect();
                    let mut payload = vec![Message::from(w), Message::from(&header.to_string())];
                    payload.extend(msgcopy);

                    routersock
                        .send(MultipartIter::from(payload))
                        .await
                        .expect("unable to send");
                }
            }
            no_events += 1;
            if no_events % 1000 == 999 {
                let micros = starttime.elapsed().as_micros();
                info!(
                    "{} to {} packets in {} microsecs = {} p/s",
                    no_events - 1000,
                    no_events,
                    micros,
                    1000000000 / micros
                );
                starttime = Instant::now();
            }
            debug!(
                "finished sending out assignment to workers {:?}",
                assignment
            );
        }
        /*if pkbuf.len() > 0 {
            println!("waiting for assignments");
        }*/
        debug!("start forwarder select");
        select! {
            control = assignment_r.next().fuse() => {
                debug!("forwarder got control message {:?}", control);
                match control {
                    Some(QueueWorkAssignment::Start{mapping_uuid}) => {
                        info!("got start {:?}", mapping_uuid);
                        asbuf = VecDeque::new();
                        pkbuf = VecDeque::new();
                        instate = ScanState::Armed;
                        forwarder_events_s.send(ForwarderEvent::Ready{mapping_uuid}).await.expect("must be able to ready");
                        info!("ready sent from fwd");
                    },
                    Some(QueueWorkAssignment::WorkAssignment {assignment}) => {
                        debug!("got assignment");
                        debug!("received wa {:?}", assignment);
                        asbuf.push_front(assignment);
                    },
                    Some(QueueWorkAssignment::Terminate{}) => {
                        info!("to terminate");
                        break;
                    },
                    None => {
                        info!("control message was none");
                        break;
                    }
                };
            },
            msg = pullsock.next().fuse() => {
                if let Some(data) = msg {
                    let stins: Vec<Message> = data?;
                    debug!("got from pull {:?}", stins[0]);
                    let first_part = stins[0].as_str();
                    if let Some(header_string) = first_part {
                        let update: serde_json::Result<Stream1Packet> = serde_json::from_str(&header_string);
                        if let Ok(header) = update {
                            if header.htype == "header" && instate == ScanState::Armed {
                                instate = ScanState::Running;
                            }
                            if instate == ScanState::Running {
                                let now = Instant::now();
                                pkbuf.push_front(TimedMultipart{multipart: stins, received:now});
                            }
                            if header.htype == "series_end" {
                                instate = ScanState::Discard;
                            }
                        }
                    }
                }
            },
            msg = routersock.next().fuse() => {
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
                    forwarder_events_s.send(ForwarderEvent::ConnectedWorker {connected_worker:cw}).await.expect("could not send message");
                }

            },
            complete => break,
        };
    }
    info!("closing sockets");
    routersock.close().await.expect("cannot close router");
    info!("forwarder terminated");

    Ok(())
}
