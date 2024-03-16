use std::io::ErrorKind;
use std::sync::Arc;

use parser::RedisValueParser;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::{Mutex, RwLock};
use tokio::task;

use crate::command::{RedisTcpStreamReadExt, RedisTcpStreamWriteExt};
use crate::parser;
use crate::redis::Redis;
use crate::value::RedisValue;
use crate::worker::WorkerMessage;

#[derive(Debug)]
pub struct ClientChannel {
    pub from_client_receiver: Arc<Mutex<Receiver<RedisValue>>>,
    pub to_client_sender: Arc<RwLock<Sender<RedisValue>>>,

    _from_client_sender: Arc<Mutex<Sender<RedisValue>>>,
    _to_client_receiver: Arc<RwLock<Receiver<RedisValue>>>,
}

impl ClientChannel {
    pub fn new() -> ClientChannel {
        let (from_client_sender, from_client_receiver) = mpsc::channel::<RedisValue>(128);
        let (to_client_sender, to_client_receiver) = mpsc::channel::<RedisValue>(128);
        ClientChannel {
            from_client_receiver: Arc::new(Mutex::new(from_client_receiver)),
            to_client_sender: Arc::new(RwLock::new(to_client_sender)),

            _from_client_sender: Arc::new(Mutex::new(from_client_sender)),
            _to_client_receiver: Arc::new(RwLock::new(to_client_receiver)),
        }
    }
}

pub async fn client_process(
    redis: Redis,
    client_id: String,
    client: TcpStream,
    worker_sender: Sender<WorkerMessage>,
) {
    println!("[client: {}] process started. ", client_id);
    let (mut reader, mut writer) = client.into_split();
    let is_done = Arc::new(RwLock::new(false));

    // read from tcp stream and put the value into the channel
    let _redis = redis.clone();
    let _client_id = client_id.clone();
    let _is_done = is_done.clone();
    let read_from_client_task = task::spawn(async move {
        println!("[client: {}] start to read from stream", _client_id);
        let mut parser = RedisValueParser::new();
        loop {
            let from_client = reader.read_command(&mut parser).await;
            match from_client {
                Ok((None, _)) => {
                    let mut v = _is_done.write().await;
                    *v = true;
                }
                Ok((Some(command), _)) => {
                    let channel = {
                        let channels = _redis.channels.read().await;
                        channels.get(&_client_id).unwrap().clone()
                    };
                    let from_client_sender = {
                        let channel = channel.read().await;
                        channel._from_client_sender.clone()
                    };
                    let from_client_sender = from_client_sender.lock().await;
                    from_client_sender.send((&command).into()).await.unwrap();
                }
                Err(e) => match e.kind() {
                    ErrorKind::ConnectionAborted => {
                        break;
                    }
                    _ => panic!(""),
                },
            };
        }
    });

    // read from to_client_receiver channel and write the value to tcp stream
    let _redis = redis.clone();
    let _client_id = client_id.clone();
    let _is_done = is_done.clone();
    let write_to_client_task = task::spawn(async move {
        loop {
            if *_is_done.read().await {
                println!("[client: {}] has done. exited. ", _client_id);
                break;
            }
            let channel = {
                let channels = _redis.channels.read().await;
                channels.get(&_client_id).unwrap().clone()
            };
            let to_client_receiver = {
                let channel = channel.read().await;
                channel._to_client_receiver.clone()
            };
            let mut to_client_receiver = to_client_receiver.write().await;
            let response = to_client_receiver.recv().await;
            let response = if let Some(v) = response {
                v
            } else {
                to_client_receiver.close();
                break;
            };
            writer.write_value(&response).await.unwrap();
            writer.flush().await.unwrap();
            println!(
                "[client: {}] value {:?} has been writed to client",
                _client_id, response
            );
        }
    });

    let redis = redis.clone();
    let _client_id = client_id.clone();
    let _is_done = is_done.clone();
    loop {
        if *_is_done.read().await {
            println!("[client: {}] has done. exited. ", _client_id);
            break;
        }
        let channel = {
            let channels = redis.channels.read().await;
            channels.get(&client_id.clone()).unwrap().clone()
        };

        let from_client_receiver = {
            let channel = channel.read().await;
            channel.from_client_receiver.clone()
        };
        let mut from_client_receiver = from_client_receiver.lock().await;
        let from_client = from_client_receiver.recv().await;

        if let Some(value) = from_client {
            worker_sender
                .send(WorkerMessage {
                    command: value.try_into().unwrap(),
                    client_id: Some(client_id.clone()),
                    responser: Some(channel.read().await.to_client_sender.clone()),
                    offset: 0,
                })
                .await
                .unwrap();
        } else {
            break;
        }
    }
    write_to_client_task.abort();
    read_from_client_task.abort();
    let _ = write_to_client_task.await;
    let _ = read_from_client_task.await;
    {
        let mut channels = redis.channels.write().await;
        channels.remove(&client_id);
    }
    println!("[client: {}] finished", client_id);
}
