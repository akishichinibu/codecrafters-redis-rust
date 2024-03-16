use std::sync::Arc;

use command::RedisCommand;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tokio::task;

use crate::redis::{Redis, StoreItem};
use crate::replica::ReplicationInfo;
use crate::{command, utilities};

use crate::value::RedisValue;

#[derive(Debug)]
pub struct WorkerMessage {
    pub command: RedisCommand,
    pub client_id: Option<String>,
    pub responser: Option<Arc<RwLock<Sender<RedisValue>>>>,
    pub offset: usize,
}

pub async fn worker_process(redis: Redis, mut receiver: Receiver<WorkerMessage>) {
    println!("[worker] process launched; {}", redis.host());

    loop {
        let message = if let Some(v) = receiver.recv().await {
            v
        } else {
            continue;
        };
        println!("[worker] messaged received: {:?}", message);
        let command: RedisCommand = message.command.clone();

        let response = match message.command {
            RedisCommand::Ping => vec![RedisValue::simple_string("PONG")],
            RedisCommand::Echo(value) => vec![RedisValue::BulkString(Some(value))],
            RedisCommand::Get(key) => {
                let key: String = (&key).into();
                let store = redis.store.read().await;
                println!("[worker] store: {:?}", store);
                match store.get(&key) {
                    Some(item) => {
                        if item.expired_at == 0 || item.expired_at >= utilities::now() {
                            vec![item.value.clone()]
                        } else {
                            drop(store);
                            let mut store = redis.store.write().await;
                            store.remove(&key);
                            vec![RedisValue::null_bulk_string()]
                        }
                    }
                    None => vec![RedisValue::null_bulk_string()],
                }
            }
            RedisCommand::Info(_) => vec![ReplicationInfo {
                role: match redis.config.clone().get_replica_of() {
                    Some((_, _)) => "slave".to_string(),
                    None => "master".to_string(),
                },
                replica_id: message.client_id.unwrap(),
            }
            .into()],
            RedisCommand::Replconf(v1, v2) => {
                let key: String = (&v1).into();
                let key = key.to_lowercase();
                match key.as_str() {
                    "getack" => {
                        vec![RedisValue::Array(vec![
                            RedisValue::bulk_string("replconf"),
                            RedisValue::bulk_string("ack"),
                            RedisValue::bulk_string(message.offset.to_string().as_str()),
                        ])]
                    }
                    _ => vec![RedisValue::simple_string("OK")],
                }
            }
            RedisCommand::Psync(_, _) => {
                let id = message.client_id.unwrap();
                let response = format!("FULLRESYNC {} 0", id);
                {
                    let mut replicas = redis.replicas.write().await;
                    replicas.insert(id);
                    println!("replicas: {:?}", replicas);
                }
                vec![
                    RedisValue::simple_string(response.as_str()),
                    RedisValue::Rdb(
                        base64::decode("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==").unwrap(),
                    ),
                ]
            }
            RedisCommand::Set(key, value, px) => {
                let key = String::from_utf8(key.data.to_vec()).unwrap();
                let value = RedisValue::BulkString(Some(value));
                let expired_at = match px {
                    None => 0,
                    Some(px) => px + utilities::now(),
                };
                // update store
                {
                    let mut store = redis.store.write().await;
                    store.insert(key, StoreItem { value, expired_at });
                    println!("[worker] store: {:?}", store);
                }
                // if current node is master node, broadcast the write commmand to all replicas
                if redis.config.get_replica_of() == None {
                    brocast_to_replicas(redis.clone(), command).await.unwrap();
                }
                vec![RedisValue::simple_string("OK")]
            }
        };
        println!("[worker] done. response: {:?}", response);
        if let Some(responser) = message.responser {
            println!(
                "[worker] has a responser, send response back {:?}",
                response
            );
            for r in response {
                let res = responser.read().await;
                res.send(r).await.unwrap();
            }
        }
        task::yield_now().await;
    }
}

pub async fn brocast_to_replicas(redis: Redis, command: RedisCommand) -> Result<(), ()> {
    let replicas = redis.replicas.read().await;
    println!(
        "start to broadcast to replicas({}): {:?}",
        replicas.len(),
        replicas
    );

    for id in replicas.iter() {
        let channel = {
            let channels = redis.channels.read().await;
            let channel = channels.get(id).unwrap();
            channel.clone()
        };
        let to_client_sender = {
            let channel = channel.read().await;
            let writer = channel.to_client_sender.read().await;
            writer.clone()
        };
        to_client_sender.send((&command).into()).await.unwrap();
        println!("broadcast to client {} done, {:?}", id, command);
    }

    println!("broadcast to replicas done: {}", replicas.len());
    Ok(())
}
