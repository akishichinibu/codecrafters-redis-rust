use std::sync::Arc;

use command::RedisCommand;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use tokio::task::{self};

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

macro_rules! respond {
    ($responser:ident, $response:expr) => {{
        if let Some($responser) = ($responser) {
            let responser = ($responser).read().await;
            for m in ($response).iter() {
                match responser.send(m.clone()).await {
                    Ok(_) => {}
                    Err(e) => panic!("{:?}", e),
                }
            }
            println!("[worker] send response: {:?}", $response);
        }
    }};
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

        let client_id = message.client_id.clone();
        let is_replica = {
            let replicas = redis.replicas.read().await;
            replicas.contains_key(&client_id.clone().unwrap_or_default())
        };
        let responser = if let Some(responser) = message.responser {
            println!("[worker][{:?}] has a responser", client_id);
            Some(responser)
        } else {
            None
        };

        match message.command {
            RedisCommand::Ping => {
                respond!(responser, vec![RedisValue::simple_string("PONG")]);
            }
            RedisCommand::Echo(value) => {
                respond!(responser, vec![RedisValue::BulkString(Some(value.clone()))])
            }
            RedisCommand::Get(key) => {
                let key: String = (&key).into();
                let store = redis.store.read().await;
                println!("[worker][{:?}] store: {:?}", client_id, store);
                let response = match store.get(&key) {
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
                };
                respond!(responser, response)
            }
            RedisCommand::Info(_) => {
                let value: RedisValue = ReplicationInfo {
                    role: match redis.config.clone().get_replica_of() {
                        Some((_, _)) => "slave".to_string(),
                        None => "master".to_string(),
                    },
                    replica_id: message.client_id.unwrap(),
                }
                .into();
                respond!(responser, vec![value.clone()]);
            }
            RedisCommand::Replconf(v1, v2) => {
                let key: String = (&v1).into();
                let key = key.to_lowercase();
                let response = match key.as_str() {
                    "getack" => {
                        vec![RedisValue::Array(vec![
                            RedisValue::bulk_string("replconf"),
                            RedisValue::bulk_string("ack"),
                            RedisValue::bulk_string(message.offset.to_string().as_str()),
                        ])]
                    }
                    "ack" => {
                        let mut replicas = redis.replicas.write().await;
                        let v2s: String = (&v2).into();
                        let offset = v2s.parse().unwrap();
                        replicas.insert(client_id.clone().unwrap(), offset);
                        println!(
                            "replicas: {:?}, replica {:?} offset update to {}",
                            replicas, client_id, offset
                        );
                        vec![]
                    }
                    "capa" => vec![RedisValue::simple_string("OK")],
                    _ => vec![RedisValue::simple_string("OK")],
                };
                respond!(responser, response);
            }
            RedisCommand::Psync(_, _) => {
                let id = message.client_id.unwrap();
                let response = format!("FULLRESYNC {} 0", id);
                {
                    let mut replicas = redis.replicas.write().await;
                    replicas.insert(id, 0);
                    println!("replicas: {:?}", replicas);
                }
                respond!(responser, vec![
                            RedisValue::simple_string(response.as_str()),
                            RedisValue::Rdb(
                                #[allow(warnings)]
                                base64::decode("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==").unwrap(),
                            ),
                        ]);
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
                    println!("[worker][{:?}] store: {:?}", client_id, store);
                }
                // if current node is master node, broadcast the write commmand to all replicas
                if redis.config.get_replica_of() == None {
                    brocast_to_replicas(redis.clone(), command).await.unwrap();
                }
                respond!(responser, vec![RedisValue::simple_string("OK")]);
            }
            RedisCommand::Wait(number, timeout) => {
                let started_at = utilities::now();
                let _redis = redis.clone();
                let _client_id = message.client_id.clone();
                task::spawn(async move {
                    println!(
                        "[worker][{:?}][wait] wait started for {} ms at {}",
                        _client_id,
                        timeout,
                        utilities::now(),
                    );
                    loop {
                        let replica_number = {
                            let replicas = _redis.replicas.read().await;
                            replicas.len() as u64
                        };
                        let diff = utilities::now() - started_at;
                        if replica_number >= number || diff > timeout {
                            break;
                        }
                        {
                            let channels = _redis.channels.read().await;
                            if let Some(ref client_id) = message.client_id {
                                if !channels.contains_key(client_id) {
                                    println!("[worker] the current client {} has down", client_id);
                                    return;
                                }
                            }
                        };
                    }
                    let replica_number = {
                        let replicas = _redis.replicas.read().await;
                        replicas.len() as u64
                    };
                    respond!(
                        responser,
                        vec![RedisValue::Integer(replica_number as usize)]
                    );
                    println!(
                        "[worker][{:?}][wait] wait done at {} for {} ms",
                        _client_id,
                        utilities::now(),
                        timeout
                    );
                });
            }
            RedisCommand::Select(_) => {
                respond!(responser, vec![RedisValue::simple_string("ok")]);
            }
        };
    }
}

pub async fn brocast_to_replicas(redis: Redis, command: RedisCommand) -> Result<(), ()> {
    let replicas = redis.replicas.read().await;
    println!(
        "[worker] start to broadcast to replicas({}): {:?}",
        replicas.len(),
        replicas
    );

    for (id, _) in replicas.iter() {
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
        println!("[worker] broadcast to client {} done, {:?}", id, command);
    }

    println!("[worker] broadcast to replicas done: {}", replicas.len());
    Ok(())
}
