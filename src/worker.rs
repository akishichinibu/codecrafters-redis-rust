use command::RedisCommand;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task;

use crate::redis::{Redis, StoreItem};
use crate::replica::{brocast_to_replicas, ReplicationInfo};
use crate::{command, utilities};

use crate::value::RedisValue;

pub struct WorkerMessage {
    pub command: RedisCommand,
    pub client_id: Option<String>,
    pub responser: Option<Sender<RedisValue>>,
}

pub async fn worker_process(redis: Redis, receiver: Receiver<WorkerMessage>) {
    println!("worker process launched; {}", redis.host());
    let mut receiver = receiver;

    loop {
        let message = if let Some(v) = receiver.recv().await {
            v
        } else {
            continue;
        };
        println!("worker received command: {:?}", message.command);
        let command: RedisCommand = message.command.clone();
        let command2: RedisCommand = message.command.clone();

        let response = match command {
            RedisCommand::Ping => vec![RedisValue::simple_string("PONG")],
            RedisCommand::Echo(value) => vec![RedisValue::BulkString(Some(value))],
            RedisCommand::Get(key) => {
                let mut store = redis.store.write().await;
                let key = String::from_utf8(key.data.to_vec()).unwrap();
                match store.get(&key) {
                    Some(item) => {
                        if item.expired_at == 0 || item.expired_at >= utilities::now() {
                            vec![item.value.clone()]
                        } else {
                            store.remove(&key);
                            vec![RedisValue::null_bulk_string()]
                        }
                    }
                    None => vec![RedisValue::null_bulk_string()],
                }
            }
            RedisCommand::Info => vec![ReplicationInfo {
                role: match redis.config.clone().get_replica_of() {
                    Some((_, _)) => "slave".to_string(),
                    None => "master".to_string(),
                },
                replica_id: message.client_id.unwrap(),
            }
            .into()],
            RedisCommand::Replconf(_, _) => vec![RedisValue::simple_string("OK")],
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
                }
                // if current node is master node, broadcast the write commmand to all replicas
                if redis.clone().config.get_replica_of() == None {
                    let r = redis.clone();
                    task::spawn(brocast_to_replicas(r, command2));
                }
                vec![RedisValue::simple_string("OK")]
            }
        };
        if let Some(responser) = message.responser {
            for r in response {
                responser.send(r).await.unwrap()
            }
        }
    }
}
