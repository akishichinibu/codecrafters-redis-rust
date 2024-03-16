mod client;
mod command;
mod parser;
mod redis;
mod replica;
mod utilities;
mod value;
mod worker;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::iter::repeat;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::{mpsc, RwLock};
use tokio::task;

use client::client_process;
use worker::worker_process;

use crate::client::ClientChannel;
use crate::redis::Redis;

use crate::replica::{handle_replica_handshake, listen_to_master_progate};
use crate::worker::WorkerMessage;

pub async fn launch(redis: Redis) {
    let listener = TcpListener::bind(redis.clone().host()).await.unwrap();
    println!("main process launched; {}", redis.clone().host());

    // launch worker
    let (worker_sender, worker_receiver) = mpsc::channel::<WorkerMessage>(128);
    let worker = task::spawn(worker_process(redis.clone(), worker_receiver));

    // handle handshake for replica
    let replica_handler = if let Some(_) = redis.config.get_replica_of() {
        println!("current node is a replica node, try to handshake");
        let (connection, parser) = handle_replica_handshake(redis.clone()).await.unwrap();
        println!("handshake success, launch progate thread");

        Some(task::spawn(listen_to_master_progate(
            redis.clone(),
            connection,
            parser,
            worker_sender.clone(),
        )))
    } else {
        None
    };

    loop {
        match listener.accept().await {
            Ok((client, addr)) => {
                let client_id = {
                    let mut hasher = DefaultHasher::new();
                    addr.hash(&mut hasher);
                    let id = hasher.finish().to_string();
                    if id.len() < 40 {
                        let padding: String = repeat('0').take(40 - id.len()).collect();
                        id + &padding
                    } else {
                        id.as_str()[..40].into()
                    }
                };
                println!(
                    "[main] accepted connection from {:?}, id: {}",
                    addr, client_id
                );

                {
                    let mut channels = redis.channels.write().await;
                    channels.insert(
                        client_id.clone(),
                        Arc::new(RwLock::new(ClientChannel::new())),
                    );
                }

                // launch client processor
                println!("[main] client {} processor launched", client_id);
                task::spawn(client_process(
                    redis.clone(),
                    client_id.clone(),
                    client,
                    worker_sender.clone(),
                ));
            }
            Err(e) => {
                println!("accept error: {:?}", e);
                worker.abort();
                break;
            }
        }
        task::yield_now().await;
    }

    if let Some(replica_handler) = replica_handler {
        replica_handler.abort();
        replica_handler.await.unwrap().unwrap();
    }

    worker.await.unwrap();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let redis: Redis = Redis::new();
    launch(redis).await;
    Ok(())
}
