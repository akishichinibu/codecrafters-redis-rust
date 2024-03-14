mod client;
mod command;
mod parser;
mod redis;
mod replica;
mod utilities;
mod value;
mod worker;

use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::{mpsc, RwLock};
use tokio::task;

use client::client_process;
use worker::worker_process;

use crate::redis::{ClientHandler, Redis};

use crate::replica::handle_replica_handshake;
use crate::value::RedisValue;
use crate::worker::WorkerMessage;

pub async fn launch(redis: Redis) {
    let listener = TcpListener::bind(redis.clone().host()).await.unwrap();
    println!("main process launched; {}", redis.clone().host());

    // launch worker
    let (worker_sender, worker_receiver) = mpsc::channel::<WorkerMessage>(128);
    let worker = task::spawn(worker_process(redis.clone(), worker_receiver));

    // handle handshake for replica
    if let Some(r) = redis.config.get_replica_of() {
        task::spawn(handle_replica_handshake(redis, worker_sender));
    }

    loop {
        match listener.accept().await {
            Ok((client, addr)) => {
                println!("accepted connection from {:?}", addr);
                let client_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string();
                let (client_sender, client_receiver) = mpsc::channel::<RedisValue>(128);
                {
                    let client_handler = ClientHandler {
                        id: client_id,
                        writer: Arc::new(RwLock::new(client_sender)),
                        reader: Arc::new(RwLock::new(client_receiver)),
                    };
                    let mut handlers = redis.handlers.write().await;
                    handlers.insert(client_id, client_handler);
                }
                // launch client processor
                task::spawn(client_process(
                    redis.clone(),
                    client,
                    client_receiver,
                    client_sender,
                    worker_sender.clone(),
                ));
            }
            Err(e) => {
                println!("accept error: {:?}", e);
                worker.abort();
                break;
            }
        }
    }

    worker.await.unwrap();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let redis: Redis = Redis::new();
    launch(redis).await;
    Ok(())
}
