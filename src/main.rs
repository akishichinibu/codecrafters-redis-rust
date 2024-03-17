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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, RwLock};
use tokio::task;

use client::client_process;
use worker::worker_process;

use crate::client::ClientChannel;
use crate::redis::Redis;

use crate::replica::{handle_replica_handshake, listen_to_master_progate};
use crate::worker::WorkerMessage;

fn get_client_id(client: &TcpStream) -> String {
    let addr = client.peer_addr().unwrap();
    let mut hasher = DefaultHasher::new();
    addr.hash(&mut hasher);
    let id = hasher.finish().to_string();
    if id.len() < 40 {
        let padding: String = repeat('0').take(40 - id.len()).collect();
        id + &padding
    } else {
        id.as_str()[..40].into()
    }
}

pub async fn launch(redis: Redis) {
    let running = Arc::new(AtomicBool::new(true));
    let host = redis.host();
    let listener = TcpListener::bind(host.clone())
        .await
        .expect(&format!("unable to launch service in {}", host));
    println!("main process launched; {}", host);

    // launch worker
    let (worker_sender, worker_receiver) = mpsc::channel::<WorkerMessage>(128);
    let worker = task::spawn(worker_process(redis.clone(), worker_receiver));

    // handle handshake for replica
    let replica_handler = if let Some((master_host, master_port)) = redis.config.get_replica_of() {
        // try to handshake
        println!(
            "current node is a replica node of {}:{}, try to handshake",
            master_host, master_port
        );
        let (connection, parser) = handle_replica_handshake(redis.clone())
            .await
            .expect(&format!(
                "handshake with {}:{} failed",
                master_host, master_port
            ));
        // successed, start to listen to master progration
        println!(
            "handshake with {}:{} success, launch progate thread",
            master_host, master_port
        );
        let task: task::JoinHandle<Result<(), std::io::Error>> = task::spawn(
            listen_to_master_progate(redis.clone(), connection, parser, worker_sender.clone()),
        );
        Some(task)
    } else {
        None
    };

    while running.load(Ordering::SeqCst) {
        match listener.accept().await {
            Err(e) => {
                println!("unable to get client: {:?}", e);
            }
            Ok((client, addr)) => {
                let client_id = get_client_id(&client);
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
        }
    }

    worker.abort();

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
