use std::io::ErrorKind;

use parser::RedisValueParser;
use tokio::sync::mpsc::Sender;
use tokio::task;

use crate::command::{RedisTcpStreamReadExt, RedisTcpStreamWriteExt};
use crate::parser;
use crate::redis::Redis;
use crate::worker::WorkerMessage;

async fn reply_to_client(redis: Redis, client_id: String) {
    loop {
        let channel = {
            let channels = redis.channels.read().await;
            channels.get(&client_id).unwrap().clone()
        };
        let mut reader = {
            let channel = channel.read().await;
            channel.reader.clone()
        };

        let response = if let Some(v) = reader.lock().await.recv().await {
            v
        } else {
            break;
        };
        println!(
            "client received response and now send to client: {:?}",
            response
        );
        let client = {
            let clients = redis.clients.read().await;
            let client = clients.get(&client_id).unwrap();
            client.clone()
        };
        {
            let mut client = client.write().await;
            let (_, mut writer) = client.split();
            let _ = writer.write_value(&response).await;
        }
    }
    println!("client write for {} finished", client_id);
}

async fn read_from_client(
    redis: Redis,
    client_id: String,
    worker_sender: Sender<WorkerMessage>,
) -> Result<(), std::io::Error> {
    let mut parser = RedisValueParser::new();

    loop {
        let client = {
            println!("try to get client in {}", client_id);
            let clients = redis.clients.read().await;
            let client = clients.get(&client_id).unwrap();
            client.clone()
        };

        let command = {
            let mut client = client.write().await;
            let (mut reader, _) = client.split();
            println!("try to read command in {}", client_id);

            match reader.read_command(&mut parser).await {
                Ok(command) => {
                    if let Some(command) = command {
                        command
                    } else {
                        break;
                    }
                }
                Err(e) => match e.kind() {
                    ErrorKind::ConnectionAborted => {
                        break;
                    }
                    _ => return Err(e),
                },
            }
        };
        {
            println!("command is sending to worker: {:?}", command);
            let channel = {
                let channels = redis.channels.read().await;
                channels.get(&client_id).unwrap().clone()
            };
            let channel = channel.read().await;
            worker_sender
                .send(WorkerMessage {
                    command,
                    client_id: Some(client_id.clone()),
                    responser: Some(channel.writer.clone()),
                })
                .await
                .unwrap();
        }
        task::yield_now().await;
    }
    println!("client read for {} finished", client_id);
    Ok(())
}

pub async fn client_process(redis: Redis, client_id: String, worker_sender: Sender<WorkerMessage>) {
    println!("client process for {} started. ", client_id);
    // for sending the response back to the client
    let reply_to_client_task = task::spawn(reply_to_client(redis.clone(), client_id.clone()));
    read_from_client(redis.clone(), client_id.clone(), worker_sender)
        .await
        .unwrap();
    reply_to_client_task.abort();
    reply_to_client_task.await;
    {
        let mut clients = redis.clients.write().await;
        clients.remove(&client_id);
    }
    {
        let mut channels = redis.channels.write().await;
        channels.remove(&client_id);
    }
    println!("client {} finished", client_id);
}
