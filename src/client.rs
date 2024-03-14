use std::sync::Arc;

use parser::RedisValueParser;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task;

use crate::command::{RedisTcpStreamReadExt, RedisTcpStreamWriteExt};
use crate::parser;
use crate::redis::Redis;
use crate::worker::WorkerMessage;

use crate::value::RedisValue;

pub async fn client_process(
    redis: Redis,
    mut client: TcpStream,
    mut client_receiver: Receiver<RedisValue>,
    client_sender: Sender<RedisValue>,
    worker_sender: Sender<WorkerMessage>,
) {
    let client_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string();
    let (mut reader, mut writer) = client.split();

    // for sending the response back to the client
    let response_task: task::JoinHandle<()> = task::spawn(async move {
        loop {
            let response = if let Some(v) = client_receiver.recv().await {
                v
            } else {
                break;
            };
            println!(
                "client received response and now send to client: {:?}",
                response
            );

            writer.write_value(&response).await;
        }
    });

    let mut parser = RedisValueParser::new();

    loop {
        let command = match reader.read_command(&mut parser).await {
            Ok(command) => command,
            Err(e) => panic!("{}", e),
        };
        if let Some(command) = command {
            worker_sender.send(WorkerMessage {
                command,
                client_id: None,
                responser: Some(client_sender.clone()),
            });
        }
        task::yield_now().await;
    }

    response_task.await.unwrap();
}
