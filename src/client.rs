use std::io::ErrorKind;
use std::sync::Arc;

use base64::write;
use parser::RedisValueParser;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::{Mutex, RwLock};
use tokio::{select, task};

use crate::command::{RedisCommand, RedisTcpStreamReadExt, RedisTcpStreamWriteExt};
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

// async fn reply_to_client(redis: Redis, client_id: String) {
//     loop {
//         let channel = {
//             let channels = redis.channels.read().await;
//             channels.get(&client_id).unwrap().clone()
//         };
//         let to_client_reader = {
//             let channel = channel.read().await;
//             channel._to_client_receiver.clone()
//         };
//         let mut reader = to_client_reader.write().await;
//         let response = if let Some(v) = reader.recv().await {
//             v
//         } else {
//             break;
//         };
//         println!(
//             "client received response and now send to client: {:?}",
//             response
//         );
//         println!("1");
//         let client = {
//             let clients = redis.clients.read().await;
//             let client = clients.get(&client_id).unwrap();
//             client.clone()
//         };
//         println!("2");
//         {
//             let mut client = client.write().await;
//             let (_, mut writer) = client.split();
//             let _ = writer.write_value(&response).await;
//             writer.flush().await;
//             println!("value {:?} has been writed to clinet", response);
//         }
//         println!("3");
//     }
//     println!("client write for {} finished", client_id);
// }

// async fn read_from_client(redis: Redis, stream: TcpStream) -> Result<(), std::io::Error> {
//     let mut parser = RedisValueParser::new();

//     loop {
//         let command = {
//             let (mut reader, _) = stream.split();

//             match reader.read_command(&mut parser).await {
//                 Ok(command) => {
//                     if let Some(command) = command {
//                         command
//                     } else {
//                         break;
//                     }
//                 }
//                 Err(e) => match e.kind() {
//                     ErrorKind::ConnectionAborted => {
//                         break;
//                     }
//                     _ => return Err(e),
//                 },
//             }
//         };
//         {
//             println!("command is sending to worker: {:?}", command);
//             let channel = {
//                 let channels = redis.channels.read().await;
//                 channels.get(&client_id).unwrap().clone()
//             };
//             let channel = channel.read().await;
//             let from_client_sender = channel._from_client_sender.clone();
//             let writer = from_client_sender.lock().await;
//             writer.send((&command).into()).await.unwrap();
//         }
//         task::yield_now().await;
//     }
//     println!("client read for {} finished", client_id);
//     Ok(())
// }

pub async fn client_process(
    redis: Redis,
    client_id: String,
    client: TcpStream,
    worker_sender: Sender<WorkerMessage>,
) {
    println!("client process for {} started. ", client_id);
    // for sending the response back to the client
    // let reply_to_client_task = task::spawn(reply_to_client(redis.clone(), client_id.clone()));
    // let read_from_client_task = task::spawn(read_from_client(redis.clone(), client_id.clone()));

    // let client = {
    //     let redis = redis.clone();
    //     let clients = redis.clients.read().await;
    //     let client = clients.get(&client_id).unwrap();
    //     client.clone()
    // };

    // let client = client.lock().await;
    let (mut reader, mut writer) = client.into_split();

    // read from tcp stream and put the value into the channel
    let _redis = redis.clone();
    let _client_id = client_id.clone();
    task::spawn(async move {
        let mut parser = RedisValueParser::new();
        loop {
            let from_client = reader.read_command(&mut parser).await;
            match from_client {
                Ok(command) => {
                    if let Some(command) = command {
                        let channel = {
                            let channels = _redis.channels.read().await;
                            channels.get(&_client_id).unwrap().clone()
                        };
                        let from_client_sender = {
                            let channel = channel.read().await;
                            channel._from_client_sender.clone()
                        };
                        let from_client_sender = from_client_sender.lock().await;
                        from_client_sender.send((&command).into()).await;
                    } else {
                        break;
                    }
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
    task::spawn(async move {
        loop {
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
                break;
            };
            let _ = writer.write_value(&response).await;
            writer.flush().await;
            println!("value {:?} has been writed to clinet", response);
        }
    });

    let redis = redis.clone();
    let _client_id = client_id.clone();
    loop {
        let channel = {
            let channels = redis.channels.read().await;
            channels.get(&client_id.clone()).unwrap().clone()
        };

        let from_client_receiver = {
            let channel = channel.read().await;
            channel.from_client_receiver.clone()
        };
        let mut from_client_receiver = from_client_receiver.lock().await;

        // let to_client_receiver = {
        //     let channel = channel.read().await;
        //     channel._to_client_receiver.clone()
        // };
        // let mut to_client_receiver = to_client_receiver.write().await;

        let from_client = from_client_receiver.recv().await;

        if let Some(value) = from_client {
            worker_sender
                .send(WorkerMessage {
                    command: value.try_into().unwrap(),
                    client_id: Some(client_id.clone()),
                    responser: Some(channel.read().await.to_client_sender.clone()),
                })
                .await
                .unwrap();
        }

        // select! {
        //     from_client = reader.read_command(&mut parser) => {
        //         match from_client {
        //             Ok(command) => {
        //                 if let Some(command) = command {
        //                     let from_client_sender = {
        //                         let channel = channel.read().await;
        //                         channel._from_client_sender.clone()
        //                     };
        //                     let from_client_sender = from_client_sender.lock().await;
        //                     from_client_sender.send((&command).into()).await;
        //                 } else {
        //                     break;
        //                 }
        //             }
        //             Err(e) => match e.kind() {
        //                 ErrorKind::ConnectionAborted => {
        //                     break;
        //                 }
        //                 _ => panic!(""),
        //             }
        //         }
        //     }
        //     to_client = to_client_receiver.recv() => {
        //         let response = if let Some(v) = to_client {
        //             v
        //         } else {
        //             break;
        //         };
        //         let _ = writer.write_value(&response).await;
        //         writer.flush().await;
        //         println!("value {:?} has been writed to clinet", response);
        //     }
        //     from_client = from_client_receiver.recv() => {
        //         if let Some(value) = from_client {
        //             worker_sender.clone().send(WorkerMessage {
        //                 command: value.try_into().unwrap(),
        //                 client_id: Some(client_id.clone()),
        //                 responser: Some(channel.read().await.to_client_sender.clone()),
        //             })
        //             .await
        //             .unwrap();

        //         }
        //     }
        // }

        // select! {
        //     from_client = from_client_receiver.recv() => {
        //         if let Some(value) = from_client {
        //             worker_sender.clone().send(WorkerMessage {
        //                 command: value.try_into().unwrap(),
        //                 client_id: Some(client_id.clone()),
        //                 responser: Some(channel.read().await.to_client_sender.clone()),
        //             })
        //             .await
        //             .unwrap();

        //         }
        //     }
        //     to_client = to_client_receiver.recv() => {
        //         let mut client = client.write().await;
        //         let (_, mut writer) = client.split();
        //         let _ = writer.write_value(&response).await;
        //         writer.flush().await;
        //         println!("value {:?} has been writed to clinet", response);
        //     }
        // }
    }

    // reply_to_client_task.abort();
    // reply_to_client_task.await;
    // read_from_client_task.abort();
    // read_from_client_task.await;
    {
        let mut channels = redis.channels.write().await;
        channels.remove(&client_id);
    }
    println!("client {} finished", client_id);
}
