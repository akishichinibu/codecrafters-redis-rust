use crate::command::{RedisCommand, RedisTcpStreamReadExt, RedisTcpStreamWriteExt};
use crate::parser::RedisValueParser;
use crate::redis::Redis;
use crate::value::RedisValue;
use crate::worker::WorkerMessage;
use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;

pub struct ReplicationInfo {
    pub role: String,
    pub replica_id: String,
}

impl<'a> Into<RedisValue> for ReplicationInfo {
    fn into(self) -> RedisValue {
        let role = format!("role:{}", self.role);
        let master_replid = format!("master_replid:{}", self.replica_id);
        let master_repl_offset = format!("master_repl_offset:{}", 0);
        let content = vec!["# Replication", &role, &master_replid, &master_repl_offset].join("\n");
        RedisValue::bulk_string(content.as_str())
    }
}

pub async fn handle_replica_handshake(
    redis: Redis,
    worker_sender: Sender<WorkerMessage>,
) -> Result<TcpStream, std::io::Error> {
    let (master_host, master_port) = if let Some(c) = redis.clone().config.get_replica_of() {
        c
    } else {
        panic!();
    };

    let master_url = format!("{}:{}", master_host, master_port);

    let mut connection = match TcpStream::connect(&master_url).await {
        Ok(c) => c,
        Err(e) => return Err(e),
    };

    let (mut reader, mut writer) = connection.split();
    let mut parser = RedisValueParser::new();

    writer.write_command(&RedisCommand::Ping).await.unwrap();

    reader.read_value(&mut parser).await.unwrap();

    writer
        .write_command(&RedisCommand::replconf(
            "listening-port",
            redis.clone().config.port.to_string().as_str(),
        ))
        .await
        .unwrap();

    reader.read_value(&mut parser).await.unwrap();

    writer
        .write_command(&RedisCommand::replconf("capa", "psync2"))
        .await
        .unwrap();

    reader.read_value(&mut parser).await.unwrap();

    writer
        .write_command(&RedisCommand::pasync("?", "-1"))
        .await
        .unwrap();

    reader.read_value(&mut parser).await.unwrap();
    // connection.read_rdb().unwrap_or(RedisValue::Rdb(Vec::new()));

    loop {
        let command = match reader.read_command(&mut parser).await {
            Ok(command) => command,
            Err(e) => return Err(e),
        };
        if let Some(command) = command {
            let message = WorkerMessage {
                command,
                client_id: None,
                responser: None,
            };
            worker_sender.send(message).await;
        }
    }
}

pub async fn brocast_to_replicas(redis: Redis, command: RedisCommand) -> Result<(), ()> {
    let replicas = redis.replicas.read().await;
    let handlers = redis.handlers.read().await;
    let value: RedisValue = command.into();

    for id in replicas.iter() {
        let handler = handlers.get(id).unwrap();
        let writer = handler.writer.write().await;
        writer.send(value.clone());
        println!("broadcast to port done {}, {:?}", 1111, value);
    }
    Ok(())
}
