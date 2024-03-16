use crate::command::{RedisCommand, RedisTcpStreamReadExt, RedisTcpStreamWriteExt};
use crate::parser::RedisValueParser;
use crate::redis::Redis;
use crate::value::RedisValue;
use crate::worker::WorkerMessage;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
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
) -> Result<((OwnedReadHalf, OwnedWriteHalf), RedisValueParser), std::io::Error> {
    let (master_host, master_port) = if let Some(c) = redis.clone().config.get_replica_of() {
        c
    } else {
        panic!();
    };

    let master_url = format!("{}:{}", master_host, master_port);

    let connection = match TcpStream::connect(&master_url).await {
        Ok(c) => c,
        Err(e) => return Err(e),
    };

    let (mut reader, mut writer) = connection.into_split();
    let mut parser = RedisValueParser::new();

    println!("connection to master {} success", master_url);
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
    reader.read_rdb(&mut parser).await.unwrap();
    Ok(((reader, writer), parser))
}

// read the command from master node and send them to the worker node
pub async fn listen_to_master_progate(
    _redis: Redis,
    connection: (OwnedReadHalf, OwnedWriteHalf),
    mut parser: RedisValueParser,
    worker_sender: Sender<WorkerMessage>,
) -> Result<(), std::io::Error> {
    println!("[replica progate] start to listen to master node");
    let (mut reader, _) = connection;

    loop {
        let command = match reader.read_command(&mut parser).await {
            Ok(command) => command,
            Err(e) => return Err(e),
        };
        println!(
            "[replica] receive a progate commmand from master: {:?}",
            command
        );

        if let Some(command) = command {
            let message = WorkerMessage {
                command: command.clone(),
                client_id: None,
                responser: None,
            };
            worker_sender.send(message).await.unwrap();
            println!("[replica] send command to replica worker: {:?}", command);
        }
    }
}
