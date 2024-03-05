use crate::command::RedisCommand;
use crate::info::ReplicationInfo;
use crate::r#type::RedisType;
use crate::{config, utilities, HandlerError};
use once_cell::sync::Lazy;
use std::borrow::Cow;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Mutex;

static STORE: Lazy<Mutex<HashMap<String, (RedisType, u64)>>> = Lazy::new(|| {
    let m = HashMap::new();
    Mutex::new(m)
});

pub struct StreamHandler {
    stream: TcpStream,
    buffer: [u8; 1024],
}

impl<'a> StreamHandler {
    pub fn new(stream: TcpStream) -> Self {
        StreamHandler {
            stream,
            buffer: [0; 1024],
        }
    }

    pub(crate) fn read(&mut self) -> Result<Option<&[u8]>, HandlerError> {
        match self.stream.read(&mut self.buffer) {
            Ok(0) => Ok(None),
            Ok(n) => {
                println!("received buffer length: {}", n);
                Ok(Some(&self.buffer[0..n]))
            }
            Err(e) => Err(HandlerError::Io(e.to_string())),
        }
    }

    pub(crate) fn read_command(&mut self) -> Result<Option<RedisCommand>, HandlerError> {
        match self.read() {
            Ok(b) => match b {
                Some(b) => {
                    let c = RedisCommand::try_from(b);
                    match c {
                        Ok(c) => Ok(Some(c)),
                        Err(e) => Err(HandlerError::Commond(e)),
                    }
                }
                None => Ok(None),
            },
            Err(e) => Err(e),
        }
    }

    pub(crate) fn read_type(&mut self) -> Result<Option<RedisType>, HandlerError> {
        match self.read() {
            Ok(b) => match b {
                Some(b) => {
                    let c = RedisType::try_from(b);
                    match c {
                        Ok(c) => {
                            println!("value read: {:?}", c);
                            Ok(Some(c))
                        }
                        Err(e) => Err(HandlerError::Parser(e)),
                    }
                }
                None => Ok(None),
            },
            Err(e) => Err(e),
        }
    }

    pub(crate) fn write(&mut self, buffer: &[u8]) -> Result<(), HandlerError> {
        self.stream
            .write_all(buffer)
            .map_err(|r| HandlerError::Io(r.to_string()))
    }

    pub(crate) fn write_command(&mut self, command: RedisCommand) -> Result<(), HandlerError> {
        println!("command send: {:?}", command);
        let buffer: Vec<u8> = command.into();
        self.write(buffer.as_slice())
    }

    pub(crate) fn write_redis_type(&mut self, command: RedisType) -> Result<(), HandlerError> {
        let buffer: Vec<u8> = command.into();
        self.write(buffer.as_slice())
    }

    pub fn server_action(
        command: RedisCommand<'a>,
        config: &config::Config,
    ) -> Result<RedisType<'static>, HandlerError> {
        match command {
            RedisCommand::Ping => Ok(RedisType::simple_string("PONG")),
            RedisCommand::Echo(value) => match value {
                None => Ok(RedisType::null_bulk_string()),
                Some(v) => Ok(RedisType::BulkString(Some(Cow::Owned(v.to_vec())))),
            },
            RedisCommand::Get(key) => {
                let mut store = STORE.lock().unwrap();
                let key = String::from_utf8(key.to_vec()).unwrap();
                match store.get(&key) {
                    Some((value, expired_at)) => {
                        if *expired_at == 0 || *expired_at >= utilities::now() {
                            Ok(value.to_owned())
                        } else {
                            store.remove(&key);
                            Ok(RedisType::null_bulk_string())
                        }
                    }
                    None => Ok(RedisType::null_bulk_string()),
                }
            }
            RedisCommand::Set(key, value, px) => {
                let mut store = STORE.lock().unwrap();
                let key = String::from_utf8(key.to_vec()).unwrap();
                store.insert(
                    key,
                    (
                        RedisType::BulkString(Some(Cow::from(value.into_owned()))),
                        match px {
                            None => 0,
                            Some(px) => px + utilities::now(),
                        },
                    ),
                );
                Ok(RedisType::simple_string("OK"))
            }
            RedisCommand::Info => Ok(ReplicationInfo {
                role: match config.clone().get_replica_of() {
                    Some((_, _)) => "slave".to_string(),
                    None => "master".to_string(),
                },
            }
            .into()),
            RedisCommand::Replconf(_, _) => Ok(RedisType::simple_string("OK")),
            RedisCommand::Psync(_, _) => Ok(RedisType::simple_string(
                "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0",
            )),
        }
    }

    pub fn replica_handshake(replica_port: u32, master_host: &String, master_port: u32) {
        let client = TcpStream::connect(&format!("{}:{}", master_host, master_port)).unwrap();
        let mut handler = StreamHandler::new(client);

        handler.write_command(RedisCommand::Ping).unwrap();

        handler.read_type().unwrap().unwrap();

        handler
            .write_command(RedisCommand::Replconf(
                Cow::Borrowed("listening-port".as_ref()),
                Cow::Borrowed(replica_port.to_string().as_bytes()),
            ))
            .unwrap();

        handler.read_type().unwrap().unwrap();

        handler
            .write_command(RedisCommand::Replconf(
                Cow::Borrowed("capa".as_ref()),
                Cow::Borrowed("psync2".as_ref()),
            ))
            .unwrap();

        handler.read_type().unwrap().unwrap();

        handler
            .write_command(RedisCommand::Psync(
                Cow::Borrowed("?".as_ref()),
                Cow::Borrowed("-1".as_ref()),
            ))
            .unwrap();

        handler.read_type().unwrap().unwrap();
    }

    pub fn server_handle(&mut self, config: &config::Config) -> Result<(), HandlerError> {
        loop {
            match self.read_command() {
                Err(e) => return Err(e),
                Ok(b) => match b {
                    None => break,
                    Some(c) => {
                        println!("received command: {:?}", c);
                        let response = Self::server_action(c, config).unwrap();
                        self.write_redis_type(response).unwrap();
                    }
                },
            }
        }
        Ok(())
    }
}
