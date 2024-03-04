use crate::command::{self, RedisCommand};
use crate::config;
use crate::info::ReplicationInfo;
use crate::parser::{self, MessageParserError};
use crate::utilities;
use crate::value::RedisValue;
use once_cell::sync::Lazy;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::{collections::HashMap, sync::Mutex};

static STORE: Lazy<Mutex<HashMap<String, (RedisValue, u64)>>> = Lazy::new(|| {
    let m = HashMap::new();
    Mutex::new(m)
});

#[derive(PartialEq, Debug)]
pub enum HandlerError {
    Io(String),
    Parser(MessageParserError),
    Other,
}

pub struct StreamHandler {
    stream: TcpStream,
}

impl StreamHandler {
    pub fn new(stream: TcpStream) -> Self {
        StreamHandler { stream }
    }

    pub(crate) fn read(&mut self) -> Result<Option<Vec<u8>>, HandlerError> {
        let mut buffer: [u8; 1024] = [0; 1024];
        match self.stream.read(&mut buffer) {
            Ok(0) => Ok(None),
            Ok(_) => Ok(Some(buffer.to_vec())),
            Err(e) => Err(HandlerError::Io(e.to_string())),
        }
    }

    pub(crate) fn write(&mut self, buffer: &[u8]) -> Result<(), HandlerError> {
        self.stream
            .write_all(buffer)
            .map_err(|r| HandlerError::Io(r.to_string()))
    }

    pub(crate) fn action(
        &self,
        command: RedisCommand,
        config: &config::Config,
    ) -> Result<RedisValue, HandlerError> {
        match command {
            RedisCommand::Ping => Ok(RedisValue::simple_string("PONG")),
            RedisCommand::Echo(value) => Ok(value),
            RedisCommand::Get(key) => match key {
                RedisValue::BulkString(Some(key)) => {
                    let mut store = STORE.lock().unwrap();
                    match store.get(&key) {
                        Some((value, expired_at)) => {
                            if *expired_at == 0 || *expired_at >= utilities::now() {
                                Ok(value.clone())
                            } else {
                                store.remove(&key);
                                Ok(RedisValue::null_bulk_string())
                            }
                        }
                        None => Ok(RedisValue::null_bulk_string()),
                    }
                }
                _ => Err(HandlerError::Other),
            },
            RedisCommand::Set(key, value, expired_at) => match key {
                RedisValue::BulkString(key) => match key {
                    Some(key) => {
                        STORE
                            .lock()
                            .unwrap()
                            .insert(key, (value, expired_at.unwrap_or(0 as u64)));
                        Ok(RedisValue::SimpleString("OK".into()))
                    }
                    None => Err(HandlerError::Other),
                },
                _ => Err(HandlerError::Other),
            },
            RedisCommand::Info => Ok(ReplicationInfo {
                role: match config.clone().get_replica_of() {
                    Some((host, port)) => "slave".to_string(),
                    None => "master".to_string(),
                },
            }
            .into()),
            RedisCommand::Replconf(_, _) => Ok(RedisValue::SimpleString("OK".into())),
        }
    }

    pub fn handle(&mut self, config: &config::Config) -> Result<(), HandlerError> {
        loop {
            let command = match self.read() {
                Err(e) => return Err(e),
                Ok(b) => match b {
                    None => break,
                    Some(b_) => {
                        let mut parser = parser::MessageParser::new(b_.as_slice());
                        match parser.parse() {
                            Ok(v) => command::RedisCommand::try_from(v).unwrap(),
                            Err(e) => return Err(HandlerError::Parser(e)),
                        }
                    }
                },
            };
            println!("command received: {:?}", command);
            let response = self.action(command, config).unwrap();
            let response_str: String = (&response).into();
            match self.write(response_str.as_bytes()) {
                Err(e) => return Err(e),
                _ => {}
            }
        }
        Ok(())
    }
}
