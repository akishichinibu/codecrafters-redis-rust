use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::net::{TcpSocket, TcpStream};

use crate::parser::{MessageParserStateError, RedisValueParser};
use crate::value::{RedisBulkString, RedisValue};
use std::io::{Error, ErrorKind};
use std::vec;

#[derive(PartialEq, Debug, Clone)]
pub enum RedisCommand {
    Ping,
    Echo(RedisBulkString),
    Get(RedisBulkString),
    Set(RedisBulkString, RedisBulkString, Option<u64>),
    Replconf(RedisBulkString, RedisBulkString),
    Info,
    Psync(RedisBulkString, RedisBulkString),
}

impl RedisCommand {
    pub fn replconf(a1: &str, a2: &str) -> RedisCommand {
        RedisCommand::Replconf(a1.into(), a2.into())
    }
    pub fn pasync(a1: &str, a2: &str) -> RedisCommand {
        RedisCommand::Psync(a1.into(), a2.into())
    }
}

impl Into<RedisValue> for &RedisCommand {
    fn into(self) -> RedisValue {
        match self {
            RedisCommand::Ping => vec![RedisValue::bulk_string("ping")],
            RedisCommand::Echo(v) => {
                vec![
                    RedisValue::bulk_string("echo"),
                    RedisValue::BulkString(Some(v.clone())),
                ]
            }
            RedisCommand::Set(k, v, _) => vec![
                RedisValue::bulk_string("set"),
                RedisValue::BulkString(Some(k.clone())),
                RedisValue::BulkString(Some(v.clone())),
            ],
            RedisCommand::Get(k) => vec![
                RedisValue::bulk_string("get"),
                RedisValue::BulkString(Some(k.clone())),
            ],
            RedisCommand::Info => vec![RedisValue::null_bulk_string()],
            RedisCommand::Replconf(k, v) => {
                vec![
                    RedisValue::bulk_string("replconf"),
                    RedisValue::BulkString(Some(k.clone())),
                    RedisValue::BulkString(Some(v.clone())),
                ]
            }
            RedisCommand::Psync(id, offset) => vec![
                RedisValue::bulk_string("psync"),
                RedisValue::BulkString(Some(id.clone())),
                RedisValue::BulkString(Some(offset.clone())),
            ],
        }
        .into()
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum RedisCommandError {
    Malform(String),
    Malform2(MessageParserStateError),
    DismatchedArgsNum,
    UnknownCommand(String),
    NilArg,
}

impl TryInto<RedisCommand> for RedisValue {
    type Error = RedisCommandError;

    fn try_into(self) -> Result<RedisCommand, Self::Error> {
        let args = match self {
            RedisValue::Array(args) => args,
            _ => return Err(RedisCommandError::Malform("".to_string())),
        };

        let (command, args) = args.split_first().unwrap();
        println!("try to parse received: {:?} {:?}", command, args);

        let command_name = match command {
            RedisValue::BulkString(Some(s)) => String::from_utf8(s.data.clone()).unwrap(),
            _ => return Err(RedisCommandError::Malform("".to_string())),
        }
        .to_lowercase();

        let command = match command_name.as_str() {
            "ping" => RedisCommand::Ping,
            "echo" => match args.len() {
                1 => match &args[0] {
                    RedisValue::BulkString(Some(s)) => RedisCommand::Echo(s.to_owned()),
                    _ => return Err(RedisCommandError::NilArg),
                },
                _ => return Err(RedisCommandError::DismatchedArgsNum),
            },
            "get" => match args.len() {
                1 => match &args[0] {
                    RedisValue::BulkString(Some(s)) => RedisCommand::Get(s.to_owned()),
                    _ => return Err(RedisCommandError::NilArg),
                },
                _ => return Err(RedisCommandError::DismatchedArgsNum),
            },
            "set" => match args.len() {
                2 => match &args[0] {
                    RedisValue::BulkString(Some(k)) => match &args[1] {
                        RedisValue::BulkString(Some(v)) => {
                            RedisCommand::Set(k.to_owned(), v.to_owned(), None)
                        }
                        _ => return Err(RedisCommandError::NilArg),
                    },
                    _ => return Err(RedisCommandError::NilArg),
                },
                4 => {
                    let k = match &args[0] {
                        RedisValue::BulkString(Some(k)) => k.to_owned(),
                        _ => return Err(RedisCommandError::NilArg),
                    };
                    let v = match &args[1] {
                        RedisValue::BulkString(Some(v)) => v.to_owned(),
                        _ => return Err(RedisCommandError::NilArg),
                    };
                    let px = match &args[3] {
                        RedisValue::BulkString(Some(px)) => String::from_utf8(px.data.to_vec())
                            .unwrap()
                            .parse()
                            .unwrap(),
                        _ => return Err(RedisCommandError::NilArg),
                    };
                    RedisCommand::Set(k.to_owned(), v.to_owned(), Some(px))
                }
                _ => return Err(RedisCommandError::DismatchedArgsNum),
            },
            "info" => match args.len() {
                1 => RedisCommand::Info,
                _ => return Err(RedisCommandError::DismatchedArgsNum),
            },
            "replconf" => match args.len() {
                2 => {
                    let arg1 = match &args[0] {
                        RedisValue::BulkString(Some(s)) => s,
                        _ => return Err(RedisCommandError::NilArg),
                    };
                    let arg2 = match &args[1] {
                        RedisValue::BulkString(Some(s)) => s,
                        _ => return Err(RedisCommandError::NilArg),
                    };
                    RedisCommand::Replconf(arg1.to_owned(), arg2.to_owned())
                }
                _ => return Err(RedisCommandError::DismatchedArgsNum),
            },
            "psync" => match args.len() {
                2 => {
                    let arg1 = match &args[0] {
                        RedisValue::BulkString(Some(s)) => s,
                        _ => return Err(RedisCommandError::NilArg),
                    };
                    let arg2 = match &args[1] {
                        RedisValue::BulkString(Some(s)) => s,
                        _ => return Err(RedisCommandError::NilArg),
                    };

                    RedisCommand::Psync(arg1.to_owned(), arg2.to_owned())
                }
                _ => return Err(RedisCommandError::DismatchedArgsNum),
            },
            s => return Err(RedisCommandError::UnknownCommand(s.to_string())),
        };
        Ok(command)
    }
}

pub trait RedisTcpStreamReadExt {
    async fn read_value(
        &mut self,
        parser: &mut RedisValueParser,
    ) -> Result<Option<RedisValue>, std::io::Error>;
    async fn read_command(
        &mut self,
        parser: &mut RedisValueParser,
    ) -> Result<Option<RedisCommand>, std::io::Error>;
}

impl<'a> RedisTcpStreamReadExt for ReadHalf<'a> {
    async fn read_value(
        &mut self,
        parser: &mut RedisValueParser,
    ) -> Result<Option<RedisValue>, std::io::Error> {
        println!("try to read a value");
        let mut buffer: [u8; 1024] = [0; 1024];
        match self.read(buffer.as_mut_slice()).await {
            Err(e) => Err(e),
            Ok(0) => Err(ErrorKind::ConnectionAborted.into()),
            Ok(n) => {
                println!("read {}", n);
                parser.append(&buffer[0..n]);
                match parser.parse() {
                    Ok((value, _)) => Ok(value),
                    Err(e) => match e {
                        MessageParserStateError::UnexceptedTermination => Ok(None),
                        _ => panic!("{:?}", e),
                    },
                }
            }
        }
    }

    async fn read_command(
        &mut self,
        parser: &mut RedisValueParser,
    ) -> Result<Option<RedisCommand>, std::io::Error> {
        let value = match self.read_value(parser).await {
            Ok(value) => value,
            Err(e) => return Err(e),
        };
        let value = if let Some(value) = value {
            value
        } else {
            return Ok(None);
        };
        match value {
            RedisValue::Array(a) => {
                let command: RedisCommand = RedisValue::Array(a).try_into().unwrap();
                println!("received command: {:?}", command);
                Ok(Some(command))
            }
            _ => panic!(),
        }
    }
}

pub trait RedisTcpStreamWriteExt {
    async fn write_value(&mut self, value: &RedisValue) -> Result<(), std::io::Error>;
    async fn write_command(&mut self, commmand: &RedisCommand) -> Result<(), std::io::Error>;
}

impl<'a> RedisTcpStreamWriteExt for WriteHalf<'a> {
    async fn write_value(&mut self, value: &RedisValue) -> Result<(), std::io::Error> {
        let bytes: Vec<u8> = value.into();
        self.write_all(bytes.as_slice()).await
    }

    async fn write_command(&mut self, command: &RedisCommand) -> Result<(), std::io::Error> {
        let value: RedisValue = command.into();
        let bytes: Vec<u8> = (&value).into();
        self.write_all(bytes.as_slice()).await
    }
}

impl RedisTcpStreamWriteExt for TcpStream {
    async fn write_value(&mut self, value: &RedisValue) -> Result<(), std::io::Error> {
        let bytes: Vec<u8> = value.into();
        self.write_all(bytes.as_slice()).await
    }

    async fn write_command(&mut self, command: &RedisCommand) -> Result<(), std::io::Error> {
        let value: RedisValue = command.into();
        let bytes: Vec<u8> = (&value).into();
        self.write_all(bytes.as_slice()).await
    }
}

impl RedisTcpStreamReadExt for TcpStream {
    async fn read_value(
        &mut self,
        parser: &mut RedisValueParser,
    ) -> Result<Option<RedisValue>, std::io::Error> {
        let mut buffer: [u8; 1024] = [0; 1024];
        match self.read(buffer.as_mut_slice()).await {
            Ok(0) => Ok(None),
            Ok(n) => {
                println!("read {}", n);
                parser.append(&buffer[0..n]);
                match parser.parse() {
                    Ok((value, _)) => Ok(value),
                    Err(e) => panic!("{:?}", e),
                }
            }
            Err(e) => Err(e),
        }
    }

    async fn read_command(
        &mut self,
        parser: &mut RedisValueParser,
    ) -> Result<Option<RedisCommand>, std::io::Error> {
        let value = match self.read_value(parser).await {
            Ok(value) => value,
            Err(e) => return Err(e),
        };
        let value = if let Some(value) = value {
            value
        } else {
            return Ok(None);
        };
        match value {
            RedisValue::Array(a) => {
                let command: RedisCommand = RedisValue::Array(a).try_into().unwrap();
                println!("received command: {:?}", command);
                Ok(Some(command))
            }
            _ => panic!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::parser;

    use super::*;

    #[test]
    fn test_echo_command() {
        let input = b"*2\r\n$4\r\necho\r\n$3\r\nhey\r\n";
        let mut parser = RedisValueParser::new();
        parser.append(input);

        let (value, t) = parser.parse().unwrap();
        let c: RedisCommand = value.unwrap().try_into().unwrap();
        assert_eq!(RedisCommand::Echo("hey".into()), c);
    }
}
