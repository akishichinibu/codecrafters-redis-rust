use async_trait::async_trait;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};

use crate::parser::{MessageParserStateError, RedisValueParser};
use crate::value::{RedisBulkString, RedisValue};
use std::io::ErrorKind;
use std::vec;

#[derive(PartialEq, Debug, Clone)]
pub enum RedisCommand {
    Ping,
    Echo(RedisBulkString),
    Get(RedisBulkString),
    Set(RedisBulkString, RedisBulkString, Option<u64>),
    Type(RedisBulkString),
    Replconf(RedisBulkString, RedisBulkString),
    Info(RedisBulkString),
    Psync(RedisBulkString, RedisBulkString),
    Wait(u64, u64),
    Select(u64),
    Config(RedisBulkString, RedisBulkString),
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
            RedisCommand::Set(k, v, px) => {
                let mut vs = vec![RedisValue::bulk_string("set"), k.into(), v.into()];
                if let Some(px) = px {
                    vs.push(RedisValue::bulk_string("px"));
                    vs.push(RedisValue::bulk_string(px.to_string().as_str()));
                }
                vs
            }
            RedisCommand::Get(k) => vec![RedisValue::bulk_string("get"), k.into()],
            RedisCommand::Type(k) => vec![RedisValue::bulk_string("type"), k.into()],
            RedisCommand::Info(v) => vec![RedisValue::bulk_string("info"), v.into()],
            RedisCommand::Replconf(k, v) => {
                vec![RedisValue::bulk_string("replconf"), k.into(), v.into()]
            }
            RedisCommand::Psync(id, offset) => {
                vec![RedisValue::bulk_string("psync"), id.into(), offset.into()]
            }
            RedisCommand::Wait(number, timeout) => vec![
                RedisValue::bulk_string("wait"),
                RedisValue::bulk_string(number.to_string().as_str()),
                RedisValue::bulk_string(timeout.to_string().as_str()),
            ],
            RedisCommand::Select(index) => vec![
                RedisValue::bulk_string("select"),
                RedisValue::bulk_string(index.to_string().as_str()),
            ],
            RedisCommand::Config(method, key) => {
                vec![RedisValue::bulk_string("config"), method.into(), key.into()]
            }
        }
        .into()
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum RedisCommandError {
    Malform(String),
    ParsingError(MessageParserStateError),
    DismatchedArgsNum(usize, usize),
    UnknownCommand(String),
    IlleagalArg,
}

impl TryInto<RedisCommand> for RedisValue {
    type Error = RedisCommandError;

    fn try_into(self) -> Result<RedisCommand, Self::Error> {
        let args = match self {
            RedisValue::Array(args) => args,
            _ => {
                return Err(RedisCommandError::Malform(format!(
                    "can only convert an array redis value to a command, but got {:?}",
                    self
                )))
            }
        };
        println!(
            "[command] try to parse received value to command: {:?}",
            args
        );

        let (command, args) = args.split_first().unwrap();

        let command_name: String = match command {
            RedisValue::BulkString(Some(s)) => s.into(),
            _ => {
                return Err(RedisCommandError::Malform(format!(
                    "the command name must be a bulk string, but got {:?}",
                    command
                )))
            }
        };
        let command_name = command_name.to_lowercase();

        let command = match command_name.as_str() {
            "ping" => RedisCommand::Ping,
            "echo" => match args.len() {
                1 => match &args[0] {
                    RedisValue::BulkString(Some(s)) => RedisCommand::Echo(s.to_owned()),
                    _ => return Err(RedisCommandError::IlleagalArg),
                },
                n => return Err(RedisCommandError::DismatchedArgsNum(1, n)),
            },
            "get" => match args.len() {
                1 => match &args[0] {
                    RedisValue::BulkString(Some(s)) => RedisCommand::Get(s.to_owned()),
                    _ => return Err(RedisCommandError::IlleagalArg),
                },
                n => return Err(RedisCommandError::DismatchedArgsNum(1, n)),
            },
            "set" => match args.len() {
                2 => match &args[0] {
                    RedisValue::BulkString(Some(k)) => match &args[1] {
                        RedisValue::BulkString(Some(v)) => {
                            RedisCommand::Set(k.to_owned(), v.to_owned(), None)
                        }
                        _ => return Err(RedisCommandError::IlleagalArg),
                    },
                    _ => return Err(RedisCommandError::IlleagalArg),
                },
                4 => {
                    let k = match &args[0] {
                        RedisValue::BulkString(Some(k)) => k.to_owned(),
                        _ => return Err(RedisCommandError::IlleagalArg),
                    };
                    let v = match &args[1] {
                        RedisValue::BulkString(Some(v)) => v.to_owned(),
                        _ => return Err(RedisCommandError::IlleagalArg),
                    };
                    let px = match &args[3] {
                        RedisValue::BulkString(Some(px)) => String::from_utf8(px.data.to_vec())
                            .unwrap()
                            .parse()
                            .unwrap(),
                        _ => return Err(RedisCommandError::IlleagalArg),
                    };
                    RedisCommand::Set(k.to_owned(), v.to_owned(), Some(px))
                }
                n => return Err(RedisCommandError::DismatchedArgsNum(4, n)),
            },
            "type" => match args.len() {
                1 => {
                    let v = match &args[0] {
                        RedisValue::BulkString(Some(k)) => k.to_owned(),
                        _ => return Err(RedisCommandError::IlleagalArg),
                    };
                    RedisCommand::Type(v)
                }
                n => return Err(RedisCommandError::DismatchedArgsNum(1, n)),
            },
            "info" => match args.len() {
                1 => {
                    let v = match &args[0] {
                        RedisValue::BulkString(Some(k)) => k.to_owned(),
                        _ => return Err(RedisCommandError::IlleagalArg),
                    };
                    RedisCommand::Info(v)
                }
                n => return Err(RedisCommandError::DismatchedArgsNum(1, n)),
            },
            "replconf" => match args.len() {
                n => {
                    let arg1 = match &args[0] {
                        RedisValue::BulkString(Some(s)) => s,
                        _ => return Err(RedisCommandError::IlleagalArg),
                    };
                    let arg2 = match &args[1] {
                        RedisValue::BulkString(Some(s)) => s,
                        _ => return Err(RedisCommandError::IlleagalArg),
                    };
                    RedisCommand::Replconf(arg1.to_owned(), arg2.to_owned())
                }
                n => return Err(RedisCommandError::DismatchedArgsNum(2, n)),
            },
            "psync" => match args.len() {
                2 => {
                    let arg1 = match &args[0] {
                        RedisValue::BulkString(Some(s)) => s,
                        _ => return Err(RedisCommandError::IlleagalArg),
                    };
                    let arg2 = match &args[1] {
                        RedisValue::BulkString(Some(s)) => s,
                        _ => return Err(RedisCommandError::IlleagalArg),
                    };

                    RedisCommand::Psync(arg1.to_owned(), arg2.to_owned())
                }
                n => return Err(RedisCommandError::DismatchedArgsNum(2, n)),
            },
            "wait" => match args.len() {
                2 => {
                    let number: String = match &args[0] {
                        RedisValue::BulkString(Some(s)) => s.into(),
                        _ => return Err(RedisCommandError::IlleagalArg),
                    };
                    let number: u64 = number.parse().unwrap();
                    let timeout: String = match &args[1] {
                        RedisValue::BulkString(Some(s)) => s.into(),
                        _ => return Err(RedisCommandError::IlleagalArg),
                    };
                    let timeout: u64 = timeout.parse().unwrap();
                    RedisCommand::Wait(number, timeout)
                }
                n => return Err(RedisCommandError::DismatchedArgsNum(1, n)),
            },
            "select" => match args.len() {
                1 => {
                    let index: String = match &args[0] {
                        RedisValue::BulkString(Some(s)) => s.into(),
                        _ => return Err(RedisCommandError::IlleagalArg),
                    };
                    let index: u64 = index.parse().unwrap();
                    RedisCommand::Select(index)
                }
                n => return Err(RedisCommandError::DismatchedArgsNum(1, n)),
            },
            "config" => match args.len() {
                2 => {
                    let method = match &args[0] {
                        RedisValue::BulkString(Some(s)) => s,
                        _ => return Err(RedisCommandError::IlleagalArg),
                    };
                    let key = match &args[1] {
                        RedisValue::BulkString(Some(s)) => s,
                        _ => return Err(RedisCommandError::IlleagalArg),
                    };
                    RedisCommand::Config(method.to_owned(), key.to_owned())
                }
                n => return Err(RedisCommandError::DismatchedArgsNum(2, n)),
            },
            s => return Err(RedisCommandError::UnknownCommand(s.to_string())),
        };
        Ok(command)
    }
}

#[async_trait]
pub trait RedisTcpStreamReadExt {
    async fn read_value(
        &mut self,
        parser: &mut RedisValueParser,
    ) -> Result<(Option<RedisValue>, usize), std::io::Error>;
    async fn read_command(
        &mut self,
        parser: &mut RedisValueParser,
    ) -> Result<(Option<RedisCommand>, usize), std::io::Error>;
    async fn read_rdb(
        &mut self,
        parser: &mut RedisValueParser,
    ) -> Result<(Option<RedisValue>, usize), std::io::Error>;
}

#[async_trait]
impl RedisTcpStreamReadExt for OwnedReadHalf {
    async fn read_value(
        &mut self,
        parser: &mut RedisValueParser,
    ) -> Result<(Option<RedisValue>, usize), std::io::Error> {
        println!(
            "[read_value] try to read a value, parser buffer: {}",
            parser.buffer_len()
        );
        let mut buffer: [u8; 1024] = [0; 1024];

        if let Ok((Some(value), offset)) = parser.parse() {
            return Ok((Some(value), offset + 1));
        }

        match self.read(buffer.as_mut_slice()).await {
            Err(e) => Err(e),
            Ok(0) => Err(ErrorKind::ConnectionAborted.into()),
            Ok(n) => {
                println!("[read_value] read bytes, length: {}", n);
                parser.append(&buffer[0..n]);
                match parser.parse() {
                    Ok((value, offset)) => {
                        println!("[read value] parse success: {:?}", value);
                        Ok((value, offset + 1))
                    }
                    Err(e) => panic!("{:?}", e),
                }
            }
        }
    }

    async fn read_command(
        &mut self,
        parser: &mut RedisValueParser,
    ) -> Result<(Option<RedisCommand>, usize), std::io::Error> {
        let (value, length) = match self.read_value(parser).await {
            Ok(v) => v,
            Err(e) => return Err(e),
        };
        let value = if let Some(value) = value {
            value
        } else {
            return Ok((None, length));
        };
        match value {
            RedisValue::Array(a) => {
                let command: RedisCommand = RedisValue::Array(a).try_into().unwrap();
                println!("[read_command] received command({}): {:?}", length, command);
                Ok((Some(command), length))
            }
            _ => panic!(),
        }
    }

    async fn read_rdb(
        &mut self,
        parser: &mut RedisValueParser,
    ) -> Result<(Option<RedisValue>, usize), std::io::Error> {
        println!("[read_rdb] try to read a rdb {}", parser.buffer_len());
        let mut buffer: [u8; 102400] = [0; 102400];

        if let Ok((Some(value), offset)) = parser.parse_rdb() {
            return Ok((Some(value), offset + 1));
        }

        match self.read(buffer.as_mut_slice()).await {
            Err(e) => Err(e),
            Ok(0) => Err(ErrorKind::ConnectionAborted.into()),
            Ok(n) => {
                println!("[read_value] read bytes {}", n);
                parser.append(&buffer[0..n]);
                match parser.parse_rdb() {
                    Ok((v, offset)) => {
                        let length = offset + 1;
                        println!("[read_command] received rdb({})", length);
                        Ok((v, length))
                    }
                    Err(e) => panic!("{:?}", e),
                }
            }
        }
    }
}

#[async_trait]
pub trait RedisTcpStreamWriteExt {
    async fn write_value(&mut self, value: &RedisValue) -> Result<(), std::io::Error>;
    async fn write_command(&mut self, commmand: &RedisCommand) -> Result<(), std::io::Error>;
}

#[async_trait]
impl RedisTcpStreamWriteExt for OwnedWriteHalf {
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

// #[cfg(test)]
// mod tests {
//     use crate::parser;

//     use super::*;

//     #[test]
//     fn test_echo_command() {
//         let input = b"*2\r\n$4\r\necho\r\n$3\r\nhey\r\n";
//         let mut parser = RedisValueParser::new();
//         parser.append(input);

//         let (value, t) = parser.parse().unwrap();
//         let c: RedisCommand = value.unwrap().try_into().unwrap();
//         assert_eq!(RedisCommand::Echo("hey".into()), c);
//     }
// }
