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
    Replconf(RedisBulkString, RedisBulkString),
    Info(RedisBulkString),
    Psync(RedisBulkString, RedisBulkString),
    Wait(u64, u64),
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
                let mut vs = vec![
                    RedisValue::bulk_string("set"),
                    k.to_owned().into(),
                    v.to_owned().into(),
                ];
                if let Some(px) = px {
                    vs.push(RedisValue::bulk_string("px"));
                    vs.push(RedisValue::bulk_string(px.to_string().as_str()));
                }
                vs
            }
            RedisCommand::Get(k) => vec![RedisValue::bulk_string("get"), k.to_owned().into()],
            RedisCommand::Info(v) => vec![RedisValue::bulk_string("info"), v.to_owned().into()],
            RedisCommand::Replconf(k, v) => {
                vec![
                    RedisValue::bulk_string("replconf"),
                    k.to_owned().into(),
                    v.to_owned().into(),
                ]
            }
            RedisCommand::Psync(id, offset) => vec![
                RedisValue::bulk_string("psync"),
                id.to_owned().into(),
                offset.to_owned().into(),
            ],
            RedisCommand::Wait(number, timeout) => vec![
                RedisValue::bulk_string("wait"),
                RedisValue::bulk_string(number.to_string().as_str()),
                RedisValue::bulk_string(timeout.to_string().as_str()),
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
        println!(
            "[command] try to parse received value to command: {:?}",
            args
        );

        let (command, args) = args.split_first().unwrap();

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
                1 => {
                    let v = match &args[0] {
                        RedisValue::BulkString(Some(k)) => k.to_owned(),
                        _ => return Err(RedisCommandError::NilArg),
                    };
                    RedisCommand::Info(v)
                }
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
            "wait" => match args.len() {
                2 => {
                    let number: String = match &args[0] {
                        RedisValue::BulkString(Some(s)) => s.into(),
                        _ => return Err(RedisCommandError::NilArg),
                    };
                    let number: u64 = number.parse().unwrap();
                    let timeout: String = match &args[1] {
                        RedisValue::BulkString(Some(s)) => s.into(),
                        _ => return Err(RedisCommandError::NilArg),
                    };
                    let timeout: u64 = timeout.parse().unwrap();
                    RedisCommand::Wait(number, timeout)
                }
                _ => return Err(RedisCommandError::DismatchedArgsNum),
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
        println!("[read_value] try to read a value {}", parser.buffer_len());
        let mut buffer: [u8; 1024] = [0; 1024];

        if let Ok((Some(value), offset)) = parser.parse() {
            return Ok((Some(value), offset + 1));
        }

        match self.read(buffer.as_mut_slice()).await {
            Err(e) => Err(e),
            Ok(0) => Err(ErrorKind::ConnectionAborted.into()),
            Ok(n) => {
                println!("[read_value] read bytes {}", n);
                parser.append(&buffer[0..n]);
                match parser.parse() {
                    Ok((value, offset)) => {
                        println!("read value: {:?}", value);
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
        println!("try to read a rdb {}", parser.buffer_len());
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
