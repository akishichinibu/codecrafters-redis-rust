use crate::parser::RedisValueParserError;
use crate::r#type::RedisType;
use std::borrow::Cow;
use std::vec;

#[derive(PartialEq, Debug, Clone)]
pub enum RedisCommand<'a> {
    Ping,
    Echo(Option<Cow<'a, [u8]>>),
    Get(Cow<'a, [u8]>),
    Set(Cow<'a, [u8]>, Cow<'a, [u8]>, Option<u64>),
    Replconf(Cow<'a, [u8]>, Cow<'a, [u8]>),
    Info,
    Psync(Cow<'a, [u8]>, Cow<'a, [u8]>),
}

impl<'a> Into<Vec<u8>> for RedisCommand<'a> {
    fn into(self) -> Vec<u8> {
        let args: RedisType = match self {
            RedisCommand::Ping => vec![RedisType::bulk_string("ping")],
            RedisCommand::Echo(v) => vec![
                RedisType::bulk_string("echo"),
                match v {
                    Some(v) => RedisType::BulkString(Some(v)),
                    None => RedisType::null_bulk_string(),
                },
            ],
            RedisCommand::Set(k, v, _) => vec![
                RedisType::bulk_string("set"),
                RedisType::BulkString(Some(k)),
                RedisType::BulkString(Some(v)),
            ],
            RedisCommand::Get(k) => vec![
                RedisType::bulk_string("get"),
                RedisType::BulkString(Some(k)),
            ],
            RedisCommand::Info => vec![RedisType::null_bulk_string()],
            RedisCommand::Replconf(k, v) => {
                vec![
                    RedisType::bulk_string("replconf"),
                    RedisType::BulkString(Some(k)),
                    RedisType::BulkString(Some(v)),
                ]
            }
            RedisCommand::Psync(id, offset) => vec![
                RedisType::bulk_string("psync"),
                RedisType::BulkString(Some(id)),
                RedisType::BulkString(Some(offset)),
            ],
        }
        .into();
        args.into()
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum RedisCommandError {
    Malform,
    Malform2(RedisValueParserError),
    DismatchedArgsNum,
    UnknownCommand(String),
}

impl<'a> TryFrom<&'a [u8]> for RedisCommand<'a> {
    type Error = RedisCommandError;

    fn try_from(value: &'a [u8]) -> Result<RedisCommand<'a>, Self::Error> {
        let args = match RedisType::<'a>::try_from(value) {
            Ok(RedisType::Array(r)) => match &r.len() {
                0 => return Err(RedisCommandError::Malform),
                _ => r,
            },
            Err(e) => return Err(RedisCommandError::Malform2(e)),
            _ => return Err(RedisCommandError::Malform),
        };

        let (command, args) = args.split_first().unwrap();
        println!("try to parse received: {:?} {:?}", command, args);

        let command = match command {
            RedisType::BulkString(Some(s)) => String::from_utf8(s.to_vec()).unwrap(),
            _ => return Err(RedisCommandError::Malform),
        }
        .to_lowercase();

        match command.as_str() {
            "ping" => Ok(RedisCommand::Ping),
            "echo" => match args.len() {
                1 => match &args[0] {
                    RedisType::BulkString(s) => Ok(RedisCommand::Echo(s.to_owned())),
                    _ => Err(RedisCommandError::Malform),
                },
                _ => Err(RedisCommandError::DismatchedArgsNum),
            },
            "get" => match args.len() {
                1 => match &args[0] {
                    RedisType::BulkString(Some(s)) => Ok(RedisCommand::Get(s.to_owned())),
                    _ => Err(RedisCommandError::Malform),
                },
                _ => Err(RedisCommandError::DismatchedArgsNum),
            },
            "set" => match args.len() {
                2 => match &args[0] {
                    RedisType::BulkString(Some(k)) => match &args[1] {
                        RedisType::BulkString(Some(v)) => {
                            Ok(RedisCommand::Set(k.to_owned(), v.to_owned(), None))
                        }
                        _ => Err(RedisCommandError::Malform),
                    },
                    _ => Err(RedisCommandError::Malform),
                },
                4 => match &args[0] {
                    RedisType::BulkString(Some(k)) => match &args[1] {
                        RedisType::BulkString(Some(v)) => match &args[3] {
                            RedisType::BulkString(Some(px)) => {
                                let px: u64 =
                                    String::from_utf8(px.to_vec()).unwrap().parse().unwrap();
                                Ok(RedisCommand::Set(k.to_owned(), v.to_owned(), Some(px)))
                            }
                            _ => Err(RedisCommandError::Malform),
                        },
                        _ => Err(RedisCommandError::Malform),
                    },
                    _ => Err(RedisCommandError::Malform),
                },
                _ => Err(RedisCommandError::DismatchedArgsNum),
            },
            "info" => match args.len() {
                1 => Ok(RedisCommand::Info),
                _ => Err(RedisCommandError::DismatchedArgsNum),
            },
            "replconf" => match args.len() {
                2 => {
                    let arg1 = match &args[0] {
                        RedisType::BulkString(Some(s)) => s,
                        _ => return Err(RedisCommandError::Malform),
                    };
                    let arg2 = match &args[1] {
                        RedisType::BulkString(Some(s)) => s,
                        _ => return Err(RedisCommandError::Malform),
                    };
                    Ok(RedisCommand::Replconf(arg1.to_owned(), arg2.to_owned()))
                }
                _ => Err(RedisCommandError::DismatchedArgsNum),
            },
            "psync" => match args.len() {
                2 => {
                    let arg1 = match &args[0] {
                        RedisType::BulkString(Some(s)) => s,
                        _ => return Err(RedisCommandError::Malform),
                    };
                    let arg2 = match &args[1] {
                        RedisType::BulkString(Some(s)) => s,
                        _ => return Err(RedisCommandError::Malform),
                    };

                    Ok(RedisCommand::Psync(arg1.to_owned(), arg2.to_owned()))
                }
                _ => Err(RedisCommandError::DismatchedArgsNum),
            },
            s => Err(RedisCommandError::UnknownCommand(s.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_echo_command() {
        let input = b"*2\r\n$4\r\necho\r\n$3\r\nhey\r\n";
        let c = RedisCommand::try_from(input.as_slice()).unwrap();
        assert_eq!(RedisCommand::Echo(Some(Cow::Borrowed("hey".as_bytes()))), c);
    }
}
