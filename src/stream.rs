use crate::command::{RedisCommand, RedisCommandError};
use crate::parser::RedisValueParserError;
use crate::r#type::RedisType;
use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;

#[derive(PartialEq, Debug)]
pub enum StreamError {
    Io(ErrorKind),
    Parser(RedisValueParserError),
    Commond(RedisCommandError),
}

pub trait TcpStreamExt<'a> {
    fn write_redis_type(&mut self, command: RedisType) -> Result<(), StreamError>;
    fn write_command(&mut self, command: RedisCommand) -> Result<(), StreamError>;
    fn write2(&mut self, buffer: &[u8]) -> Result<(), StreamError>;
    fn read_rdb(&mut self) -> Result<RedisType<'a>, StreamError>;
    fn read_type(&mut self) -> Result<Option<RedisType<'a>>, StreamError>;
    fn read_command(&mut self) -> Result<Option<RedisCommand>, StreamError>;
    fn read_as_vec(&mut self) -> Result<Option<Vec<u8>>, StreamError>;
}

impl<'a> TcpStreamExt<'a> for TcpStream {
    fn read_as_vec(&mut self) -> Result<Option<Vec<u8>>, StreamError> {
        let mut buffer: [u8; 1024] = [0; 1024];
        match self.read(&mut buffer) {
            Ok(0) => Ok(None),
            Ok(n) => {
                println!("received buffer length: {}", n);
                Ok(Some(buffer[0..n].to_vec()))
            }
            Err(e) => Err(StreamError::Io(e.kind())),
        }
    }

    fn read_command(&mut self) -> Result<Option<RedisCommand>, StreamError> {
        match self.read_as_vec() {
            Ok(b) => match b {
                Some(b) => {
                    let c = RedisCommand::try_from(b.as_slice());
                    match c {
                        Ok(c) => {
                            println!("command readed: {:?}", c);
                            Ok(Some(c.to_owned()))
                        }
                        Err(e) => Err(StreamError::Commond(e)),
                    }
                }
                None => Ok(None),
            },
            Err(e) => Err(e),
        }
    }

    fn read_type(&mut self) -> Result<Option<RedisType<'a>>, StreamError> {
        match self.read_as_vec() {
            Ok(b) => match b {
                Some(b) => {
                    let c = RedisType::try_from(b.as_slice());
                    match c {
                        Ok(c) => {
                            println!("value read: {:?}", c);
                            Ok(Some(c.to_owned()))
                        }
                        Err(e) => Err(StreamError::Parser(e)),
                    }
                }
                None => Ok(None),
            },
            Err(e) => Err(e),
        }
    }

    fn read_rdb(&mut self) -> Result<RedisType<'a>, StreamError> {
        match self.read_as_vec() {
            Ok(b) => match b {
                Some(b) => {
                    assert_eq!(b'$', b[0]);
                    let mut t = 1;
                    while b[t] != b'\n' {
                        t += 1
                    }
                    Ok(RedisType::Rdb(b[t + 1..].to_vec()))
                }
                None => Err(StreamError::Io(ErrorKind::InvalidData)),
            },
            Err(e) => Err(e),
        }
    }

    fn write2(&mut self, buffer: &[u8]) -> Result<(), StreamError> {
        self.write_all(buffer)
            .map_err(|r| StreamError::Io(r.kind()))
    }

    fn write_command(&mut self, command: RedisCommand) -> Result<(), StreamError> {
        println!("command send: {:?}", command);
        let buffer: Vec<u8> = command.into();
        self.write2(buffer.as_slice())
    }

    fn write_redis_type(&mut self, command: RedisType) -> Result<(), StreamError> {
        let buffer: Vec<u8> = command.into();
        self.write2(buffer.as_slice())
    }
}
