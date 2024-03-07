use crate::r#type::RedisType;
use std::borrow::Cow;

#[derive(PartialEq, Debug)]
enum MessageParserState {
    Init,
    WaitForSimpleString,
    WaitForBulkString,
    WaitForArray,

    WaitForArrayElement(usize),
    WaitForInteger,
    WaitForIntegerRest(bool),

    WaitForSr,
    WaitForSn,

    WaitForData(usize),
}

#[derive(PartialEq, Debug, Clone)]
pub enum RedisValueParserError {
    UnexceptedToken(u8),
    UnexceptedTermination,
}

pub struct RedisValueParser<'a> {
    state: MessageParserState,
    buffer: &'a [u8],
    t: usize,
}

impl<'a> RedisValueParser<'a> {
    pub fn new(buffer: &[u8]) -> RedisValueParser {
        RedisValueParser {
            state: MessageParserState::Init,
            buffer: buffer,
            t: 0,
        }
    }

    fn get(&self) -> Option<u8> {
        self.buffer.get(self.t).map(|b| *b)
    }

    fn get_and_move_next(&mut self) -> Option<u8> {
        let c = self.get();
        self.t += 1;
        c
    }

    fn get_range_as_slice(&mut self, offset: usize) -> Option<&'a [u8]> {
        if self.t + offset < self.buffer.len() {
            let slice = &self.buffer[self.t..self.t + offset];
            self.t += offset;
            Some(slice)
        } else {
            None
        }
    }

    pub(crate) fn parse_integer(&mut self) -> Result<usize, RedisValueParserError> {
        let mut _buffer: Vec<u8> = Vec::with_capacity(10);

        self.state = MessageParserState::WaitForInteger;

        loop {
            match self.state {
                MessageParserState::WaitForInteger => match self.get_and_move_next() {
                    Some(c) => match c {
                        b'0'..=b'9' => {
                            _buffer.push(c);
                            self.state = MessageParserState::WaitForIntegerRest(c == b'0');
                        }
                        _ => {
                            return Err(RedisValueParserError::UnexceptedToken(c));
                        }
                    },
                    None => {
                        return Err(RedisValueParserError::UnexceptedTermination);
                    }
                },
                MessageParserState::WaitForIntegerRest(is_heading_zero) => {
                    match self.get_and_move_next() {
                        Some(b'\r') => {
                            self.state = MessageParserState::WaitForSn;
                            let value: usize = String::from_utf8_lossy(&_buffer).parse().unwrap();
                            return Ok(value);
                        }
                        Some(c) => match c {
                            b'0'..=b'9' => {
                                if is_heading_zero {
                                    return Err(RedisValueParserError::UnexceptedToken(c));
                                }
                                _buffer.push(c);
                            }
                            _ => {
                                return Err(RedisValueParserError::UnexceptedToken(c));
                            }
                        },
                        None => {
                            return Err(RedisValueParserError::UnexceptedTermination);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    pub(crate) fn parse_bulk_string(&mut self) -> Result<RedisType<'a>, RedisValueParserError> {
        let mut _length: usize = 0;
        let mut buffer: Option<&[u8]> = None;

        self.state = MessageParserState::WaitForBulkString;

        loop {
            match self.state {
                MessageParserState::WaitForBulkString => match self.get_and_move_next() {
                    Some(b'$') => match self.parse_integer() {
                        Ok(l) => {
                            _length = l;
                        }
                        Err(err) => {
                            return Err(err);
                        }
                    },
                    None => {
                        return Err(RedisValueParserError::UnexceptedTermination);
                    }
                    _ => {}
                },
                MessageParserState::WaitForSn => match self.get_and_move_next() {
                    Some(b'\n') => match buffer {
                        None => self.state = MessageParserState::WaitForData(_length),
                        Some(b) => return Ok(RedisType::bulk_string_from_bytes(b)),
                    },

                    Some(b) => return Err(RedisValueParserError::UnexceptedToken(b)),
                    None => {
                        return Err(RedisValueParserError::UnexceptedTermination);
                    }
                },
                MessageParserState::WaitForSr => match self.get_and_move_next() {
                    Some(b'\r') => {
                        self.state = MessageParserState::WaitForSn;
                    }
                    Some(b) => return Err(RedisValueParserError::UnexceptedToken(b)),
                    None => {
                        return Err(RedisValueParserError::UnexceptedTermination);
                    }
                },
                MessageParserState::WaitForData(l) => match self.get_range_as_slice(l) {
                    Some(s) => {
                        buffer = Some(s);
                        self.state = MessageParserState::WaitForSr;
                    }
                    None => return Err(RedisValueParserError::UnexceptedTermination),
                },
                _ => {}
            }
        }
    }

    pub(crate) fn parse_simple_string(&mut self) -> Result<RedisType<'a>, RedisValueParserError> {
        let mut start: usize = 0;

        self.state = MessageParserState::WaitForSimpleString;

        loop {
            match self.state {
                MessageParserState::WaitForSimpleString => match self.get_and_move_next() {
                    Some(b'+') => {
                        self.state = MessageParserState::WaitForData(0);
                        start = self.t;
                    }
                    Some(b) => return Err(RedisValueParserError::UnexceptedToken(b)),
                    None => {
                        return Err(RedisValueParserError::UnexceptedTermination);
                    }
                },
                MessageParserState::WaitForSn => match self.get_and_move_next() {
                    Some(b'\n') => {
                        return Ok(RedisType::SimpleString(Cow::from(
                            &self.buffer[start..self.t],
                        )))
                    }
                    Some(b) => return Err(RedisValueParserError::UnexceptedToken(b)),
                    None => {
                        return Err(RedisValueParserError::UnexceptedTermination);
                    }
                },
                MessageParserState::WaitForData(_) => match self.get_and_move_next() {
                    Some(b'\r') => {
                        self.state = MessageParserState::WaitForSn;
                    }
                    Some(_) => {
                        continue;
                    }
                    None => {
                        return Err(RedisValueParserError::UnexceptedTermination);
                    }
                },
                _ => {}
            }
        }
    }

    pub(crate) fn parse_array(&mut self) -> Result<RedisType<'a>, RedisValueParserError> {
        let mut _size: usize = 0;
        let mut _elements = Vec::new();

        self.state = MessageParserState::WaitForArray;

        loop {
            match self.state {
                MessageParserState::WaitForArray => match self.get_and_move_next() {
                    Some(b'*') => match self.parse_integer() {
                        Ok(l) => {
                            _size = l;
                            println!("the length of the array: {}", _size);
                            self.state = MessageParserState::WaitForSn;
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    },
                    _ => {}
                },
                MessageParserState::WaitForSn => match self.get_and_move_next() {
                    Some(b'\n') => {
                        self.state = MessageParserState::WaitForArrayElement(_size);
                    }
                    Some(b) => return Err(RedisValueParserError::UnexceptedToken(b)),
                    None => {
                        return Err(RedisValueParserError::UnexceptedTermination);
                    }
                },
                MessageParserState::WaitForArrayElement(0) => {
                    return Ok(RedisType::Array(_elements))
                }
                MessageParserState::WaitForArrayElement(_) => match self.parse() {
                    Ok(v) => {
                        _size -= 1;
                        println!("element rest {} at {}: {:?}", _size, self.t, v);
                        _elements.push(v);
                        self.state = MessageParserState::WaitForArrayElement(_size);
                    }
                    Err(e) => return Err(e),
                },
                _ => {}
            }
        }
    }

    pub fn parse(&mut self) -> Result<RedisType<'a>, RedisValueParserError> {
        self.state = MessageParserState::Init;
        // println!("buffer: {:?}", self.buffer[self.t..].to_vec());

        match self.get() {
            Some(b'*') => self.parse_array(),
            Some(b'$') => self.parse_bulk_string(),
            Some(b'+') => self.parse_simple_string(),
            Some(b) => Err(RedisValueParserError::UnexceptedToken(b)),
            None => Err(RedisValueParserError::UnexceptedTermination),
        }
    }
}

impl<'a> TryFrom<&'a [u8]> for RedisType<'a> {
    type Error = RedisValueParserError;
    fn try_from(value: &'a [u8]) -> Result<RedisType<'a>, Self::Error> {
        let mut parser = RedisValueParser::<'a>::new(value);
        parser.parse()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_array() {
        let input = "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_bytes();
        let mut parser = RedisValueParser::new(input);

        match parser.parse().unwrap() {
            RedisType::Array(s) => {
                assert_eq!(2, s.len());
                assert_eq!(RedisType::bulk_string("hello"), s[0]);
                assert_eq!(RedisType::bulk_string("world"), s[1]);
            }
            _ => {
                panic!();
            }
        }
    }

    #[test]
    fn test_parse_array2() {
        let input = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".as_bytes();
        let mut parser = RedisValueParser::new(input);

        match parser.parse().unwrap() {
            RedisType::Array(s) => {
                assert_eq!(3, s.len());
                assert_eq!(RedisType::bulk_string("PSYNC"), s[0]);
                assert_eq!(RedisType::bulk_string("?"), s[1]);
                assert_eq!(RedisType::bulk_string("-1"), s[2]);
            }
            _ => {
                panic!();
            }
        }
    }

    #[test]
    fn test_parse_empty_array() {
        let input = "*0\r\n".as_bytes();
        let mut parser = RedisValueParser::new(input);

        match parser.parse().unwrap() {
            RedisType::Array(s) => {
                assert_eq!(0, s.len());
            }
            _ => {
                panic!();
            }
        }
    }

    #[test]
    fn test_parse_integer() {
        let input = "232\r".as_bytes();
        let mut parser = RedisValueParser::new(input);
        assert_eq!(232 as usize, parser.parse_integer().unwrap());
    }

    #[test]
    fn test_parser_bulk_string() {
        let input = "$5\r\n23223\r\n".as_bytes();
        let mut parser = RedisValueParser::new(input);
        assert_eq!(
            RedisType::bulk_string("23223"),
            parser.parse_bulk_string().unwrap()
        );
    }
}
