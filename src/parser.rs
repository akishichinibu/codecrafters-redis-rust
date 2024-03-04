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

#[derive(PartialEq, Debug)]
pub enum MessageParserError {
    UnexceptedToken(u8),
    UnexceptedTermination,
}

#[derive(PartialEq, Debug, Clone)]
pub enum RedisValue {
    SimpleString(String),
    BulkString(Option<String>),
    Array(Vec<RedisValue>),
}

impl RedisValue {
    pub fn bulk_string<S: Into<String>>(s: S) -> RedisValue {
        RedisValue::BulkString(Some(s.into()))
    }

    pub fn null_bulk_string() -> RedisValue {
        RedisValue::BulkString(None)
    }
}

impl Into<String> for &RedisValue {
    fn into(self) -> String {
        match self {
            RedisValue::SimpleString(s) => format!("+{}\r\n", s),
            RedisValue::BulkString(s) => match s {
                Some(s) => format!("${}\r\n{}\r\n", s.len(), s),
                None => "$-1\r\n".to_string(),
            },
            RedisValue::Array(a) => {
                let header = format!("*{}\r\n", a.len());
                let s: Vec<String> = a.iter().map(|r| r.into()).collect();
                header + &s.join("\r\n")
            }
        }
    }
}

impl Into<String> for RedisValue {
    fn into(self) -> String {
        (&self).into()
    }
}

pub struct MessageParser<'a> {
    state: MessageParserState,
    buffer: &'a [u8],
    t: usize,
}

impl<'a> MessageParser<'a> {
    pub fn new(buffer: &[u8]) -> MessageParser {
        MessageParser {
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

    fn get_range_as_string(&mut self, offset: usize) -> Option<String> {
        if self.t + offset < self.buffer.len() {
            let s = String::from_utf8(self.buffer[self.t..self.t + offset].to_vec()).unwrap();
            self.t += offset;
            Some(s)
        } else {
            None
        }
    }

    pub(crate) fn parse_integer(&mut self) -> Result<usize, MessageParserError> {
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
                            return Err(MessageParserError::UnexceptedToken(c));
                        }
                    },
                    None => {
                        return Err(MessageParserError::UnexceptedTermination);
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
                                    return Err(MessageParserError::UnexceptedToken(c));
                                }
                                _buffer.push(c);
                            }
                            _ => {
                                return Err(MessageParserError::UnexceptedToken(c));
                            }
                        },
                        None => {
                            return Err(MessageParserError::UnexceptedTermination);
                        }
                    }
                }
                _ => {}
            }
        }
    }

    pub(crate) fn parse_bulk_string(&mut self) -> Result<RedisValue, MessageParserError> {
        let mut _length: usize = 0;

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
                        return Err(MessageParserError::UnexceptedTermination);
                    }
                    _ => {}
                },
                MessageParserState::WaitForSn => match self.get_and_move_next() {
                    Some(b'\n') => {
                        self.state = MessageParserState::WaitForData(_length);
                    }
                    Some(b) => return Err(MessageParserError::UnexceptedToken(b)),
                    None => {
                        return Err(MessageParserError::UnexceptedTermination);
                    }
                },
                MessageParserState::WaitForData(l) => match self.get_range_as_string(l) {
                    Some(s) => {
                        return Ok(RedisValue::bulk_string(s));
                    }
                    None => return Err(MessageParserError::UnexceptedTermination),
                },
                _ => {}
            }
        }
    }

    pub(crate) fn parse_simple_string(&mut self) -> Result<RedisValue, MessageParserError> {
        let mut _buffer: Vec<u8> = Vec::new();

        self.state = MessageParserState::WaitForSimpleString;
        loop {
            match self.state {
                MessageParserState::WaitForSimpleString => match self.get_and_move_next() {
                    Some(b'\r') => {
                        self.state = MessageParserState::WaitForSn;
                    }
                    Some(c) => {
                        _buffer.push(c);
                    }
                    None => {
                        return Err(MessageParserError::UnexceptedTermination);
                    }
                },
                MessageParserState::WaitForSn => match self.get_and_move_next() {
                    Some(b'\n') => {
                        return Ok(RedisValue::SimpleString(
                            String::from_utf8(_buffer).unwrap(),
                        ))
                    }
                    Some(b) => return Err(MessageParserError::UnexceptedToken(b)),
                    None => {
                        return Err(MessageParserError::UnexceptedTermination);
                    }
                },
                _ => {}
            }
        }
    }

    pub(crate) fn parse_array(&mut self) -> Result<RedisValue, MessageParserError> {
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
                MessageParserState::WaitForSr => match self.get_and_move_next() {
                    Some(b'\r') => {
                        self.state = MessageParserState::WaitForSn;
                    }
                    Some(b) => return Err(MessageParserError::UnexceptedToken(b)),
                    None => {
                        return Err(MessageParserError::UnexceptedTermination);
                    }
                },
                MessageParserState::WaitForSn => match self.get_and_move_next() {
                    Some(b'\n') => {
                        self.state = MessageParserState::WaitForArrayElement(_size);
                    }
                    Some(b) => return Err(MessageParserError::UnexceptedToken(b)),
                    None => {
                        return Err(MessageParserError::UnexceptedTermination);
                    }
                },
                MessageParserState::WaitForArrayElement(s) => match s {
                    0 => {
                        return Ok(RedisValue::Array(_elements));
                    }
                    _ => match self.parse() {
                        Ok(v) => {
                            println!("element rest {}: {:?}", _size, v);
                            _elements.push(v);
                            self.state = MessageParserState::WaitForSr;
                            _size -= 1;
                        }
                        Err(e) => return Err(e),
                    },
                },
                _ => {}
            }
        }
    }

    pub fn parse(&mut self) -> Result<RedisValue, MessageParserError> {
        self.state = MessageParserState::Init;

        match self.get() {
            Some(b'*') => self.parse_array(),
            Some(b'$') => self.parse_bulk_string(),
            Some(b'+') => self.parse_simple_string(),
            Some(b) => Err(MessageParserError::UnexceptedToken(b)),
            None => Err(MessageParserError::UnexceptedTermination),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_array() {
        let input = "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n".as_bytes();
        let mut parser = MessageParser::new(input);

        match parser.parse().unwrap() {
            RedisValue::Array(s) => {
                assert_eq!(2, s.len());
                assert_eq!(RedisValue::bulk_string("hello"), s[0]);
                assert_eq!(RedisValue::bulk_string("world"), s[1]);
            }
            _ => {
                panic!();
            }
        }
    }

    #[test]
    fn test_parse_empty_array() {
        let input = "*0\r\n".as_bytes();
        let mut parser = MessageParser::new(input);

        match parser.parse().unwrap() {
            RedisValue::Array(s) => {
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
        let mut parser = MessageParser::new(input);
        assert_eq!(232 as usize, parser.parse_integer().unwrap());
    }

    #[test]
    fn test_parser_bulk_string() {
        let input = "$5\r\n23223\r\n".as_bytes();
        let mut parser = MessageParser::new(input);
        assert_eq!(
            RedisValue::bulk_string("23223".to_string()),
            parser.parse_bulk_string().unwrap()
        );
    }

    #[test]
    fn test_redis_value_to_string() {
        let s1: String = RedisValue::bulk_string("abcde".to_lowercase()).into();
        assert_eq!("$5\r\nabcde\r\n", s1);
    }
}
