use crate::value::RedisValue;

#[derive(PartialEq, Debug, Clone)]
enum LengthState {
    Reading,
    Loading,
    Loaded(usize),
}

#[derive(PartialEq, Debug, Clone)]
enum MessageParserState {
    Initial,
    WaitForSr,
    WaitForSn,
    ReadingLength {
        length: Option<usize>,
        heading_zero: bool,
    },
    ReadingBulkString {
        length: LengthState,
        content: Vec<u8>,
    },
    ReadingArray {
        length: LengthState,
        collected: usize,
    },
}

#[derive(PartialEq, Debug, Clone)]
pub enum MessageParserStateError {
    UnexceptedToken(u8, usize, u32),
    UnexceptedTermination,
    UnexceptedValue(String),
    Unknown,
}

impl MessageParserState {
    fn reading_length() -> MessageParserState {
        MessageParserState::ReadingLength {
            length: None,
            heading_zero: false,
        }
    }
}

pub struct RedisValueParser {
    bytes_buffer: Vec<u8>,
    stack: Vec<MessageParserState>,
    buffer: Vec<RedisValue>,
}

impl RedisValueParser {
    pub fn new() -> RedisValueParser {
        RedisValueParser {
            bytes_buffer: Vec::new(),
            stack: Vec::new(),
            buffer: Vec::new(),
        }
    }

    fn push_states_in_reverse<'a, T: Into<Vec<MessageParserState>>>(&mut self, states: T) {
        states
            .into()
            .iter()
            .rev()
            .for_each(|s| self.stack.push(s.to_owned()));
    }

    pub fn append(&mut self, input: &[u8]) {
        self.bytes_buffer.extend_from_slice(input);
    }

    pub fn parse(&mut self) -> Result<(Option<RedisValue>, usize), MessageParserStateError> {
        self.stack.push(MessageParserState::Initial);
        let mut input = self.bytes_buffer.iter().enumerate();
        let mut last_pos: usize = 0;

        loop {
            let state = if let Some(state) = self.stack.pop() {
                state
            } else {
                break;
            };

            println!(
                "* ; state: {:?}; stack: {:?}; buffer: {:?}",
                state, self.stack, self.buffer
            );
            match state {
                MessageParserState::Initial => match input.next() {
                    Some((t, b'$')) => {
                        println!("{} {:?} {:?}: found an string", t, state, self.stack);
                        last_pos = t;
                        self.stack.push(MessageParserState::ReadingBulkString {
                            length: LengthState::Reading,
                            content: Vec::new(),
                        });
                    }
                    Some((t, b'*')) => {
                        println!("{:?} {:?}: found an array", state, self.stack);
                        last_pos = t;
                        self.stack.push(MessageParserState::ReadingArray {
                            length: LengthState::Reading,
                            collected: 0,
                        });
                    }
                    Some((t, eb)) => {
                        return Err(MessageParserStateError::UnexceptedToken(*eb, t, line!()))
                    }
                    None => return Ok((None, last_pos)),
                },
                MessageParserState::ReadingBulkString {
                    length,
                    mut content,
                } => match length {
                    LengthState::Reading => {
                        self.push_states_in_reverse(vec![
                            MessageParserState::reading_length(),
                            MessageParserState::ReadingBulkString {
                                length: LengthState::Loading,
                                content,
                            },
                        ]);
                    }
                    LengthState::Loading => match self.buffer.pop() {
                        Some(RedisValue::Integer(l)) => {
                            self.push_states_in_reverse(vec![
                                MessageParserState::ReadingBulkString {
                                    length: LengthState::Loaded(l),
                                    content,
                                },
                            ]);
                        }
                        _ => {
                            return Err(MessageParserStateError::UnexceptedValue(format!(
                                "Except integer at {}",
                                last_pos,
                            )))
                        }
                    },
                    LengthState::Loaded(l) => match input.next() {
                        Some((t, b)) => {
                            content.push(*b);
                            last_pos = t;
                            if content.len() < l {
                                self.stack.push(MessageParserState::ReadingBulkString {
                                    length,
                                    content,
                                });
                            } else {
                                let s = RedisValue::bulk_string_from_bytes(content.as_slice());
                                self.buffer.push(s);

                                self.push_states_in_reverse(vec![
                                    MessageParserState::WaitForSr,
                                    MessageParserState::WaitForSn,
                                ]);
                            }
                        }
                        None => return Ok((None, last_pos)),
                    },
                },
                MessageParserState::ReadingArray { length, collected } => match length {
                    LengthState::Reading => {
                        self.push_states_in_reverse(vec![
                            MessageParserState::reading_length(),
                            MessageParserState::ReadingArray {
                                length: LengthState::Loading,
                                collected: 0,
                            },
                        ]);
                    }
                    LengthState::Loading => match self.buffer.pop() {
                        Some(RedisValue::Integer(l)) => {
                            self.stack.push(MessageParserState::ReadingArray {
                                length: LengthState::Loaded(l),
                                collected,
                            });
                        }
                        _ => {
                            return Err(MessageParserStateError::UnexceptedValue(format!(
                                "Except integer at {}",
                                last_pos,
                            )))
                        }
                    },
                    LengthState::Loaded(length) => {
                        if collected < length {
                            self.push_states_in_reverse(vec![
                                MessageParserState::Initial,
                                MessageParserState::ReadingArray {
                                    length: LengthState::Loaded(length),
                                    collected: collected + 1,
                                },
                            ]);
                        } else {
                            let s = RedisValue::Array(self.buffer.drain(0..length).collect());
                            self.buffer.push(s);
                        }
                    }
                },
                MessageParserState::WaitForSn => match input.next() {
                    Some((t, b'\n')) => last_pos = t,
                    Some((t, eb)) => {
                        return Err(MessageParserStateError::UnexceptedToken(*eb, t, line!()))
                    }
                    None => return Ok((None, last_pos)),
                },
                MessageParserState::WaitForSr => match input.next() {
                    Some((t, b'\r')) => last_pos = t,
                    Some((t, eb)) => {
                        return Err(MessageParserStateError::UnexceptedToken(*eb, t, line!()))
                    }
                    None => return Ok((None, last_pos)),
                },
                MessageParserState::ReadingLength {
                    length,
                    heading_zero,
                } => match length {
                    None => match input.next() {
                        Some((t, b)) => match b {
                            b'0'..=b'9' => self.stack.push(MessageParserState::ReadingLength {
                                length: Some((b - b'0') as usize),
                                heading_zero,
                            }),
                            eb => {
                                return Err(MessageParserStateError::UnexceptedToken(
                                    *eb,
                                    t,
                                    line!(),
                                ))
                            }
                        },
                        None => return Ok((None, last_pos)),
                    },
                    Some(length) => match input.next() {
                        Some((t, b)) => match b {
                            b'0'..=b'9' => {
                                if heading_zero && *b == b'0' {
                                    return Err(MessageParserStateError::UnexceptedToken(
                                        *b,
                                        t,
                                        line!(),
                                    ));
                                }
                                self.stack.push(MessageParserState::ReadingLength {
                                    length: Some(length * 10 + (b - b'0') as usize),
                                    heading_zero,
                                });
                            }
                            b'\r' => {
                                self.buffer.push(RedisValue::Integer(length));
                                self.stack.push(MessageParserState::WaitForSn);
                            }
                            eb => {
                                return Err(MessageParserStateError::UnexceptedToken(
                                    *eb,
                                    t,
                                    line!(),
                                ))
                            }
                        },
                        None => return Ok((None, last_pos)),
                    },
                },
            }
        }

        let v = self.buffer.pop();
        self.bytes_buffer.drain(0..=last_pos);
        return Ok((v, last_pos));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser_bulk_string() {
        let mut input = "$5\r\n12345\r\n$3\r\nxyz\r\n$5\r\nabcde\r\n".as_bytes();
        let mut parser = RedisValueParser::new();

        parser.append(input);
        let (values, t) = parser.parse().unwrap();
        assert_eq!(RedisValue::bulk_string("12345"), values.unwrap());

        let (values, t) = parser.parse().unwrap();
        assert_eq!(RedisValue::bulk_string("xyz"), values.unwrap());

        let (values, t) = parser.parse().unwrap();
        assert_eq!(RedisValue::bulk_string("abcde"), values.unwrap());
        // assert_eq!(
        //     RedisValue::bulk_string("23223"),
        //     parser.parse_bulk_string().unwrap()
        // );
    }

    #[test]
    fn test_parse_array() {
        let mut input = "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n$5\r\na".as_bytes();
        let mut parser = RedisValueParser::new();
        let (values, t) = parser.parse().unwrap();

        match values {
            Some(RedisValue::Array(s)) => {
                assert_eq!(2, s.len());
                assert_eq!(RedisValue::bulk_string("hello"), s[0]);
                assert_eq!(RedisValue::bulk_string("world"), s[1]);
            }
            _ => {
                panic!();
            }
        }

        let (values, t) = parser.parse().unwrap();
        assert_eq!(None, values);

        parser.append("bcde\r\n".as_bytes());
        let (values, t) = parser.parse().unwrap();
        assert_eq!(Some(RedisValue::bulk_string("abcde")), values);
    }

    // #[test]
    // fn test_parse_array2() {
    //     let input = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n".as_bs();
    //     let mut parser = RedisValueParser::new(input);

    //     match parser.parse_next().unwrap() {
    //         RedisValue::Array(s) => {
    //             assert_eq!(3, s.len());
    //             assert_eq!(RedisValue::bulk_string("PSYNC"), s[0]);
    //             assert_eq!(RedisValue::bulk_string("?"), s[1]);
    //             assert_eq!(RedisValue::bulk_string("-1"), s[2]);
    //         }
    //         _ => {
    //             panic!();
    //         }
    //     }
    // }

    #[test]
    fn test_parse_empty_array() {
        let input = "*0\r\n".as_bytes();
        let mut parser = RedisValueParser::new();
        let (value, t) = parser.parse().unwrap();
        assert_eq!(Some(RedisValue::Array(vec![])), value);
        assert_eq!(3, t);
    }

    // #[test]
    // fn test_parse_integer() {
    //     let input = "232\r".as_bs();
    //     let mut parser = RedisValueParser::new(input);
    //     assert_eq!(232 as usize, parser.parse_integer().unwrap());
    // }
}
