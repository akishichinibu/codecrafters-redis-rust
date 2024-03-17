use std::collections::VecDeque;

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
    ReadingSimpleString {
        content: Vec<u8>,
    },
    ReadingArray {
        length: LengthState,
        collected: usize,
    },
    ReadingRdb {
        length: LengthState,
        content: Vec<u8>,
    },
}

#[derive(PartialEq, Debug, Clone)]
pub enum MessageParserStateError {
    UnexceptedToken(u8, usize, u32),
    UnexceptedValue(String),
}

impl MessageParserState {
    fn reading_length() -> MessageParserState {
        MessageParserState::ReadingLength {
            length: None,
            heading_zero: false,
        }
    }

    fn reading_bulk_string() -> MessageParserState {
        MessageParserState::ReadingBulkString {
            length: LengthState::Reading,
            content: Vec::new(),
        }
    }

    fn reading_simple_string() -> MessageParserState {
        MessageParserState::ReadingSimpleString {
            content: Vec::new(),
        }
    }
}

trait VecExt<'a, U, T>
where
    U: 'a,
{
    fn push_in_reverse(&mut self, elements: T);
}

impl<'a, U, T> VecExt<'a, U, T> for Vec<U>
where
    U: Clone,
    U: 'a,
    T: Into<Vec<U>>,
{
    fn push_in_reverse(&mut self, elements: T) {
        elements
            .into()
            .iter()
            .rev()
            .for_each(|s| self.push(s.clone()));
    }
}

pub struct RedisValueParser {
    bytes_buffer: VecDeque<u8>,
    value_buffer: Vec<RedisValue>,
    state_stack: Vec<MessageParserState>,
}

impl RedisValueParser {
    pub fn new() -> RedisValueParser {
        RedisValueParser {
            bytes_buffer: VecDeque::new(),
            state_stack: Vec::new(),
            value_buffer: Vec::new(),
        }
    }

    pub fn append(&mut self, input: &[u8]) {
        self.bytes_buffer.extend(input);
    }

    pub fn buffer_len(&self) -> usize {
        self.bytes_buffer.len()
    }

    fn parse_loop(&mut self) -> Result<(Option<RedisValue>, usize), MessageParserStateError> {
        let mut input = self.bytes_buffer.iter().enumerate();
        let mut last_pos: usize = 0;

        loop {
            let state = if let Some(state) = self.state_stack.pop() {
                state
            } else {
                break;
            };

            // println!(
            //     "* ; state: {:?}; stack: {:?}; buffer: {:?}",
            //     state, self.state_stack, self.value_buffer
            // );
            match state {
                MessageParserState::Initial => match input.next() {
                    Some((t, b'$')) => {
                        // println!(
                        //     "[parser] {} {:?} {:?}: found an string",
                        //     t, state, self.state_stack
                        // );
                        last_pos = t;
                        self.state_stack
                            .push(MessageParserState::reading_bulk_string());
                    }
                    Some((t, b'*')) => {
                        // println!(
                        //     "[parser] {:?} {:?}: found an array",
                        //     state, self.state_stack
                        // );
                        last_pos = t;
                        self.state_stack.push(MessageParserState::ReadingArray {
                            length: LengthState::Reading,
                            collected: 0,
                        });
                    }
                    Some((t, b'+')) => {
                        // println!(
                        //     "[parser] {:?} {:?}: found an simple string",
                        //     state, self.state_stack
                        // );
                        last_pos = t;
                        self.state_stack
                            .push(MessageParserState::reading_simple_string());
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
                        self.state_stack.push_in_reverse(vec![
                            MessageParserState::reading_length(),
                            MessageParserState::ReadingBulkString {
                                length: LengthState::Loading,
                                content,
                            },
                        ]);
                    }
                    LengthState::Loading => match self.value_buffer.pop() {
                        Some(RedisValue::Integer(l)) => {
                            self.state_stack.push_in_reverse(vec![
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
                                self.state_stack
                                    .push(MessageParserState::ReadingBulkString {
                                        length,
                                        content,
                                    });
                            } else {
                                let s = RedisValue::bulk_string_from_bytes(content.as_slice());
                                self.value_buffer.push(s);

                                self.state_stack.push_in_reverse(vec![
                                    MessageParserState::WaitForSr,
                                    MessageParserState::WaitForSn,
                                ]);
                            }
                        }
                        None => return Ok((None, last_pos)),
                    },
                },
                MessageParserState::ReadingRdb {
                    length,
                    mut content,
                } => match length {
                    LengthState::Reading => {
                        self.state_stack.push_in_reverse(vec![
                            MessageParserState::reading_length(),
                            MessageParserState::ReadingRdb {
                                length: LengthState::Loading,
                                content,
                            },
                        ]);
                    }
                    LengthState::Loading => match self.value_buffer.pop() {
                        Some(RedisValue::Integer(l)) => {
                            self.state_stack.push_in_reverse(vec![
                                MessageParserState::ReadingRdb {
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
                                self.state_stack
                                    .push(MessageParserState::ReadingRdb { length, content });
                            } else {
                                let s = RedisValue::Rdb(content.to_vec());
                                self.value_buffer.push(s);
                            }
                        }
                        None => return Ok((None, last_pos)),
                    },
                },
                MessageParserState::ReadingArray { length, collected } => match length {
                    LengthState::Reading => {
                        self.state_stack.push_in_reverse(vec![
                            MessageParserState::reading_length(),
                            MessageParserState::ReadingArray {
                                length: LengthState::Loading,
                                collected: 0,
                            },
                        ]);
                    }
                    LengthState::Loading => match self.value_buffer.pop() {
                        Some(RedisValue::Integer(l)) => {
                            self.state_stack.push(MessageParserState::ReadingArray {
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
                            self.state_stack.push_in_reverse(vec![
                                MessageParserState::Initial,
                                MessageParserState::ReadingArray {
                                    length: LengthState::Loaded(length),
                                    collected: collected + 1,
                                },
                            ]);
                        } else {
                            let s = RedisValue::Array(self.value_buffer.drain(0..length).collect());
                            self.value_buffer.push(s);
                        }
                    }
                },
                MessageParserState::ReadingSimpleString { mut content } => match input.next() {
                    Some((_, b'\r')) => {
                        self.value_buffer
                            .push(RedisValue::simple_string_from_bytes(content.as_slice()));
                        self.state_stack.push(MessageParserState::WaitForSn);
                    }
                    Some((_, b)) => {
                        content.push(*b);
                        self.state_stack
                            .push(MessageParserState::ReadingSimpleString { content })
                    }
                    None => return Ok((None, last_pos)),
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
                            b'0'..=b'9' => {
                                self.state_stack.push(MessageParserState::ReadingLength {
                                    length: Some((b - b'0') as usize),
                                    heading_zero,
                                })
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
                                self.state_stack.push(MessageParserState::ReadingLength {
                                    length: Some(length * 10 + (b - b'0') as usize),
                                    heading_zero,
                                });
                            }
                            b'\r' => {
                                self.value_buffer.push(RedisValue::Integer(length));
                                self.state_stack.push(MessageParserState::WaitForSn);
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

        let v = self.value_buffer.pop();
        self.bytes_buffer.drain(0..=last_pos);
        return Ok((v, last_pos));
    }

    pub fn parse_rdb(&mut self) -> Result<(Option<RedisValue>, usize), MessageParserStateError> {
        if let Some(first) = self.bytes_buffer.front() {
            assert_eq!(b'$', *first);
            self.bytes_buffer.pop_front().unwrap();
            self.state_stack.push(MessageParserState::ReadingRdb {
                length: LengthState::Reading,
                content: Vec::new(),
            });
            self.parse_loop()
        } else {
            Ok((None, 0))
        }
    }

    pub fn parse(&mut self) -> Result<(Option<RedisValue>, usize), MessageParserStateError> {
        self.state_stack.push(MessageParserState::Initial);
        self.parse_loop()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser_bulk_string() {
        let input = "$5\r\n12345\r\n$3\r\nxyz\r\n$5\r\nabcde\r\n".as_bytes();
        let mut parser = RedisValueParser::new();

        parser.append(input);
        let (values, t) = parser.parse().unwrap();
        assert_eq!(RedisValue::bulk_string("12345"), values.unwrap());

        let (values, t) = parser.parse().unwrap();
        assert_eq!(RedisValue::bulk_string("xyz"), values.unwrap());

        let (values, t) = parser.parse().unwrap();
        assert_eq!(RedisValue::bulk_string("abcde"), values.unwrap());

        assert!(parser.parse().unwrap().0.is_none());
    }

    #[test]
    fn test_parse_array() {
        let input = "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n$5\r\na".as_bytes();
        let mut parser = RedisValueParser::new();
        parser.append(input);

        let (values, _) = parser.parse().unwrap();

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

    #[test]
    fn test_parse_simple_string() {
        let input = "+HAPPY\r\n".as_bytes();
        let mut parser = RedisValueParser::new();
        parser.append(input);
        let (value, t) = parser.parse().unwrap();
        assert_eq!(Some(RedisValue::SimpleString("HAPPY".into())), value);
    }
}
