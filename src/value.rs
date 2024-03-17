use std::fmt::Debug;

const CRLF: &[u8; 2] = b"\r\n";

#[derive(PartialEq, Clone)]
pub struct RedisBulkString {
    pub data: Vec<u8>,
}

impl Into<RedisBulkString> for Vec<u8> {
    fn into(self) -> RedisBulkString {
        RedisBulkString { data: self }
    }
}

impl Into<RedisBulkString> for &str {
    fn into(self) -> RedisBulkString {
        self.as_bytes().to_vec().into()
    }
}

impl Into<String> for &RedisBulkString {
    fn into(self) -> String {
        String::from_utf8(self.data.to_owned()).unwrap()
    }
}

impl Into<RedisValue> for &RedisBulkString {
    fn into(self) -> RedisValue {
        RedisValue::BulkString(Some(RedisBulkString {
            data: self.data.clone(),
        }))
    }
}

impl Debug for RedisBulkString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            String::from_utf8_lossy(self.data.as_slice())
                .replace("\r", "\\r")
                .replace("\n", "\\n")
        )
    }
}

#[derive(PartialEq, Clone)]
pub enum RedisValue {
    SimpleString(String),
    BulkString(Option<RedisBulkString>),
    Array(Vec<RedisValue>),
    Integer(usize),
    Rdb(Vec<u8>),
}

impl Debug for RedisValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisValue::SimpleString(s) => write!(f, "SimpleString[{}]", s),
            RedisValue::BulkString(s) => match s {
                None => write!(f, "BulkString[nil]"),
                Some(s) => write!(f, "BulkString[{:?}]", s),
            },
            RedisValue::Array(a) => {
                let elements: Vec<String> = a.iter().map(|r| format!("{:?}", r)).collect();
                write!(f, "Array[{}]", elements.join(", "))
            }
            RedisValue::Integer(s) => write!(f, "Integer[{}]", s),
            RedisValue::Rdb(content) => write!(f, "Rdb[{:?}]", content),
        }
    }
}

impl RedisValue {
    pub fn bulk_string<'a, S: Into<&'a str>>(s: S) -> RedisValue {
        RedisValue::bulk_string_from_bytes(s.into().as_bytes())
    }

    pub fn bulk_string_from_bytes<'a, S: Into<&'a [u8]>>(s: S) -> RedisValue {
        RedisValue::BulkString(Some(s.into().to_vec().into()))
    }

    pub fn null_bulk_string() -> RedisValue {
        RedisValue::BulkString(None)
    }

    pub fn simple_string<'a, S: Into<&'a str>>(s: S) -> RedisValue {
        RedisValue::SimpleString(s.into().to_string())
    }

    pub fn simple_string_from_bytes<'a, S: Into<&'a [u8]>>(s: S) -> RedisValue {
        RedisValue::SimpleString(String::from_utf8(s.into().to_vec()).unwrap())
    }
}

impl Into<Vec<u8>> for &RedisValue {
    fn into(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = Vec::new();
        match self {
            RedisValue::SimpleString(s) => {
                buffer.push(b'+');
                buffer.extend_from_slice(&s.as_bytes());
                buffer.extend_from_slice(CRLF);
            }
            RedisValue::BulkString(s) => {
                buffer.push(b'$');
                match s {
                    Some(s) => {
                        buffer.extend_from_slice(s.data.len().to_string().as_bytes());
                        buffer.extend_from_slice(CRLF);
                        buffer.extend_from_slice(&s.data);
                    }
                    None => buffer.extend_from_slice(b"-1"),
                }
                buffer.extend_from_slice(CRLF);
            }
            RedisValue::Array(a) => {
                buffer.push(b'*');
                buffer.extend_from_slice(a.len().to_string().as_bytes());
                buffer.extend_from_slice(CRLF);
                for s in a {
                    let b: Vec<u8> = s.into();
                    buffer.extend(b);
                }
            }
            RedisValue::Rdb(c) => {
                buffer.push(b'$');
                buffer.extend_from_slice(c.len().to_string().as_bytes());
                buffer.extend_from_slice(CRLF);
                buffer.extend_from_slice(&c);
            }
            RedisValue::Integer(i) => {
                buffer.push(b':');
                buffer.extend_from_slice(i.to_string().as_bytes());
                buffer.extend_from_slice(CRLF);
            }
        }
        buffer
    }
}

impl Into<RedisValue> for Vec<RedisValue> {
    fn into(self) -> RedisValue {
        RedisValue::Array(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_value_to_string() {
        let s1: Vec<u8> = (&RedisValue::bulk_string("abcde")).into();
        assert_eq!(b"$5\r\nabcde\r\n", s1.as_slice());
    }

    #[test]
    fn test_redis_value_to_string2() {
        let s1: Vec<u8> = (&RedisValue::bulk_string("abcde")).into();
        assert_eq!(b"$5\r\nabcde\r\n", s1.as_slice());
    }
}
