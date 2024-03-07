use std::borrow::Cow;

#[derive(PartialEq, Debug, Clone)]
pub enum RedisType<'a> {
    SimpleString(Cow<'a, [u8]>),
    BulkString(Option<Cow<'a, [u8]>>),
    Array(Vec<RedisType<'a>>),
    Rdb(Vec<u8>),
}

impl<'a> RedisType<'a> {
    pub fn bulk_string_from_bytes<S: Into<&'a [u8]>>(s: S) -> RedisType<'a> {
        RedisType::BulkString(Some(Cow::from(s.into())))
    }

    pub fn bulk_string<S: Into<&'a str>>(s: S) -> RedisType<'a> {
        RedisType::BulkString(Some(Cow::from(s.into().as_bytes())))
    }

    pub fn null_bulk_string() -> RedisType<'a> {
        RedisType::BulkString(None)
    }

    pub fn simple_string<S: Into<&'a str>>(s: S) -> RedisType<'a> {
        RedisType::SimpleString(Cow::from(s.into().as_bytes()))
    }

    pub fn to_owned(&self) -> RedisType<'static> {
        match self {
            RedisType::SimpleString(s) => {
                RedisType::SimpleString(Cow::Owned(s.clone().into_owned()))
            }
            RedisType::BulkString(s) => match s {
                Some(s) => RedisType::BulkString(Some(Cow::Owned(s.clone().into_owned()))),
                None => RedisType::BulkString(None),
            },
            RedisType::Array(a) => RedisType::Array(a.iter().map(|r| r.to_owned()).collect()),
            RedisType::Rdb(s) => RedisType::SimpleString(Cow::Owned(s.clone())),
        }
    }
}

impl<'a> Into<Vec<u8>> for RedisType<'a> {
    fn into(self) -> Vec<u8> {
        let mut buffer: Vec<u8> = Vec::new();
        match self {
            RedisType::SimpleString(s) => {
                buffer.push(b'+');
                buffer.extend_from_slice(&s);
                buffer.extend_from_slice(b"\r\n");
            }
            RedisType::BulkString(s) => {
                match s {
                    Some(s) => {
                        buffer.push(b'$');
                        buffer.extend_from_slice(s.len().to_string().as_bytes());
                        buffer.extend_from_slice(b"\r\n");
                        buffer.extend_from_slice(&s);
                    }
                    None => buffer.extend_from_slice(b"$-1"),
                }
                buffer.extend_from_slice(b"\r\n");
            }
            RedisType::Array(a) => {
                buffer.push(b'*');
                buffer.extend_from_slice(a.len().to_string().as_bytes());
                buffer.extend_from_slice(b"\r\n");
                for s in a {
                    let b: Vec<u8> = s.into();
                    buffer.extend(b);
                }
            }
            RedisType::Rdb(c) => {
                buffer.push(b'$');
                buffer.extend_from_slice(c.len().to_string().as_bytes());
                buffer.extend_from_slice(b"\r\n");
                buffer.extend_from_slice(&c);
            }
        }
        buffer
    }
}

impl<'a> Into<RedisType<'a>> for Vec<RedisType<'a>> {
    fn into(self) -> RedisType<'a> {
        RedisType::Array(self.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_value_to_string() {
        let s1: Vec<u8> = RedisType::bulk_string("abcde").into();
        assert_eq!(b"$5\r\nabcde\r\n", s1.as_slice());
    }

    #[test]
    fn test_redis_value_to_string2() {
        let s1: Vec<u8> = RedisType::bulk_string("abcde").into();
        assert_eq!(b"$5\r\nabcde\r\n", s1.as_slice());
    }
}
