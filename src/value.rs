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

    pub fn simple_string<S: Into<String>>(s: S) -> RedisValue {
        RedisValue::SimpleString(s.into())
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
                header + &s.join("")
            }
        }
    }
}

impl Into<String> for RedisValue {
    fn into(self) -> String {
        (&self).into()
    }
}
