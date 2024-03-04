use crate::utilities;
use crate::value::RedisValue;

#[derive(PartialEq, Debug, Clone)]
pub enum RedisCommand {
    Ping,
    Echo(RedisValue),
    Set(RedisValue, RedisValue, Option<u64>),
    Get(RedisValue),
    Info,
}

impl RedisCommand {
    fn set(key: &String, value: RedisValue) -> Self {
        RedisCommand::Set(RedisValue::bulk_string(key), value, None)
    }

    fn set_with_px(key: &String, value: RedisValue, px: u64) -> Self {
        RedisCommand::Set(
            RedisValue::bulk_string(key),
            value,
            Some(px + utilities::now()),
        )
    }

    fn get(key: &String) -> Self {
        RedisCommand::Get(RedisValue::bulk_string(key))
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum RedisCommandError {
    Malform,
    DismatchedArgsNum,
    UnknownCommand,
}

impl TryFrom<RedisValue> for RedisCommand {
    type Error = RedisCommandError;

    fn try_from(value: RedisValue) -> Result<Self, Self::Error> {
        let command_args = match value {
            RedisValue::Array(a) => a,
            _ => return Err(RedisCommandError::Malform),
        };

        let command = match command_args.first() {
            Some(RedisValue::BulkString(Some(s))) => s.to_lowercase(),
            _ => return Err(RedisCommandError::Malform),
        };

        match command.as_str() {
            "ping" => Ok(RedisCommand::Ping),
            "echo" => match command_args.len() {
                2 => Ok(RedisCommand::Echo(command_args[1].clone())),
                _ => Err(RedisCommandError::DismatchedArgsNum),
            },
            "get" => match command_args.len() {
                2 => match &command_args[1] {
                    RedisValue::BulkString(Some(s)) => Ok(RedisCommand::get(s)),
                    _ => return Err(RedisCommandError::Malform),
                },
                _ => Err(RedisCommandError::DismatchedArgsNum),
            },
            "set" => match command_args.len() {
                3 => match &command_args[1] {
                    RedisValue::BulkString(Some(s)) => {
                        Ok(RedisCommand::set(s, command_args[2].clone()))
                    }
                    _ => return Err(RedisCommandError::Malform),
                },
                5 => {
                    let (key, value) = match &command_args[1] {
                        RedisValue::BulkString(Some(s)) => (s, command_args[2].clone()),
                        _ => return Err(RedisCommandError::Malform),
                    };
                    let px: u64 = match &command_args[4] {
                        RedisValue::BulkString(Some(s)) => s.parse().unwrap(),
                        _ => return Err(RedisCommandError::Malform),
                    };
                    Ok(RedisCommand::set_with_px(&key, value, px))
                }
                _ => Err(RedisCommandError::DismatchedArgsNum),
            },
            "info" => match command_args.len() {
                2 => Ok(RedisCommand::Info),
                _ => Err(RedisCommandError::DismatchedArgsNum),
            },
            _ => Err(RedisCommandError::UnknownCommand),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_echo_command() {
        let s = RedisCommand::try_from(RedisValue::Array(vec![
            RedisValue::bulk_string("echo"),
            RedisValue::bulk_string("hello"),
        ]))
        .unwrap();
        assert_eq!(RedisCommand::Echo(RedisValue::bulk_string("hello")), s);
    }
}
