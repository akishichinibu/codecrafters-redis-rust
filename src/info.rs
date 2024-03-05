use std::borrow::Cow;

use crate::r#type::RedisType;

pub struct ReplicationInfo {
    pub role: String,
}

impl<'a> Into<RedisType<'a>> for ReplicationInfo {
    fn into(self) -> RedisType<'a> {
        let role = format!("role:{}", self.role);
        let master_replid = format!(
            "master_replid:{}",
            "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        );
        let master_repl_offset = format!("master_repl_offset:{}", 0);
        let content = vec!["# Replication", &role, &master_replid, &master_repl_offset].join("\n");
        RedisType::BulkString(Some(Cow::from(content.into_bytes())))
    }
}
