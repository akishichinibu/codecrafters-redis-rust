use crate::value::RedisValue;

pub struct ReplicationInfo {
    pub role: String,
}

impl Into<RedisValue> for ReplicationInfo {
    fn into(self) -> RedisValue {
        let role = format!("role:{}", self.role);
        let master_replid = format!(
            "master_replid:{}",
            "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
        );
        let master_repl_offset = format!("master_repl_offset:{}", 0);
        RedisValue::bulk_string(
            vec!["# Replication", &role, &master_replid, &master_repl_offset].join("\n"),
        )
    }
}
