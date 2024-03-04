use std::time::{SystemTime, UNIX_EPOCH};

pub fn now() -> u64 {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let now = now.as_secs() * 1000 + now.subsec_millis() as u64;
    now
}
