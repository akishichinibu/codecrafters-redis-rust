use crate::client::ClientChannel;
use crate::value::RedisValue;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use structopt::StructOpt;
use tokio::sync::RwLock;

#[derive(Debug, StructOpt, Clone)]
#[structopt(name = "redis")]
pub struct RedisConfig {
    #[structopt(long, default_value = "127.0.0.1")]
    pub host: String,
    #[structopt(long, default_value = "6379")]
    pub port: u32,
    #[structopt(long)]
    pub replicaof: Option<Vec<String>>,
}

impl RedisConfig {
    pub fn get_replica_of(&self) -> Option<(String, usize)> {
        match &self.replicaof {
            Some(args) => {
                let host = args[0].to_string();
                let port = args[1].parse().unwrap();
                Some((host, port))
            }
            None => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StoreItem {
    pub value: RedisValue,
    pub expired_at: u64,
}

#[derive(Debug, Clone)]
pub struct Redis {
    pub config: Arc<RedisConfig>,

    pub store: Arc<RwLock<HashMap<String, StoreItem>>>,

    pub channels: Arc<RwLock<HashMap<String, Arc<RwLock<ClientChannel>>>>>,
    pub replicas: Arc<RwLock<HashMap<String, usize>>>,
}

impl Redis {
    pub fn new() -> Self {
        let config = RedisConfig::from_args();
        Redis {
            config: Arc::new(config),
            store: Arc::new(RwLock::new(HashMap::new())),

            channels: Arc::new(RwLock::new(HashMap::new())),
            replicas: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn host(&self) -> String {
        format!("{}:{}", self.config.host, self.config.port)
    }
}
