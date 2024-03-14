use crate::value::RedisValue;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use structopt::StructOpt;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;

#[derive(Debug, StructOpt, Clone)]
#[structopt(name = "redis")]
pub struct RedisConfig {
    #[structopt(long, default_value = "localhost")]
    pub host: String,
    #[structopt(long, default_value = "6379")]
    pub port: u32,
    #[structopt(long)]
    pub replicaof: Option<Vec<String>>,
}

impl RedisConfig {
    pub fn get_replica_of(self) -> Option<(String, usize)> {
        match self.replicaof {
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
pub struct ClientHandler {
    pub id: String,
    pub writer: Arc<RwLock<Sender<RedisValue>>>,
    pub reader: Arc<RwLock<Receiver<RedisValue>>>,
}

#[derive(Debug, Clone)]
pub struct StoreItem {
    pub value: RedisValue,
    pub expired_at: u64,
}

#[derive(Debug, Clone)]
pub struct Redis {
    pub config: RedisConfig,

    pub store: Arc<RwLock<HashMap<String, StoreItem>>>,

    pub clients: Arc<RwLock<Vec<TcpStream>>>,
    pub handlers: Arc<RwLock<HashMap<String, ClientHandler>>>,

    pub replicas: Arc<RwLock<HashSet<String>>>,
}

impl Redis {
    pub fn new() -> Self {
        let config = RedisConfig::from_args();
        Redis {
            config: config.clone(),
            store: Arc::new(RwLock::new(HashMap::new())),

            clients: Arc::new(RwLock::new(Vec::new())),
            handlers: Arc::new(RwLock::new(HashMap::new())),
            replicas: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    pub fn host(&self) -> String {
        format!("{}:{}", self.config.host, self.config.port)
    }
}
