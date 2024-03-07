use std::borrow::Cow;
use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread::{self, sleep};
use std::time::Duration;

use r#type::RedisType;
use stream::StreamError;
use structopt::StructOpt;

use crate::command::RedisCommand;
use crate::info::ReplicationInfo;
use crate::stream::TcpStreamExt;
mod command;
mod config;
mod info;
mod parser;
mod stream;
mod r#type;
mod utilities;

struct Redis<'a> {
    host: String,
    port: usize,
    config: config::Config,
    store: Arc<Mutex<HashMap<String, (RedisType<'a>, u64)>>>,
    replicas: Arc<Mutex<HashMap<usize, Arc<Mutex<TcpStream>>>>>,
}

fn launch() {
    let config = config::Config::from_args();
    println!("{:?}", config);

    let mut redis = Redis::new(&config);
    println!("listen to {}", redis.url());
    let listener = TcpListener::bind(redis.url()).unwrap();

    let config = redis.config.clone();
    let store = redis.store.clone();
    let replicas = redis.replicas.clone();

    let t = match config.clone().get_replica_of() {
        Some(_) => {
            // current instance is a replica node
            match redis.handle_replica_handshake() {
                Ok(c) => {
                    let master_connection = Arc::new(Mutex::new(c));

                    println!("handle replica handshake successfully");
                    let t = thread::spawn(move || {
                        println!("start for thread {:?}", std::thread::current().id());
                        loop {
                            let mut master_ref = master_connection.lock().unwrap();
                            let config = redis.config.clone();
                            let store = redis.store.clone();
                            let replicas = redis.replicas.clone();
                            match master_ref.read_command() {
                                Err(e) => panic!("{:?}", e),
                                Ok(b) => match b {
                                    None => break,
                                    Some(command) => {
                                        let response = Redis::execute_command(
                                            config.clone(),
                                            store.clone(),
                                            replicas.clone(),
                                            master_connection.clone(),
                                            command,
                                        )
                                        .unwrap();
                                        for r in response {
                                            master_ref.write_redis_type(r).unwrap();
                                        }
                                    }
                                },
                            }
                        }
                        println!("thread {:?} end", std::thread::current().id());
                    });
                    Some(t)
                }
                Err(e) => {
                    println!("error when connect to master {:?}", e);
                    None
                }
            }
        }
        None => None,
    };

    for client in listener.incoming() {
        match client {
            Ok(client) => {
                client
                    .set_read_timeout(Some(Duration::from_millis(1000)))
                    .unwrap();
                let client = Arc::new(Mutex::new(client));
                let config = config.clone();
                let store = store.clone();
                let replicas = replicas.clone();
                thread::spawn(move || {
                    println!("start for thread {:?}", std::thread::current().id());
                    loop {
                        let command = {
                            let mut client_ref = client.lock().unwrap();
                            match client_ref.read_command() {
                                Err(e) => match e {
                                    StreamError::Io(ErrorKind::WouldBlock) => {
                                        drop(client_ref);
                                        sleep(Duration::from_millis(100));
                                        continue;
                                    }
                                    _ => {
                                        panic!("{:?}", e)
                                    }
                                },
                                Ok(b) => match b {
                                    None => break,
                                    Some(command) => command.to_owned(),
                                },
                            }
                        };

                        let response = Redis::execute_command(
                            config.clone(),
                            store.clone(),
                            replicas.clone(),
                            client.clone(),
                            command,
                        )
                        .unwrap();

                        for r in response {
                            {
                                let mut client_ref = client.lock().unwrap();
                                client_ref.write_redis_type(r).unwrap();
                            }
                        }
                    }
                    println!("thread {:?} end", std::thread::current().id());
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    t.and_then(|r| Some(r.join()));
}

impl<'a> Redis<'a> {
    fn new(config: &config::Config) -> Self {
        let c = config.clone();
        Redis {
            host: "127.0.0.1".to_string(),
            port: c.port as usize,
            config: config.clone(),
            store: Arc::new(Mutex::new(HashMap::new())),
            replicas: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn url(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    pub fn brocast_replicas(
        replicas: Arc<Mutex<HashMap<usize, Arc<Mutex<TcpStream>>>>>,
        command: &RedisCommand<'_>,
    ) {
        let clients: Vec<Arc<Mutex<TcpStream>>> = {
            let replicas_ref = replicas.lock().unwrap();
            replicas_ref.iter().map(|(_, r)| r.clone()).collect()
        };

        for replica in clients {
            {
                let mut replica_ref = replica.lock().unwrap();
                println!("get ref for {}", 1111);
                replica_ref.write_command(command.clone()).unwrap();
                replica_ref.read_type().unwrap();
            }
            println!("broadcast to port done {}, {:?}", 1111, command);
        }
    }

    pub fn execute_command(
        config: config::Config,
        store: Arc<Mutex<HashMap<String, (RedisType<'a>, u64)>>>,
        replicas: Arc<Mutex<HashMap<usize, Arc<Mutex<TcpStream>>>>>,
        client: Arc<Mutex<TcpStream>>,
        command: RedisCommand<'_>,
    ) -> Result<Vec<RedisType<'static>>, StreamError> {
        let c = command.to_owned();
        match command {
            RedisCommand::Ping => Ok(vec![RedisType::simple_string("PONG")]),
            RedisCommand::Echo(value) => match value {
                None => Ok(vec![RedisType::null_bulk_string()]),
                Some(v) => Ok(vec![RedisType::BulkString(Some(Cow::Owned(v.to_vec())))]),
            },
            RedisCommand::Get(key) => {
                let mut store = store.lock().unwrap();
                let key = String::from_utf8(key.to_vec()).unwrap();
                match store.get(&key) {
                    Some((value, expired_at)) => {
                        if *expired_at == 0 || *expired_at >= utilities::now() {
                            Ok(vec![value.to_owned()])
                        } else {
                            store.remove(&key);
                            Ok(vec![RedisType::null_bulk_string()])
                        }
                    }
                    None => Ok(vec![RedisType::null_bulk_string()]),
                }
            }
            RedisCommand::Set(key, value, px) => {
                let key = String::from_utf8(key.to_vec()).unwrap();
                let value = RedisType::BulkString(Some(Cow::from(value.into_owned())));
                let expired_at = match px {
                    None => 0,
                    Some(px) => px + utilities::now(),
                };
                {
                    let mut store = store.lock().unwrap();
                    store.insert(key, (value, expired_at));
                }
                if config.get_replica_of() == None {
                    Redis::brocast_replicas(replicas, &c);
                }
                Ok(vec![RedisType::simple_string("OK")])
            }
            RedisCommand::Info => Ok(vec![ReplicationInfo {
                role: match config.clone().get_replica_of() {
                    Some((_, _)) => "slave".to_string(),
                    None => "master".to_string(),
                },
            }
            .into()]),
            RedisCommand::Replconf(_, _) => Ok(vec![RedisType::simple_string("OK")]),
            RedisCommand::Psync(_, _) => {
                let port: usize = 11111;
                println!("get replica from port {}", port);
                {
                    let mut replica_ref = replicas.lock().unwrap();
                    replica_ref.insert(port, client);
                    println!("replicas: {:?}", replica_ref);
                }
                Ok(vec![
                    RedisType::simple_string("FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0"),
                    RedisType::Rdb(
                        base64::decode("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==").unwrap(),
                    ),
                ])
            }
        }
    }

    pub fn handle_replica_handshake(&mut self) -> Result<TcpStream, StreamError> {
        let config = self.config.clone();
        let (master_host, master_port) = config.clone().get_replica_of().unwrap();
        let mut client = match TcpStream::connect(&format!("{}:{}", master_host, master_port)) {
            Ok(c) => c,
            Err(e) => return Err(StreamError::Io(e.kind())),
        };
        client.write_command(RedisCommand::Ping).unwrap_or_default();

        client.read_type().unwrap_or_default();

        client
            .write_command(RedisCommand::Replconf(
                Cow::Borrowed("listening-port".as_ref()),
                Cow::Borrowed(config.clone().port.to_string().as_bytes()),
            ))
            .unwrap();

        client.read_type().unwrap_or_default();

        client
            .write_command(RedisCommand::Replconf(
                Cow::Borrowed("capa".as_ref()),
                Cow::Borrowed("psync2".as_ref()),
            ))
            .unwrap_or_default();

        client.read_type().unwrap_or_default();

        client
            .write_command(RedisCommand::Psync(
                Cow::Borrowed("?".as_ref()),
                Cow::Borrowed("-1".as_ref()),
            ))
            .unwrap_or_default();

        client.read_type().unwrap_or_default();

        client.read_rdb().unwrap_or(RedisType::Rdb(Vec::new()));
        Ok(client)
    }
}

fn main() {
    launch();
}
