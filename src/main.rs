use std::net::TcpListener;
use std::thread;

use command::RedisCommandError;
use parser::RedisValueParserError;
use structopt::StructOpt;

use crate::handler::StreamHandler;
mod command;
mod config;
mod handler;
mod info;
mod parser;
mod r#type;
mod utilities;

struct Redis {
    host: String,
    port: usize,
    master: Option<(String, usize)>,
    config: config::Config,
}

impl Redis {
    fn new(config: &config::Config) -> Self {
        let c = config.clone();
        Redis {
            host: "127.0.0.1".to_string(),
            port: c.port as usize,
            master: c.get_replica_of(),
            config: config.clone(),
        }
    }

    pub fn url(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

#[derive(PartialEq, Debug)]
pub enum HandlerError {
    Io(String),
    Parser(RedisValueParserError),
    Commond(RedisCommandError),
    Other,
}

impl<'a> Redis {
    fn launch(&mut self) {
        println!("listen to {}", self.url());
        let listener = TcpListener::bind(self.url()).unwrap();

        match &self.master {
            Some((master_host, master_port)) => {
                StreamHandler::replica_handshake(self.config.port, master_host, *master_port as u32)
            }
            None => {}
        }

        for stream in listener.incoming() {
            match stream {
                Ok(_stream) => {
                    let c = self.config.clone();
                    thread::spawn(move || {
                        let mut handler = handler::StreamHandler::new(_stream);
                        handler.server_handle(&c).unwrap();
                    });
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        }
    }
}

fn main() {
    let config = config::Config::from_args();
    println!("{:?}", config);
    let mut redis = Redis::new(&config);
    redis.launch();
}
