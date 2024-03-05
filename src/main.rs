use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

use command::RedisCommand;
use structopt::StructOpt;
use value::RedisValue;

mod command;
mod config;
mod handler;
mod info;
mod parser;
mod utilities;
mod value;

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

impl Redis {
    fn launch(&mut self) {
        println!("listen to {}", self.url());
        let listener = TcpListener::bind(self.url()).unwrap();

        match &self.master {
            Some((host, port)) => {
                let mut client = TcpStream::connect(&format!("{}:{}", host, port)).unwrap();
                let value: RedisValue = RedisCommand::Ping.into();
                let value_str: String = value.into();
                client.write_all(value_str.as_bytes()).unwrap();

                let mut buf: [u8; 1024] = [0; 1024];

                let l1 = client.read(&mut buf).unwrap();
                let c = self.config.clone();
                let response1 = handler::handle_buffer2(&c, buf[0..l1].to_vec()).unwrap();
                println!("{:?}", response1);

                let value: RedisValue = RedisCommand::Replconf(
                    RedisValue::bulk_string("listening-port"),
                    RedisValue::bulk_string(self.config.port.to_string()),
                )
                .into();
                let value_str: String = value.into();
                client.write_all(value_str.as_bytes()).unwrap();

                let l1 = client.read(&mut buf).unwrap();
                let c = self.config.clone();
                let response1 = handler::handle_buffer2(&c, buf[0..l1].to_vec()).unwrap();
                println!("{:?}", response1);

                let value: RedisValue = RedisCommand::Replconf(
                    RedisValue::bulk_string("capa"),
                    RedisValue::bulk_string("psync2"),
                )
                .into();
                let value_str: String = value.into();
                client.write_all(value_str.as_bytes()).unwrap();

                let l1 = client.read(&mut buf).unwrap();
                let c = self.config.clone();
                let response1 = handler::handle_buffer2(&c, buf[0..l1].to_vec()).unwrap();
                println!("{:?}", response1);

                let value: RedisValue = RedisCommand::Psync(
                    RedisValue::bulk_string("?"),
                    RedisValue::bulk_string("-1"),
                )
                .into();
                let value_str: String = value.into();
                client.write_all(value_str.as_bytes()).unwrap();

                let l1 = client.read(&mut buf).unwrap();
                let c = self.config.clone();
                let response1 = handler::handle_buffer2(&c, buf[0..l1].to_vec()).unwrap();
                println!("{:?}", response1);
            }
            None => {}
        }

        for stream in listener.incoming() {
            match stream {
                Ok(_stream) => {
                    let c = self.config.clone();
                    thread::spawn(move || {
                        let mut handler = handler::StreamHandler::new(_stream);
                        handler.handle(&c).unwrap();
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
