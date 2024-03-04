use std::net::TcpListener;
use std::thread;

use structopt::StructOpt;

mod command;
mod config;
mod handler;
mod parser;
mod utilities;

fn main() {
    let config = config::Config::from_args();
    println!("{:?}", config);

    let url = format!("{}:{}", "127.0.0.1", config.port);
    println!("listen to {}", url);
    let listener = TcpListener::bind(url).unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                let c = config.clone();
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
