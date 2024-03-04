use std::net::TcpListener;
use std::thread;
use structopt::StructOpt;

mod command;
mod handler;
mod parser;
mod utilities;

#[derive(Debug, StructOpt)]
#[structopt(name = "redis")]
struct Opt {
    #[structopt(long, default_value = "6379")]
    port: u32,
}

fn main() {
    let opt = Opt::from_args();

    let url = format!("{}:{}", "127.0.0.1", opt.port);
    println!("listen to {}", url);
    let listener = TcpListener::bind(url).unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(_stream) => {
                thread::spawn(move || {
                    let mut handler = handler::StreamHandler::new(_stream);
                    handler.handle().unwrap();
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
