use structopt::StructOpt;

#[derive(Debug, StructOpt, Clone)]
#[structopt(name = "redis")]
pub struct Config {
    #[structopt(long, default_value = "6379")]
    pub port: u32,
    #[structopt(long)]
    pub replicaof: Option<Vec<String>>,
}

impl Config {
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
