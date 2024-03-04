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
                let port = args[1].parse().unwrap();
                Some((args[0].clone(), port))
            }
            None => None,
        }
    }
}
