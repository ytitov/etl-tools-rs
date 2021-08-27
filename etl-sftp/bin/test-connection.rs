use clap::Clap;
use etl_core::preamble::*;
use etl_core::utils;
use serde::{Deserialize, Serialize};
use ssh2;

#[derive(Clap, Debug)]
#[clap(version = "1.0", author = "Yuri Titov <ytitov@gmail.com>")]
pub struct Args {
    #[clap(
        short,
        long,
        default_value = "./sftp_config.toml",
        about = "Path to the input config file"
    )]
    pub config: String,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Config {
    pub url: String,
    /// uses "contains" matching, so partial is fine
    pub ssh_key_path: String,
}

#[tokio::main(worker_threads = 1)]
async fn main() -> anyhow::Result<()> {
    let args: Args = Args::parse();
    let Config {
        ref url,
        ref ssh_key_path,
    } = utils::load_toml(&args.config, true)?;

    use ssh2::Session;
    use std::net::TcpStream;
    use std::net::ToSocketAddrs;

    // Connect to the local SSH server
    /*
     */
    let mut addr = url.to_socket_addrs()?;
    let addr1 = addr.next().unwrap();
    println!("using ip: {:?}", &addr1);
    let tcp = TcpStream::connect(addr1).unwrap();
    let mut sess = Session::new().unwrap();
    let mut agent = sess.agent()?;
    agent.connect()?;
    agent.list_identities()?;
    let list = agent.identities()?;
    let key = list
        .iter()
        // find the key
        .find(|i| {
            println!("see key {}", i.comment());
            i.comment().contains(ssh_key_path)
        })
        .expect("did not find key");
    sess.set_tcp_stream(tcp);
    sess.handshake().unwrap();
    // perform the handshake using agent
    agent.userauth("kpiTransfer", &key)?;

    let client = sess.sftp()?;

    use std::path::Path;
    let files = client.readdir(Path::new("/"))?;

    for file in files {
        println!("file {:?}", &file);
    }

    Ok(())
}
