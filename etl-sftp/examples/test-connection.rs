use clap::Parser;
use etl_core::deps::anyhow;
use etl_core::deps::tokio;
use etl_core::utils;
use serde::{Deserialize, Serialize};

use etl_sftp::*;

#[derive(Parser, Debug)]
#[clap(version = "1.0", author = "Yuri Titov <ytitov@gmail.com>")]
pub struct Args {
    #[clap(short, long, default_value = "./sftp_config.toml")]
    /// Path to the input config file
    pub config: String,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Config {
    pub url: String,
    /// uses "contains" matching, so partial is fine
    pub ssh_key_path: Option<String>,
    pub username: String,
    pub password: Option<String>,
}

#[tokio::main(worker_threads = 1)]
async fn main() -> anyhow::Result<()> {
    let args: Args = Args::parse();
    let Config {
        ref url,
        ref ssh_key_path,
        ref username,
        ref password,
    } = utils::load_toml(&args.config, true)?;

    let client = match (password, ssh_key_path) {
        (Some(ref password), None) => ssh_connect(
            url,
            Credentials::UserPassword {
                username: username.to_string(),
                pw: password.to_string(),
            },
        )?,
        (None, Some(ref ssh_key_path)) => ssh_connect(
            url,
            Credentials::SshKeyPath {
                username: username.to_string(),
                ssh_key_path: ssh_key_path.to_string(),
            },
        )?,
        _ => panic!("Need either username and password OR username and ssh_key_path"),
    };

    use std::path::Path;
    println!("Reading files from root directory");
    let files = client.readdir(Path::new("/"))?;

    for file in files {
        println!("file {:?}", &file);
    }

    Ok(())
}
