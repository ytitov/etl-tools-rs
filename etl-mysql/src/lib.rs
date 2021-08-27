pub mod datastore;
pub use sqlx;
use sqlx::mysql::*;
use sqlx::Error;

pub struct CreatePoolParams {
    pub max_connections: u8,
    pub connect_timeout_seconds: u64,
    pub user: String,
    pub pw: String,
    pub host: String,
    pub port: String,
    pub db_name: String,
}

impl Default for CreatePoolParams {
    fn default() -> Self {
        CreatePoolParams {
            max_connections: 2,
            // 2 hr timeout
            connect_timeout_seconds: 60_u64 * 60 * 2,
            user: String::from("root"),
            pw: String::from("admin"),
            host: String::from("localhost"),
            port: String::from("3006"),
            db_name: String::from("default"),
        }
    }
}

pub fn create_pool(o: CreatePoolParams) -> Result<MySqlPool, Error> {
    let CreatePoolParams {
        max_connections,
        connect_timeout_seconds,
        user,
        pw,
        host,
        port,
        db_name
    } = o;
    MySqlPoolOptions::new()
        .max_connections(max_connections as u32)
        // 3 hours timeout
        .connect_timeout(std::time::Duration::from_secs(connect_timeout_seconds))
        //.min_connections(1)
        .idle_timeout(Some(std::time::Duration::from_secs(60 * 10)))
        .max_lifetime(Some(std::time::Duration::from_secs(60 * 60 * 2)))
        .after_connect(|_conn| {
            Box::pin(async move {
                println!("MySql connection established");
                Ok(())
            })
        })
        .connect_lazy(&format!(
            "mysql://{}:{}@{}:{}/{}",
            user, pw, host, port, db_name
        ))
}
