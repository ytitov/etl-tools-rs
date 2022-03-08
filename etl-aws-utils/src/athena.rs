use etl_core::deps::anyhow::{self, anyhow};
use etl_core::deps::async_trait;
use etl_job::job::{JobRunner, command::*};
use rusoto_athena::*;
use rusoto_core::region::Region;
use rusoto_core::request::HttpClient;
use rusoto_credential::ProfileProvider;
use serde::{Deserialize, Serialize};
use std::time::Instant;
use etl_core::deps::tokio::time::sleep;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AthenaConfig {
    pub s3_credentials: String,
    pub s3_bucket: String,
    pub s3_prefix: String,
    #[serde(default = "default_athena_catalog_name")]
    pub athena_catalog_name: String,
    pub athena_db_name: String,
    pub region: Region,
}

impl AthenaConfig {
    pub fn append_s3_prefix<S: Into<String>>(mut self, s: S) -> Self {
        self.s3_prefix.push_str(&s.into());
        self
    }
}

impl Default for AthenaConfig {
    fn default() -> Self {
        AthenaConfig {
            s3_credentials: "path/to/creds".to_string(),
            s3_bucket: "bucket name only (without `s3:://`)".to_string(),
            s3_prefix: "the subfolder or prefix for all results".to_string(),
            athena_db_name: "name of the database where the tables should be".to_string(),
            athena_catalog_name: default_athena_catalog_name(),
            region: Region::UsEast1,
        }
    }
}

fn default_athena_catalog_name() -> String {
    "AwsDataCatalog".to_string()
}

pub struct AthenaQueryJobCommand {
    id: String,
    config: AthenaConfig,
    queries: Vec<String>,
    /// created for select queries to wait for the csv result
    is_select_query: bool,
}

impl AthenaQueryJobCommand {
    /// Convenience method for creating a single query command
    pub fn query_results_cmd<S: Into<String>, Q: Into<String>>(
        id: S,
        query: Q,
        config: AthenaConfig,
    ) -> Box<dyn JobCommand> {
        Box::new(AthenaQueryJobCommand {
            id: id.into(),
            queries: vec![query.into()],
            config,
            is_select_query: true,
        })
    }

    pub fn new<S: Into<String>, Q: Into<String>>(id: S, query: Q, config: AthenaConfig) -> Self {
        AthenaQueryJobCommand {
            id: id.into(),
            queries: vec![query.into()],
            config,
            is_select_query: false,
        }
    }

    pub fn into_cmd(self) -> Box<dyn JobCommand> {
        Box::new(AthenaQueryJobCommand {
            id: self.id,
            queries: self.queries,
            config: self.config,
            is_select_query: false,
        })
    }

    pub fn add_query<S: Into<String>>(mut self, query: S) -> Self {
        self.queries.push(query.into());
        self
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct AthenaResult {
    pub result: String,
}

#[async_trait]
impl<'a> JobCommand for AthenaQueryJobCommand {
    async fn run(self: Box<Self>, jr: &mut JobRunner) -> anyhow::Result<()> {
        let mut result = None;
        for (_, query) in self.queries.iter().enumerate() {
            result = Some(athena_query(&self.config, query).await?);
            //jr.set_state(self.name(), &AthenaResult { idx, result });
        }
        if result.is_some() {
            let s3_file_full_path = format!("{}{}", self.config.s3_prefix, result.clone().unwrap());
            jr.set_state(
                self.name(),
                &AthenaResult {
                    result: s3_file_full_path.clone(),
                },
            );
            if self.is_select_query && result.is_some() {
                let AthenaConfig {
                    s3_credentials,
                    s3_bucket,
                    s3_prefix: _,
                    athena_catalog_name: _,
                    athena_db_name: _,
                    region: _,
                } = &self.config;
                loop {
                    if crate::s3_utils::is_result_on_s3(
                        s3_credentials,
                        s3_bucket,
                        "",
                        &s3_file_full_path,
                        ".csv",
                    )
                    .await?
                    {
                        jr.set_state(
                            self.name(),
                            &AthenaResult {
                                result: format!("{}.csv", &s3_file_full_path),
                            },
                        );
                        break;
                    } else {
                        sleep(std::time::Duration::from_millis(1000)).await;
                    }
                }
            }
        } else {
            return Err(anyhow::anyhow!(
                "Did not get file, which is needed to get results of the query"
            ));
        }
        Ok(())
    }
    fn name(&self) -> String {
        format!("{}", &self.id)
    }
}

/// This will run an athena query then query for results every second for 10 seconds.  
/// If query takes longer than 10 seconds, this will not work.  At this time this is
/// just an example, it likely needs a timeout parameter or something along those lines.
/// Example usage:
/// ```ignore
/// use rusoto_core::request::HttpClient;
/// use rusoto_core::region::Region;
/// use rusoto_athena::AthenaClient;
/// let client = AthenaClient::new_with(
///     HttpClient::new().unwrap(),
///     profile_provider.clone(),
///     Region::UsEast1,
/// );
///
/// let query = "select * from patient limit 10".to_string();
/// let job_id = athena_query(&client, "ihde_fhir", &query, "a_bucket_name").await?;
/// ```
pub async fn athena_query(athena_cfg: &AthenaConfig, query: &str) -> anyhow::Result<String> {
    let AthenaConfig {
        s3_credentials: credentials,
        s3_bucket: output_bucket,
        s3_prefix: output_bucket_prefix,
        athena_catalog_name,
        athena_db_name,
        region,
    } = athena_cfg;
    let client = AthenaClient::new_with(
        HttpClient::new()?,
        ProfileProvider::with_default_configuration(credentials),
        region.to_owned(),
    );
    let gdbi = GetDatabaseInput {
        // TODO: move this to configuration
        //catalog_name: "AwsDataCatalog".to_owned(),
        catalog_name: athena_catalog_name.to_owned(),
        database_name: athena_db_name.to_owned(),
    };
    let client_request_token = uuid::Uuid::new_v4().to_string();
    let output_location = Some(format!("s3://{}/{}", output_bucket, output_bucket_prefix));
    let r = client
        .start_query_execution(StartQueryExecutionInput {
            client_request_token: Some(client_request_token.clone()),
            query_execution_context: Some(QueryExecutionContext {
                catalog: Some(gdbi.catalog_name.clone()),
                database: Some(gdbi.database_name.clone()),
            }),
            query_string: query.to_owned(),
            work_group: Some("primary".to_string()),
            result_configuration: Some(ResultConfiguration {
                encryption_configuration: None,
                output_location,
            }),
        })
        .await?;
    let start_instant = Instant::now();
    loop {
        let elapsed_duration = start_instant.elapsed();
        match client
            .get_query_execution(GetQueryExecutionInput {
                query_execution_id: r.query_execution_id.clone().unwrap(),
            })
            .await
        {
            Ok(GetQueryExecutionOutput {
                query_execution: None,
            }) => {
                println!("this is weird, not sure what to do here");
                return Err(anyhow!(
                    "Fatal error did not get a query_execution in output"
                ));
            }
            Ok(GetQueryExecutionOutput {
                query_execution: Some(q_ex),
            }) => {
                let QueryExecution {
                    /*
                    query: q_exec_query,
                    query_execution_context,
                    result_configuration,
                    statement_type,
                    statistics,
                    query_execution_id,
                    work_group,
                    */
                    status,
                    ..
                } = q_ex;
                if let Some(s) = status {
                    //println!("status {:?}", &s);
                    println!(
                        "{:?} TIME: {}s bucket: {:?}",
                        s.state,
                        elapsed_duration.as_secs(),
                        output_bucket_prefix
                    );
                    let _s = s.clone();
                    match s.state {
                        Some(s) if s == "SUCCEEDED" => {
                            //println!("Athena query ok: {}", query);
                            //println!("-- query_execution_id: {:?}", &r.query_execution_id);
                            return Ok(r.query_execution_id.unwrap());
                        }
                        Some(s) if s == "FAILED" => {
                            //println!("Query that failed: {}, status: {:?}", query, _s);
                            return Err(anyhow!("Query failed: {:?}", &query));
                        }
                        _ => {}
                    }
                }
            }
            Err(er) => {
                return Err(anyhow!("Fatal error: {}", er));
            }
        }
        sleep(std::time::Duration::from_millis(2000)).await;
    }
}
