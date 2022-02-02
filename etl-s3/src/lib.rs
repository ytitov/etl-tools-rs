use etl_core::deps::{anyhow, tokio};
use regex::Regex;
use rusoto_core::HttpClient;
use rusoto_credential::ProfileProvider;
pub use rusoto_credential::{ChainProvider, ProvideAwsCredentials};
use rusoto_s3::*;
use std::collections::HashSet;

pub mod datastore;

pub struct ListS3FilesRequest<P: ProvideAwsCredentials + Send + Sync + 'static> {
    pub profile_provider: P,
    pub bucket: String,
    pub prefix: Option<String>,
    pub regex: Option<Regex>,
    pub region: rusoto_core::Region,
}

pub async fn list_s3_files<P: ProvideAwsCredentials + Send + Sync + 'static>(
    req: ListS3FilesRequest<P>,
) -> anyhow::Result<Vec<String>> {
    let mut r = Vec::new();
    let client = S3Client::new_with(HttpClient::new()?, req.profile_provider, req.region);
    let request = ListObjectsV2Request {
        bucket: req.bucket.clone(),
        prefix: req.prefix.clone(),
        ..Default::default()
    };
    let mut result = client.list_objects_v2(request).await?;
    fill_keys(&mut r, result.contents);
    while result.is_truncated.is_some() && result.is_truncated.unwrap() == true {
        result = client
            .list_objects_v2(ListObjectsV2Request {
                bucket: req.bucket.clone(),
                prefix: req.prefix.clone(),
                continuation_token: result.next_continuation_token,
                ..Default::default()
            })
            .await?;
        fill_keys(&mut r, result.contents);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    if let Some(re_exp) = req.regex {
        Ok(r.into_iter().filter(|n| re_exp.is_match(&n)).collect())
    } else {
        Ok(r)
    }
}

/// lists all s3 files and keeps making the requests until is_truncated is false
pub async fn list_s3_files_custom(
    profile_provider: ProfileProvider,
    s3_bucket: &str,
    prefix: Option<&str>,
    delimiter: Option<&str>,
) -> anyhow::Result<(HashSet<String>, Vec<String>)> {
    let client = create_client(profile_provider.clone());
    let mut vec = Vec::new();
    let mut hs = HashSet::new();
    let prefix = prefix.map(String::from);
    let delimiter = delimiter.map(String::from);
    let request = ListObjectsV2Request {
        bucket: s3_bucket.to_owned(),
        prefix: prefix.clone(),
        delimiter: delimiter.clone(),
        ..Default::default()
    };
    let mut result = client.list_objects_v2(request).await?;
    fill_keys(&mut vec, result.contents);
    fill_prefixes(&mut hs, result.common_prefixes);
    while result.is_truncated.is_some() && result.is_truncated.unwrap() == true {
        result = client
            .list_objects_v2(ListObjectsV2Request {
                bucket: s3_bucket.to_owned(),
                continuation_token: result.next_continuation_token,
                prefix: prefix.clone(),
                delimiter: delimiter.clone(),
                ..Default::default()
            })
            .await?;
        fill_keys(&mut vec, result.contents);
        fill_prefixes(&mut hs, result.common_prefixes);
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    Ok((hs, vec))
}

pub async fn is_result_on_s3(
    s3_credentials: &str,
    s3_bucket: &str,
    s3_bucket_key_prefix: &str,
    client_request_token: &str,
    file_extension: &str,
) -> anyhow::Result<bool> {
    let p = ProfileProvider::with_default_configuration(s3_credentials);
    let (_, files) = list_s3_files_custom(p, s3_bucket, Some(s3_bucket_key_prefix), None).await?;
    let path = format!(
        "{}{}{}",
        s3_bucket_key_prefix, client_request_token, file_extension
    );
    //println!("Looking for path: {}", &path);
    for file in files {
        if file == path {
            return Ok(true);
        }
    }
    Ok(false)
}

fn create_client(profile_provider: ProfileProvider) -> S3Client {
    S3Client::new_with(
        HttpClient::new().unwrap(),
        profile_provider,
        rusoto_core::Region::UsEast1,
    )
}

use rusoto_s3::CommonPrefix;
fn fill_prefixes(hs: &mut HashSet<String>, pref: Option<Vec<CommonPrefix>>) {
    if let Some(list) = pref {
        for obj in list {
            if let Some(key_str) = obj.prefix {
                hs.insert(key_str);
            }
        }
    }
}
use rusoto_s3::Object;
fn fill_keys(vec: &mut Vec<String>, contents: Option<Vec<Object>>) {
    if let Some(list) = contents {
        for obj in list {
            if let Some(key_str) = obj.key {
                vec.push(key_str);
            }
        }
    }
}
