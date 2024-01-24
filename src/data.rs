use std::sync::Arc;

use backoff::{future::retry, ExponentialBackoff};
use reqwest::Method;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use thiserror::Error;

use crate::http_client::{HttpClient, HttpClientError};

#[derive(Error, Debug)]
pub enum FetchJsonError {
    #[error("error while constructing json fetch request: {0:?}")]
    RequestConstruction(#[source] HttpClientError),
    #[error("error while performing json fetching request: {0:?}")]
    Request(#[source] reqwest::Error),
    #[error("error while deserializing json fetch response: {0:?}")]
    Deserialization(#[source] reqwest::Error),
}

async fn fetch_json<J: DeserializeOwned>(
    cid: String,
    s3_cdn_http_client: Arc<HttpClient>,
    ipfs_http_client: Arc<HttpClient>,
) -> Result<J, FetchJsonError> {
    let cid = cid.to_lowercase();

    match s3_cdn_http_client
        .request(Method::GET, cid.clone())
        .await
        .map_err(|err| FetchJsonError::RequestConstruction(err))?
        .send()
        .await
    {
        Ok(res) => {
            return res
                .json::<J>()
                .await
                .map_err(|err| FetchJsonError::Deserialization(err));
        }
        _ => ipfs_http_client
            .request(Method::POST, format!("/api/v0/cat?arg={cid}"))
            .await
            .map_err(|err| FetchJsonError::RequestConstruction(err))?
            .send()
            .await
            .map_err(|err| FetchJsonError::Request(err))?
            .json::<J>()
            .await
            .map_err(|err| FetchJsonError::Deserialization(err)),
    }
}

pub async fn fetch_json_with_retry<J: DeserializeOwned>(
    cid: String,
    s3_cdn_http_client: Arc<HttpClient>,
    ipfs_http_client: Arc<HttpClient>,
    backoff: ExponentialBackoff,
) -> Result<J, FetchJsonError> {
    let fetch = || async {
        fetch_json::<J>(
            cid.clone(),
            s3_cdn_http_client.clone(),
            ipfs_http_client.clone(),
        )
        .await
        .map_err(|err| match err {
            FetchJsonError::RequestConstruction(_) | FetchJsonError::Request(_) => {
                backoff::Error::Transient {
                    err,
                    retry_after: None,
                }
            }
            FetchJsonError::Deserialization(_) => backoff::Error::Permanent(err),
        })
    };

    retry(backoff, fetch).await
}

#[derive(Error, Debug)]
pub enum StoreCidIpfsError {
    #[error("error while constructing cid ipfs store request: {0:?}")]
    RequestConstruction(#[source] HttpClientError),
    #[error("error while performing cid ipfs store request: {0:?}")]
    Request(#[source] reqwest::Error),
    #[error("error while deserializing ipfs cid store request: {0:?}")]
    Deserialization(#[source] reqwest::Error),
    #[error("cid mismatch: got {0}, expected {1}")]
    CidMismatch(String, String),
}

#[derive(Serialize, Deserialize)]
pub struct StoreCidRequestResponse {
    cid: String,
}

pub async fn store_cid_ipfs(
    cid: String,
    data_uploader_http_client: Arc<HttpClient>,
) -> Result<(), StoreCidIpfsError> {
    let store_response = data_uploader_http_client
        .request(Method::POST, format!("/data/ipfs"))
        .await
        .map_err(|err| StoreCidIpfsError::RequestConstruction(err))?
        .json(&StoreCidRequestResponse { cid: cid.clone() })
        .send()
        .await
        .map_err(|err| StoreCidIpfsError::Request(err))?
        .json::<StoreCidRequestResponse>()
        .await
        .map_err(|err| StoreCidIpfsError::Deserialization(err))?;

    if store_response.cid != cid {
        Err(StoreCidIpfsError::CidMismatch(store_response.cid, cid))
    } else {
        Ok(())
    }
}

pub async fn store_cid_ipfs_with_retry<J: Serialize>(
    cid: String,
    data_uploader_http_client: Arc<HttpClient>,
    backoff: ExponentialBackoff,
) -> Result<(), StoreCidIpfsError> {
    let store = || async {
        store_cid_ipfs(cid.clone(), data_uploader_http_client.clone())
            .await
            .map_err(|err| match err {
                StoreCidIpfsError::RequestConstruction(_) | StoreCidIpfsError::Request(_) => {
                    backoff::Error::Transient {
                        err,
                        retry_after: None,
                    }
                }
                StoreCidIpfsError::Deserialization(_) | StoreCidIpfsError::CidMismatch(_, _) => {
                    backoff::Error::Permanent(err)
                }
            })
    };

    retry(backoff, store).await
}
