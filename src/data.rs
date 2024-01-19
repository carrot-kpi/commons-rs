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
    s3_http_client: Arc<HttpClient>,
    ipfs_http_client: Arc<HttpClient>,
) -> Result<J, FetchJsonError> {
    let cid = cid.to_lowercase();

    match s3_http_client
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
    s3_http_client: Arc<HttpClient>,
    ipfs_http_client: Arc<HttpClient>,
    backoff: ExponentialBackoff,
) -> Result<J, FetchJsonError> {
    let fetch = || async {
        fetch_json::<J>(
            cid.clone(),
            s3_http_client.clone(),
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
pub enum StoreJsonIpfsError {
    #[error("error while constructing json ipfs store request: {0:?}")]
    RequestConstruction(#[source] HttpClientError),
    #[error("error while performing json ipfs store request: {0:?}")]
    Request(#[source] reqwest::Error),
    #[error("error while deserializing ipfs json store request: {0:?}")]
    Deserialization(#[source] reqwest::Error),
    #[error("cid mismatch: got {0}, expected {1}")]
    CidMismatch(String, String),
}

#[derive(Deserialize)]
pub struct StoreResponse {
    cid: String,
}

#[derive(Serialize)]
pub struct StoreJsonRequest<J: Serialize> {
    data: J,
}

pub async fn store_json_ipfs<J: Serialize>(
    json: &J,
    expected_cid: String,
    data_uploader_http_client: Arc<HttpClient>,
) -> Result<(), StoreJsonIpfsError> {
    let store_response = data_uploader_http_client
        .request(Method::POST, format!("/data/json/ipfs"))
        .await
        .map_err(|err| StoreJsonIpfsError::RequestConstruction(err))?
        .json(&json)
        .send()
        .await
        .map_err(|err| StoreJsonIpfsError::Request(err))?
        .json::<StoreResponse>()
        .await
        .map_err(|err| StoreJsonIpfsError::Deserialization(err))?;

    if store_response.cid != expected_cid {
        Err(StoreJsonIpfsError::CidMismatch(
            store_response.cid,
            expected_cid,
        ))
    } else {
        Ok(())
    }
}

pub async fn store_json_ipfs_with_retry<J: Serialize>(
    json: J,
    expected_cid: String,
    data_uploader_http_client: Arc<HttpClient>,
    backoff: ExponentialBackoff,
) -> Result<(), StoreJsonIpfsError> {
    let store = || async {
        store_json_ipfs(
            &json,
            expected_cid.clone(),
            data_uploader_http_client.clone(),
        )
        .await
        .map_err(|err| match err {
            StoreJsonIpfsError::RequestConstruction(_) | StoreJsonIpfsError::Request(_) => {
                backoff::Error::Transient {
                    err,
                    retry_after: None,
                }
            }
            StoreJsonIpfsError::Deserialization(_) | StoreJsonIpfsError::CidMismatch(_, _) => {
                backoff::Error::Permanent(err)
            }
        })
    };

    retry(backoff, store).await
}
