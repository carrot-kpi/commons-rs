use std::sync::Arc;

use backoff::{future::retry, ExponentialBackoff};
use reqwest::{Body, Method};
use serde::{de::DeserializeOwned, Deserialize};
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

pub async fn fetch_json_with_retry<J: DeserializeOwned>(
    cid: String,
    ipfs_http_client: Arc<HttpClient>,
    backoff: ExponentialBackoff,
) -> Result<J, FetchJsonError> {
    let fetch = || async {
        ipfs_http_client
            .request(Method::POST, format!("/api/v0/cat?arg={cid}"))
            .await
            .map_err(|err| backoff::Error::Transient {
                err: FetchJsonError::RequestConstruction(err),
                retry_after: None,
            })?
            .send()
            .await
            .map_err(|err| backoff::Error::Transient {
                err: FetchJsonError::Request(err),
                retry_after: None,
            })?
            .json::<J>()
            .await
            .map_err(|err| backoff::Error::Permanent(FetchJsonError::Deserialization(err)))
    };

    retry(backoff, fetch).await
}

#[derive(Error, Debug)]
pub enum IpfsPinError {
    #[error("error while constructing pin request: {0:?}")]
    RequestConstruction(#[source] HttpClientError),
    #[error("error while performing pin request: {0:?}")]
    Request(#[source] reqwest::Error),
    #[error("error while deserializing pin request: {0:?}")]
    Deserialization(#[source] reqwest::Error),
    #[error("expected 1 pinned cid, got {0}")]
    InconsistentPinnedCidsAmount(usize),
    #[error("cid mismatch: got {0}, expected {1}")]
    CidMismatch(String, String),
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct PinResponse {
    pins: Vec<String>,
}

pub async fn pin_cid_with_retry(
    cid: String,
    ipfs_http_client: Arc<HttpClient>,
    backoff: ExponentialBackoff,
) -> Result<(), IpfsPinError> {
    let operation = || async {
        let cid = cid.clone();

        // get raw pin response
        let request = match ipfs_http_client
            .request(Method::POST, format!("/api/v0/pin/add?arg={cid}"))
            .await
        {
            Ok(req) => req,
            Err(err) => {
                return Err(backoff::Error::Transient {
                    err: IpfsPinError::RequestConstruction(err),
                    retry_after: None,
                });
            }
        };

        let response = match request.send().await {
            Ok(req) => req,
            Err(err) => {
                return Err(backoff::Error::Transient {
                    err: IpfsPinError::Request(err),
                    retry_after: None,
                });
            }
        };

        // convert pin response to json
        let PinResponse { pins } = match response.json::<PinResponse>().await {
            Ok(res) => res,
            Err(err) => {
                return Err(backoff::Error::Transient {
                    err: IpfsPinError::Deserialization(err),
                    retry_after: None,
                });
            }
        };

        if pins.len() != 1 {
            return Err(backoff::Error::Permanent(
                IpfsPinError::InconsistentPinnedCidsAmount(pins.len()),
            ));
        }

        let pin = pins.into_iter().next().unwrap(); // should be safe due to check above
        if pin != cid {
            return Err(backoff::Error::Permanent(IpfsPinError::CidMismatch(
                pin, cid,
            )));
        }

        Ok(())
    };

    retry(backoff, operation).await
}

#[derive(Error, Debug)]
pub enum Web3StoragePinError {
    #[error("error while constructing web3.storage car fetching request: {0:?}")]
    GetCarRequestConstruction(#[source] HttpClientError),
    #[error("error while performing car fetching request: {0:?}")]
    GetCarRequest(#[source] reqwest::Error),
    #[error("error while constructing web3.storage car upload request: {0:?}")]
    UploadCarRequestConstruction(#[source] HttpClientError),
    #[error("error while performing car upload request: {0:?}")]
    UploadCarRequest(#[source] reqwest::Error),
    #[error("error while deserializing car upload request: {0:?}")]
    UploadCarDeserialization(#[source] reqwest::Error),
    #[error("cid mismatch between original cid and uploaded cid: got {0}, expected {1}")]
    CidMismatch(String, String),
}

#[derive(Deserialize)]
pub struct CARUploadResponse {
    cid: String,
}

pub async fn pin_cid_web3_storage_with_retry(
    cid: String,
    ipfs_http_client: Arc<HttpClient>,
    web3_storage_http_client: Arc<HttpClient>,
    backoff: ExponentialBackoff,
) -> Result<(), Web3StoragePinError> {
    let operation = || async {
        let cid = cid.clone();

        // export car from ipfs
        let car_response = ipfs_http_client
            .request(
                Method::POST,
                format!("/api/v0/dag/export?arg={cid}&progress=false"),
            )
            .await
            .map_err(|err| backoff::Error::Transient {
                err: Web3StoragePinError::GetCarRequestConstruction(err),
                retry_after: None,
            })?
            .send()
            .await
            .map_err(|err| backoff::Error::Transient {
                err: Web3StoragePinError::GetCarRequest(err),
                retry_after: None,
            })?;

        // upload car to web3.storage
        let car_upload_response = web3_storage_http_client
            .request(Method::POST, "/car")
            .await
            .map_err(|err| backoff::Error::Transient {
                err: Web3StoragePinError::UploadCarRequestConstruction(err),
                retry_after: None,
            })?
            .body(Body::wrap_stream(car_response.bytes_stream()))
            .send()
            .await
            .map_err(|err| backoff::Error::Transient {
                err: Web3StoragePinError::UploadCarRequest(err),
                retry_after: None,
            })?
            .json::<CARUploadResponse>()
            .await
            .map_err(|err| backoff::Error::Transient {
                err: Web3StoragePinError::UploadCarDeserialization(err),
                retry_after: None,
            })?;

        if car_upload_response.cid != *cid {
            return Err(backoff::Error::Permanent(Web3StoragePinError::CidMismatch(
                car_upload_response.cid,
                cid,
            )));
        }

        Ok(())
    };

    retry(backoff, operation).await
}
