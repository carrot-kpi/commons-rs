use std::time::Duration;

use governor::{
    clock::{QuantaClock, QuantaInstant},
    middleware::NoOpMiddleware,
    state::{InMemoryState, NotKeyed},
    RateLimiter,
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum HttpClientError {
    #[error("could not initialize base http client: {0:?}")]
    Initialization(#[source] reqwest::Error),
    #[error("malformed base url {0}: {1:?}")]
    MalformedUrl(String, #[source] url::ParseError),
    #[error("error joining base url {0} with path {1}: {2:?}")]
    PathJoin(String, String, #[source] url::ParseError),
}

pub struct HttpClient {
    inner: reqwest::Client,
    base_url: reqwest::Url,
    bearer_auth_token: Option<String>,
    rate_limiter:
        Option<RateLimiter<NotKeyed, InMemoryState, QuantaClock, NoOpMiddleware<QuantaInstant>>>,
}

impl<'a> HttpClient {
    pub fn builder<S: AsRef<str>>(base_url: S, timeout: Duration) -> HttpClientBuilder<S> {
        HttpClientBuilder::new(base_url, timeout)
    }

    pub async fn request<S: AsRef<str> + 'a>(
        &'a self,
        method: reqwest::Method,
        path: S,
    ) -> Result<reqwest::RequestBuilder, HttpClientError> {
        let path = path.as_ref();
        let url = self.base_url.join(path).map_err(|err| {
            HttpClientError::PathJoin(self.base_url.as_str().to_owned(), path.to_owned(), err)
        })?;

        if let Some(rate_limiter) = &self.rate_limiter {
            rate_limiter.until_ready().await;
        }

        let builder = self.inner.request(method, url);
        Ok(match &self.bearer_auth_token {
            Some(token) => builder.bearer_auth(token),
            None => builder,
        })
    }
}

pub struct HttpClientBuilder<S: AsRef<str>> {
    base_url: S,
    timeout: Duration,
    bearer_auth_token: Option<String>,
    rate_limiter:
        Option<RateLimiter<NotKeyed, InMemoryState, QuantaClock, NoOpMiddleware<QuantaInstant>>>,
}

impl<S: AsRef<str>> HttpClientBuilder<S> {
    pub fn new(base_url: S, timeout: Duration) -> Self {
        Self {
            base_url,
            timeout,
            bearer_auth_token: None,
            rate_limiter: None,
        }
    }

    pub fn build(self) -> Result<HttpClient, HttpClientError> {
        Ok(HttpClient {
            inner: reqwest::Client::builder()
                .timeout(self.timeout)
                .build()
                .map_err(|err| HttpClientError::Initialization(err))?,
            base_url: reqwest::Url::parse(self.base_url.as_ref()).map_err(|err| {
                HttpClientError::MalformedUrl(self.base_url.as_ref().to_owned(), err)
            })?,
            bearer_auth_token: self.bearer_auth_token,
            rate_limiter: self.rate_limiter,
        })
    }

    pub fn base_url(mut self, base_url: S) -> Self {
        self.base_url = base_url;
        self
    }

    pub fn bearer_auth_token(mut self, token: String) -> Self {
        self.bearer_auth_token = Some(token);
        self
    }

    pub fn rate_limiter(
        mut self,
        rate_limiter: RateLimiter<
            NotKeyed,
            InMemoryState,
            QuantaClock,
            NoOpMiddleware<QuantaInstant>,
        >,
    ) -> Self {
        self.rate_limiter = Some(rate_limiter);
        self
    }
}
