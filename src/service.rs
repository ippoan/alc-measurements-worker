//! [`measurements::service::Service`] の Workers での実装: Service Binding の Fetcher (`ALC_LINEWORKS`)。
//! 型は ippoan/alc-trouble-worker の `src/service.rs` と同じ (Fetcher と future は JS の値で `Send` でないので、
//! `worker::send` の `SendWrapper`・`SendFuture` で包む。Workers は単一 thread)。
//!
//! エラーは段の label だけに落とす (ランタイムの生のエラー文・本文はログに出さない)。応答は status だけ読む。
//! このファイルは wasm 専用で、CI のテストと coverage の gate の外 (確かめるのは staging / 本番の実物)。

use futures_util::future::BoxFuture;
use measurements::service::{Service, ServiceRequest, ServiceResponse, Unreachable};
use wasm_bindgen::JsValue;
use worker::send::{SendFuture, SendWrapper};
use worker::{Fetcher, Headers, Method, Request, RequestInit};

/// URL の host は名目の値 (Service Binding は host で経路が決まらない)
const NOMINAL_ORIGIN: &str = "https://service-binding";

pub struct ServiceBinding(SendWrapper<Fetcher>);

impl ServiceBinding {
    pub fn new(fetcher: Fetcher) -> Self {
        Self(SendWrapper::new(fetcher))
    }
}

impl Service for ServiceBinding {
    fn post(&self, req: ServiceRequest) -> BoxFuture<'_, Result<ServiceResponse, Unreachable>> {
        Box::pin(SendFuture::new(async move {
            let headers = Headers::new();
            for (name, value) in &req.headers {
                headers
                    .set(name, value)
                    .map_err(|_| Unreachable("headers"))?;
            }
            let mut init = RequestInit::new();
            init.with_method(Method::Post)
                .with_headers(headers)
                .with_body(Some(JsValue::from_str(&req.body)));
            let request = Request::new_with_init(&format!("{NOMINAL_ORIGIN}{}", req.path), &init)
                .map_err(|_| Unreachable("request"))?;
            let resp = self
                .0
                .fetch_request(request)
                .await
                .map_err(|_| Unreachable("fetch"))?;
            Ok(ServiceResponse {
                status: resp.status().as_u16(),
            })
        }))
    }
}
