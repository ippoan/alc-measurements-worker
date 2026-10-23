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

/// 応答の本文の上限 (メンバー一覧。これを超えたら読めない = `body`)
const BODY_LIMIT: usize = 4 * 1024 * 1024;

pub struct ServiceBinding(SendWrapper<Fetcher>);

impl ServiceBinding {
    pub fn new(fetcher: Fetcher) -> Self {
        Self(SendWrapper::new(fetcher))
    }
}

impl ServiceBinding {
    /// GET は本文を送らない。応答は status と本文を読む (本文が読めなければ `body` の label)
    fn send(
        &self,
        method: Method,
        req: ServiceRequest,
    ) -> BoxFuture<'_, Result<ServiceResponse, Unreachable>> {
        Box::pin(SendFuture::new(async move {
            let headers = Headers::new();
            for (name, value) in &req.headers {
                headers
                    .set(name, value)
                    .map_err(|_| Unreachable("headers"))?;
            }
            let mut init = RequestInit::new();
            init.with_method(method.clone()).with_headers(headers);
            if method == Method::Post {
                init.with_body(Some(JsValue::from_str(&req.body)));
            }
            let request = Request::new_with_init(&format!("{NOMINAL_ORIGIN}{}", req.path), &init)
                .map_err(|_| Unreachable("request"))?;
            let resp = self
                .0
                .fetch_request(request)
                .await
                .map_err(|_| Unreachable("fetch"))?;
            let status = resp.status().as_u16();
            let body = axum::body::to_bytes(axum::body::Body::new(resp.into_body()), BODY_LIMIT)
                .await
                .map_err(|_| Unreachable("body"))?
                .to_vec();
            Ok(ServiceResponse { status, body })
        }))
    }
}

impl Service for ServiceBinding {
    fn post(&self, req: ServiceRequest) -> BoxFuture<'_, Result<ServiceResponse, Unreachable>> {
        self.send(Method::Post, req)
    }

    fn get(&self, req: ServiceRequest) -> BoxFuture<'_, Result<ServiceResponse, Unreachable>> {
        self.send(Method::Get, req)
    }
}
