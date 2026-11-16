//! The lease and archive store on Azure Blob Storage.
//!
//! HEAD is one block blob and every write of it is one single-shot Put Blob
//! carrying `If-Match: <etag>` (or `If-None-Match: *` for the very first
//! write). The service applies the write only if the condition holds, so a
//! node that lost the lease while it was not looking gets a 412 and nothing
//! of its write lands. That conditional write is the fence; nothing else is.
//! Archive objects are created with `If-None-Match: *`, so a name is written
//! once and a second writer cannot replace it.
//!
//! Every answer is mapped to one of the outcomes [`crate::store`] names, and
//! the mapping never guesses in the unsafe direction:
//!
//! - HEAD is "missing" only on a 404 whose `x-ms-error-code` is
//!   `BlobNotFound`. Any other answer to a read, a 404 for a missing
//!   container included, is an error, because a node that believes HEAD is
//!   missing may create a new one at epoch 1.
//! - A write is "refused" only on a 412, on a 409 to a create, or on a 404
//!   `BlobNotFound` under `If-Match`.
//! - A 5xx, a 408 or a transport error after the request went out is
//!   `Unknown`: the write may have been applied. The lease then reads HEAD.
//! - Anything that failed before the request left (no token, connection
//!   refused) or was rejected outright (400, 401, 403) is `Unreachable`:
//!   nothing was written.
//!
//! On a VM the store signs requests with a managed identity token from the
//! instance metadata service. A shared key is accepted only for an endpoint
//! on loopback, which is how the tests reach the Azurite emulator; a real
//! account is never addressed with a key.

use crate::store::{
    Cond, Etag, HeadRead, HeadStore, ObjectStore, PutOutcome, StoreError, WriteOutcome,
};
use base64::Engine;
use hmac::{Hmac, Mac};
use reqwest::blocking::{Body, Client, Response};
use reqwest::Method;
use sha2::Sha256;
use std::fs::File;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// The Blob service version every request names.
pub const API_VERSION: &str = "2023-11-03";

const IMDS_TOKEN_URL: &str = "http://169.254.169.254/metadata/identity/oauth2/token\
?api-version=2018-02-01&resource=https%3A%2F%2Fstorage.azure.com%2F";
const IMDS_TIMEOUT: Duration = Duration::from_secs(2);
/// A token is fetched again this long before it expires.
const TOKEN_MARGIN_SECS: u64 = 300;
/// HEAD is a few kilobytes. A larger body is not a HEAD this code wrote.
const MAX_HEAD_BYTES: usize = 1 << 20;
const LIST_PAGE: u32 = 5_000;

/// How requests are signed.
#[derive(Clone)]
pub enum Auth {
    /// A managed identity token from the instance metadata service.
    /// `client_id` picks one identity when the VM has several.
    Imds { client_id: Option<String> },
    /// A shared key, for the local emulator only. Refused for any endpoint
    /// that is not on loopback.
    EmulatorKey { account: String, key: Vec<u8> },
}

impl std::fmt::Debug for Auth {
    /// Never prints a key.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Auth::Imds { client_id } => f
                .debug_struct("Imds")
                .field("client_id", client_id)
                .finish(),
            Auth::EmulatorKey { account, .. } => f
                .debug_struct("EmulatorKey")
                .field("account", account)
                .finish_non_exhaustive(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct AzureConfig {
    /// `https://<account>.blob.core.windows.net`, or the emulator's
    /// `http://127.0.0.1:10000/<account>`.
    pub endpoint: String,
    /// The container that holds HEAD.
    pub head_container: String,
    /// The HEAD blob's name in that container.
    pub head_blob: String,
    /// The container archive objects are created in.
    pub object_container: String,
    pub auth: Auth,
    /// The time limit for a HEAD read or write.
    pub head_timeout: Duration,
    /// The time limit for uploading one archive object.
    pub put_timeout: Duration,
}

impl AzureConfig {
    /// The deployed layout: HEAD at `lease/HEAD.json`, objects in
    /// `archive`, a managed identity token, 5 s for HEAD and 20 s for an
    /// object upload.
    pub fn new(endpoint: &str, client_id: Option<String>) -> Self {
        AzureConfig {
            endpoint: endpoint.to_string(),
            head_container: "lease".into(),
            head_blob: "HEAD.json".into(),
            object_container: "archive".into(),
            auth: Auth::Imds { client_id },
            head_timeout: Duration::from_secs(5),
            put_timeout: Duration::from_secs(20),
        }
    }

    /// Check the configuration without building a client or sending
    /// anything, and return the endpoint without a trailing slash and its
    /// path part.
    pub fn validate(&self) -> Result<(String, String), String> {
        let url = reqwest::Url::parse(&self.endpoint)
            .map_err(|e| format!("bad store endpoint {:?}: {e}", self.endpoint))?;
        let host = url.host_str().unwrap_or("");
        let loopback = matches!(host, "127.0.0.1" | "localhost" | "[::1]" | "::1");
        if url.query().is_some() || url.fragment().is_some() {
            return Err("the store endpoint must not carry a query or a fragment".into());
        }
        match (url.scheme(), loopback) {
            ("https", _) | ("http", true) => {}
            _ => return Err("the store endpoint must be https unless it is on loopback".into()),
        }
        if matches!(self.auth, Auth::EmulatorKey { .. }) && !loopback {
            return Err("a shared key is accepted only for an endpoint on loopback".into());
        }
        for (what, name) in [
            ("head container", &self.head_container),
            ("object container", &self.object_container),
        ] {
            let ok = (3..=63).contains(&name.len())
                && name
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
            if !ok {
                return Err(format!("bad {what} name {name:?}"));
            }
        }
        if !name_ok(&self.head_blob) {
            return Err(format!("bad head blob name {:?}", self.head_blob));
        }
        let base = self.endpoint.trim_end_matches('/').to_string();
        let path = url.path().trim_end_matches('/').to_string();
        Ok((base, path))
    }
}

struct Token {
    value: String,
    good_until: Instant,
}

struct Inner {
    cfg: AzureConfig,
    /// The endpoint without a trailing slash.
    base: String,
    /// The path part of the endpoint: empty on Azure, `/<account>` on the
    /// emulator.
    base_path: String,
    http: Client,
    token: Mutex<Option<Token>>,
    page: u32,
}

/// The lease and archive store on Azure Blob Storage. Clones share one
/// client and one token.
///
/// It makes blocking calls, so build it and use it on a plain thread, not
/// on an async runtime's worker.
#[derive(Clone)]
pub struct AzureStore(Arc<Inner>);

/// A request that got no answer.
enum SendError {
    /// It never left: nothing was written.
    NotSent(String),
    /// It left and may have been applied.
    Unknown(String),
}

impl SendError {
    fn text(&self) -> &str {
        match self {
            SendError::NotSent(s) | SendError::Unknown(s) => s,
        }
    }
}

struct Req {
    method: Method,
    /// Path below the endpoint, percent-encoded, starting with a slash.
    path: String,
    query: Vec<(&'static str, String)>,
    headers: Vec<(&'static str, String)>,
    body: Option<(Body, u64)>,
    timeout: Duration,
}

/// A blob or object name this store will address: relative, no empty,
/// `.` or `..` part, no control characters.
fn name_ok(name: &str) -> bool {
    name.len() <= 1024
        && !name.bytes().any(|b| b.is_ascii_control())
        && name
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
}

/// Percent-encode everything but unreserved characters. With `keep_slash`
/// a slash stays, for a blob name used as a path.
fn pct(s: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        let plain = b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~');
        if plain || (keep_slash && b == b'/') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// The headers that carry a write's condition.
pub fn cond_headers(cond: &Cond) -> Vec<(&'static str, String)> {
    match cond {
        Cond::IfMatch(Etag(e)) => vec![("if-match", e.clone())],
        Cond::IfNoneMatch => vec![("if-none-match", "*".to_string())],
        Cond::Unconditional => Vec::new(),
    }
}

/// What the answer to a HEAD write means. `etag` is the reply's ETag and
/// `code` its `x-ms-error-code`.
pub fn map_head_write(
    status: u16,
    etag: Option<&str>,
    code: Option<&str>,
    cond: &Cond,
) -> WriteOutcome {
    match status {
        200 | 201 => match etag {
            Some(e) if !e.is_empty() => WriteOutcome::Done(Etag(e.to_string())),
            // Applied, but the new ETag is not known: let the lease read it.
            _ => WriteOutcome::Unknown,
        },
        412 => WriteOutcome::PreconditionFailed,
        409 if *cond == Cond::IfNoneMatch => WriteOutcome::PreconditionFailed,
        404 if matches!(cond, Cond::IfMatch(_)) && code == Some("BlobNotFound") => {
            WriteOutcome::PreconditionFailed
        }
        408 => WriteOutcome::Unknown,
        s if s >= 500 => WriteOutcome::Unknown,
        _ => WriteOutcome::Unreachable,
    }
}

/// What the answer to a create-only object put means.
pub fn map_put(status: u16) -> PutOutcome {
    match status {
        200 | 201 => PutOutcome::Created,
        409 | 412 => PutOutcome::PreconditionFailed,
        408 => PutOutcome::Unknown,
        s if s >= 500 => PutOutcome::Unknown,
        _ => PutOutcome::Unreachable,
    }
}

/// What the answer to a HEAD read means. Only a 404 that the service itself
/// labels `BlobNotFound` is a missing HEAD.
pub fn map_head_read(
    status: u16,
    etag: Option<&str>,
    code: Option<&str>,
    body: Vec<u8>,
) -> HeadRead {
    match (status, etag, code) {
        (200, Some(e), _) if !e.is_empty() => {
            if body.len() > MAX_HEAD_BYTES {
                return Err(StoreError(format!("HEAD is {} bytes", body.len())));
            }
            Ok(Some((body, Etag(e.to_string()))))
        }
        (200, _, _) => Err(StoreError("HEAD read had no ETag".into())),
        (404, _, Some("BlobNotFound")) => Ok(None),
        (s, _, c) => Err(StoreError(format!(
            "HEAD read answered {s} {}",
            c.unwrap_or("with no error code")
        ))),
    }
}

/// Undo the five XML entities a blob name can be escaped with.
fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// The blob names and the continuation marker in one List Blobs page.
pub fn parse_list(xml: &str) -> Result<(Vec<String>, Option<String>), String> {
    if !xml.contains("<EnumerationResults") || !xml.contains("</EnumerationResults>") {
        return Err("not a complete List Blobs answer".into());
    }
    let mut names = Vec::new();
    let mut rest = xml;
    while let Some(i) = rest.find("<Blob>") {
        let blob = &rest[i + 6..];
        let end = blob.find("</Blob>").ok_or("unclosed Blob element")?;
        let item = &blob[..end];
        let a = item.find("<Name>").ok_or("a Blob with no Name")?;
        let b = item.find("</Name>").ok_or("a Blob with no Name")?;
        if b < a + 6 {
            return Err("a Blob with a broken Name".into());
        }
        names.push(xml_unescape(&item[a + 6..b]));
        rest = &blob[end + 7..];
    }
    let marker = match (xml.find("<NextMarker>"), xml.find("</NextMarker>")) {
        (Some(a), Some(b)) if b > a + 12 => Some(xml_unescape(&xml[a + 12..b])),
        _ => None,
    };
    Ok((names, marker))
}

/// The token and its lifetime in seconds from an instance metadata answer.
/// The lifetime arrives as a string or as a number.
pub fn parse_token(json: &str) -> Result<(String, u64), String> {
    let v: serde_json::Value =
        serde_json::from_str(json).map_err(|_| "the token answer is not JSON".to_string())?;
    let token = v
        .get("access_token")
        .and_then(|t| t.as_str())
        .filter(|t| !t.is_empty())
        .ok_or("the token answer has no access_token")?;
    let secs = match v.get("expires_in") {
        Some(serde_json::Value::String(s)) => s.parse::<u64>().ok(),
        Some(n) => n.as_u64(),
        None => None,
    }
    .ok_or("the token answer has no expires_in")?;
    Ok((token.to_string(), secs))
}

/// A Unix time as an RFC 1123 date, the form `x-ms-date` takes.
pub fn rfc1123(unix: u64) -> String {
    const DAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let days = unix / 86_400;
    let secs = unix % 86_400;
    let z = days + 719_468;
    let era = z / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + u64::from(m <= 2);
    format!(
        "{}, {:02} {} {} {:02}:{:02}:{:02} GMT",
        DAYS[((days + 4) % 7) as usize],
        d,
        MONTHS[(m - 1) as usize],
        y,
        secs / 3_600,
        secs % 3_600 / 60,
        secs % 60
    )
}

/// The Shared Key string to sign for the Blob service. `headers` are the
/// request's headers with lower-case names; `resource` is the canonical
/// resource, `/<account><path>` followed by one `\n<name>:<value>` per
/// query parameter in name order.
pub fn string_to_sign(
    method: &str,
    content_length: u64,
    headers: &[(&str, String)],
    resource: &str,
) -> String {
    let get = |name: &str| {
        headers
            .iter()
            .find(|(n, _)| *n == name)
            .map_or("", |(_, v)| v.as_str())
    };
    let len = if content_length == 0 {
        String::new()
    } else {
        content_length.to_string()
    };
    let mut ms: Vec<&(&str, String)> = headers
        .iter()
        .filter(|(n, _)| n.starts_with("x-ms-"))
        .collect();
    ms.sort_by(|a, b| a.0.cmp(b.0));
    let mut canon = String::new();
    for (n, v) in ms {
        canon.push_str(n);
        canon.push(':');
        canon.push_str(v.trim());
        canon.push('\n');
    }
    // Verb, Content-Encoding, Content-Language, Content-Length,
    // Content-MD5, Content-Type, Date, If-Modified-Since, If-Match,
    // If-None-Match, If-Unmodified-Since, Range.
    format!(
        "{method}\n\n\n{len}\n\n{}\n\n\n{}\n{}\n\n\n{canon}{resource}",
        get("content-type"),
        get("if-match"),
        get("if-none-match"),
    )
}

fn sign(key: &[u8], text: &str) -> Result<String, String> {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(key).map_err(|_| "bad shared key length".to_string())?;
    mac.update(text.as_bytes());
    Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

fn header<'a>(resp: &'a Response, name: &str) -> Option<&'a str> {
    resp.headers().get(name).and_then(|v| v.to_str().ok())
}

impl AzureStore {
    pub fn new(cfg: AzureConfig) -> Result<Self, String> {
        let (base, base_path) = cfg.validate()?;
        let http = Client::builder()
            .no_proxy()
            .connect_timeout(Duration::from_secs(3))
            .timeout(cfg.head_timeout)
            .build()
            .map_err(|e| format!("http client: {e}"))?;
        Ok(AzureStore(Arc::new(Inner {
            cfg,
            base,
            base_path,
            http,
            token: Mutex::new(None),
            page: LIST_PAGE,
        })))
    }

    /// The store for the Azurite emulator. `key_b64` is the emulator
    /// account's key, never a real account's.
    pub fn emulator(
        endpoint: &str,
        account: &str,
        key_b64: &str,
        head_container: &str,
        object_container: &str,
    ) -> Result<Self, String> {
        let key = base64::engine::general_purpose::STANDARD
            .decode(key_b64.trim())
            .map_err(|_| "the emulator key is not base64".to_string())?;
        let mut cfg = AzureConfig::new(endpoint, None);
        cfg.head_container = head_container.to_string();
        cfg.object_container = object_container.to_string();
        cfg.auth = Auth::EmulatorKey {
            account: account.to_string(),
            key,
        };
        Self::new(cfg)
    }

    pub fn config(&self) -> &AzureConfig {
        &self.0.cfg
    }

    fn bearer(&self) -> Result<String, String> {
        let Auth::Imds { client_id } = &self.0.cfg.auth else {
            return Err("no identity configured".into());
        };
        let mut slot = self.0.token.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(t) = slot.as_ref() {
            if Instant::now() < t.good_until {
                return Ok(t.value.clone());
            }
        }
        let mut url = IMDS_TOKEN_URL.to_string();
        if let Some(id) = client_id {
            url.push_str("&client_id=");
            url.push_str(&pct(id, false));
        }
        let asked = Instant::now();
        let resp = self
            .0
            .http
            .get(url)
            .header("Metadata", "true")
            .timeout(IMDS_TIMEOUT)
            .send()
            .map_err(|e| format!("identity endpoint: {}", kind(&e)))?;
        let status = resp.status().as_u16();
        if status != 200 {
            return Err(format!("identity endpoint answered {status}"));
        }
        let text = resp
            .text()
            .map_err(|e| format!("identity endpoint: {}", kind(&e)))?;
        let (value, secs) = parse_token(&text)?;
        let life = if secs > 2 * TOKEN_MARGIN_SECS {
            secs - TOKEN_MARGIN_SECS
        } else {
            secs / 2
        };
        *slot = Some(Token {
            value: value.clone(),
            good_until: asked + Duration::from_secs(life),
        });
        Ok(value)
    }

    fn forget_token(&self) {
        *self.0.token.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }

    fn send(&self, r: Req) -> Result<Response, SendError> {
        let inner = &self.0;
        let mut headers = r.headers;
        headers.push(("x-ms-version", API_VERSION.to_string()));
        let len = r.body.as_ref().map_or(0, |(_, n)| *n);
        let authorization = match &inner.cfg.auth {
            Auth::Imds { .. } => format!("Bearer {}", self.bearer().map_err(SendError::NotSent)?),
            Auth::EmulatorKey { account, key } => {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_or(0, |d| d.as_secs());
                headers.push(("x-ms-date", rfc1123(now)));
                let mut resource = format!("/{account}{}{}", inner.base_path, r.path);
                let mut q = r.query.clone();
                q.sort();
                for (n, v) in &q {
                    resource.push_str(&format!("\n{n}:{v}"));
                }
                let text = string_to_sign(r.method.as_str(), len, &headers, &resource);
                let sig = sign(key, &text).map_err(SendError::NotSent)?;
                format!("SharedKey {account}:{sig}")
            }
        };
        let mut url = format!("{}{}", inner.base, r.path);
        for (i, (n, v)) in r.query.iter().enumerate() {
            url.push(if i == 0 { '?' } else { '&' });
            url.push_str(n);
            url.push('=');
            url.push_str(&pct(v, false));
        }
        let mut b = inner
            .http
            .request(r.method, url)
            .timeout(r.timeout)
            .header("authorization", authorization);
        for (n, v) in &headers {
            b = b.header(*n, v.as_str());
        }
        if let Some((body, _)) = r.body {
            b = b.body(body);
        }
        let resp = b.send().map_err(|e| {
            let what = kind(&e).to_string();
            if e.is_builder() || e.is_connect() {
                SendError::NotSent(what)
            } else {
                SendError::Unknown(what)
            }
        })?;
        if matches!(resp.status().as_u16(), 401 | 403) {
            self.forget_token();
        }
        Ok(resp)
    }

    fn blob_path(container: &str, name: &str) -> String {
        format!("/{container}/{}", pct(name, true))
    }

    fn put_blob(
        &self,
        container: &str,
        name: &str,
        body: (Body, u64),
        content_type: &str,
        cond: &Cond,
        timeout: Duration,
    ) -> Result<Response, SendError> {
        let mut headers = cond_headers(cond);
        headers.push(("x-ms-blob-type", "BlockBlob".to_string()));
        headers.push(("content-type", content_type.to_string()));
        self.send(Req {
            method: Method::PUT,
            path: Self::blob_path(container, name),
            query: Vec::new(),
            headers,
            body: Some(body),
            timeout,
        })
    }

    /// Create a container. The deployed containers come from the template;
    /// this is for the emulator.
    pub fn create_container(&self, name: &str) -> Result<(), String> {
        let r = self
            .send(Req {
                method: Method::PUT,
                path: format!("/{name}"),
                query: vec![("restype", "container".to_string())],
                headers: Vec::new(),
                body: None,
                timeout: self.0.cfg.head_timeout,
            })
            .map_err(|e| format!("create container: {}", e.text()))?;
        match r.status().as_u16() {
            201 | 409 => Ok(()),
            s => Err(format!(
                "create container answered {s} {}",
                header(&r, "x-ms-error-code").unwrap_or("")
            )),
        }
    }
}

/// A short, secret-free description of a transport error.
fn kind(e: &reqwest::Error) -> &'static str {
    if e.is_connect() {
        "could not connect"
    } else if e.is_timeout() {
        "timed out"
    } else if e.is_builder() {
        "bad request"
    } else {
        "transport error"
    }
}

impl HeadStore for AzureStore {
    fn read_head(&self) -> HeadRead {
        let cfg = &self.0.cfg;
        let resp = self
            .send(Req {
                method: Method::GET,
                path: Self::blob_path(&cfg.head_container, &cfg.head_blob),
                query: Vec::new(),
                headers: Vec::new(),
                body: None,
                timeout: cfg.head_timeout,
            })
            .map_err(|e| StoreError(format!("HEAD read: {}", e.text())))?;
        let status = resp.status().as_u16();
        let etag = header(&resp, "etag").map(str::to_string);
        let code = header(&resp, "x-ms-error-code").map(str::to_string);
        let body = if status == 200 {
            resp.bytes()
                .map_err(|e| StoreError(format!("HEAD read body: {}", kind(&e))))?
                .to_vec()
        } else {
            Vec::new()
        };
        map_head_read(status, etag.as_deref(), code.as_deref(), body)
    }

    fn write_head(&self, body: &[u8], cond: &Cond) -> WriteOutcome {
        if body.contains(&b'\n') {
            return WriteOutcome::Unreachable;
        }
        let cfg = &self.0.cfg;
        let sent = self.put_blob(
            &cfg.head_container,
            &cfg.head_blob,
            (Body::from(body.to_vec()), body.len() as u64),
            "application/json",
            cond,
            cfg.head_timeout,
        );
        match sent {
            Ok(resp) => map_head_write(
                resp.status().as_u16(),
                header(&resp, "etag"),
                header(&resp, "x-ms-error-code"),
                cond,
            ),
            Err(SendError::NotSent(_)) => WriteOutcome::Unreachable,
            Err(SendError::Unknown(_)) => WriteOutcome::Unknown,
        }
    }
}

impl ObjectStore for AzureStore {
    fn put_new(&self, name: &str, src: &Path) -> PutOutcome {
        let cfg = &self.0.cfg;
        let is_head = cfg.object_container == cfg.head_container && name == cfg.head_blob;
        if !name_ok(name) || is_head {
            return PutOutcome::Unreachable;
        }
        let Ok(file) = File::open(src) else {
            return PutOutcome::Unreachable;
        };
        let Ok(len) = file.metadata().map(|m| m.len()) else {
            return PutOutcome::Unreachable;
        };
        let sent = self.put_blob(
            &cfg.object_container,
            name,
            (Body::sized(file, len), len),
            "application/octet-stream",
            &Cond::IfNoneMatch,
            cfg.put_timeout,
        );
        match sent {
            Ok(resp) => map_put(resp.status().as_u16()),
            Err(SendError::NotSent(_)) => PutOutcome::Unreachable,
            Err(SendError::Unknown(_)) => PutOutcome::Unknown,
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        let cfg = &self.0.cfg;
        let mut out = Vec::new();
        let mut marker: Option<String> = None;
        loop {
            let mut query = vec![
                ("restype", "container".to_string()),
                ("comp", "list".to_string()),
                ("maxresults", self.0.page.to_string()),
            ];
            if !prefix.is_empty() {
                query.push(("prefix", prefix.to_string()));
            }
            if let Some(m) = &marker {
                query.push(("marker", m.clone()));
            }
            let resp = self
                .send(Req {
                    method: Method::GET,
                    path: format!("/{}", cfg.object_container),
                    query,
                    headers: Vec::new(),
                    body: None,
                    timeout: cfg.put_timeout,
                })
                .map_err(|e| StoreError(format!("list: {}", e.text())))?;
            let status = resp.status().as_u16();
            if status != 200 {
                return Err(StoreError(format!(
                    "list answered {status} {}",
                    header(&resp, "x-ms-error-code").unwrap_or("")
                )));
            }
            let text = resp
                .text()
                .map_err(|e| StoreError(format!("list body: {}", kind(&e))))?;
            let (names, next) = parse_list(&text).map_err(StoreError)?;
            out.extend(names);
            match next {
                Some(m) => marker = Some(m),
                None => break,
            }
        }
        // HEAD is not an object, even when both share a container.
        if cfg.object_container == cfg.head_container {
            out.retain(|n| n != &cfg.head_blob);
        }
        out.sort();
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::caslease::{audit, CasLease, Config, Head, LeaseMutant};
    use crate::store::contract_suite;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn etag(s: &str) -> Cond {
        Cond::IfMatch(Etag(s.to_string()))
    }

    #[test]
    fn a_write_carries_its_condition_and_nothing_else() {
        assert_eq!(
            cond_headers(&etag("\"0x8DC\"")),
            vec![("if-match", "\"0x8DC\"".to_string())]
        );
        assert_eq!(
            cond_headers(&Cond::IfNoneMatch),
            vec![("if-none-match", "*".to_string())]
        );
        assert!(cond_headers(&Cond::Unconditional).is_empty());
    }

    #[test]
    fn head_write_answers_map_to_the_named_outcomes() {
        let m = etag("\"a\"");
        let done = WriteOutcome::Done(Etag("\"b\"".into()));
        assert_eq!(map_head_write(201, Some("\"b\""), None, &m), done);
        // Applied without an ETag in the reply: unknown, never a guess.
        assert_eq!(map_head_write(201, None, None, &m), WriteOutcome::Unknown);
        assert_eq!(
            map_head_write(412, None, Some("ConditionNotMet"), &m),
            WriteOutcome::PreconditionFailed
        );
        assert_eq!(
            map_head_write(409, None, Some("BlobAlreadyExists"), &Cond::IfNoneMatch),
            WriteOutcome::PreconditionFailed
        );
        // A 409 to a renew is not a fence.
        assert_eq!(
            map_head_write(409, None, None, &m),
            WriteOutcome::Unreachable
        );
        assert_eq!(
            map_head_write(404, None, Some("BlobNotFound"), &m),
            WriteOutcome::PreconditionFailed
        );
        // A missing container is a broken deployment, not a fence.
        assert_eq!(
            map_head_write(404, None, Some("ContainerNotFound"), &m),
            WriteOutcome::Unreachable
        );
        for s in [408, 500, 502, 503, 504] {
            assert_eq!(map_head_write(s, None, None, &m), WriteOutcome::Unknown);
        }
        for s in [400, 401, 403, 429] {
            assert_eq!(map_head_write(s, None, None, &m), WriteOutcome::Unreachable);
        }
    }

    #[test]
    fn object_put_answers_map_to_the_named_outcomes() {
        assert_eq!(map_put(201), PutOutcome::Created);
        assert_eq!(map_put(409), PutOutcome::PreconditionFailed);
        assert_eq!(map_put(412), PutOutcome::PreconditionFailed);
        assert_eq!(map_put(500), PutOutcome::Unknown);
        assert_eq!(map_put(503), PutOutcome::Unknown);
        assert_eq!(map_put(408), PutOutcome::Unknown);
        assert_eq!(map_put(403), PutOutcome::Unreachable);
        assert_eq!(map_put(404), PutOutcome::Unreachable);
    }

    #[test]
    fn only_a_labelled_blob_not_found_is_a_missing_head() {
        assert_eq!(
            map_head_read(200, Some("\"e\""), None, b"{}".to_vec()),
            Ok(Some((b"{}".to_vec(), Etag("\"e\"".into()))))
        );
        assert_eq!(
            map_head_read(404, None, Some("BlobNotFound"), Vec::new()),
            Ok(None)
        );
        // Every other answer is an error, so the lease never creates a
        // second HEAD because of it.
        assert!(map_head_read(404, None, Some("ContainerNotFound"), Vec::new()).is_err());
        assert!(map_head_read(404, None, None, Vec::new()).is_err());
        assert!(map_head_read(200, None, None, b"{}".to_vec()).is_err());
        for s in [204, 301, 401, 403, 429, 500, 503] {
            assert!(map_head_read(s, Some("\"e\""), None, Vec::new()).is_err());
        }
        let big = vec![b'x'; MAX_HEAD_BYTES + 1];
        assert!(map_head_read(200, Some("\"e\""), None, big).is_err());
    }

    #[test]
    fn the_string_to_sign_follows_the_blob_service_layout() {
        let headers = vec![
            ("if-match", "\"0x1\"".to_string()),
            ("x-ms-blob-type", "BlockBlob".to_string()),
            ("content-type", "application/json".to_string()),
            ("x-ms-version", API_VERSION.to_string()),
            ("x-ms-date", "Sun, 06 Nov 1994 08:49:37 GMT".to_string()),
        ];
        let s = string_to_sign("PUT", 7, &headers, "/acct/acct/lease/HEAD.json");
        assert_eq!(
            s,
            "PUT\n\n\n7\n\napplication/json\n\n\n\"0x1\"\n\n\n\n\
x-ms-blob-type:BlockBlob\nx-ms-date:Sun, 06 Nov 1994 08:49:37 GMT\n\
x-ms-version:2023-11-03\n/acct/acct/lease/HEAD.json"
        );
        // No body: the length field is empty, not 0.
        let g = string_to_sign(
            "GET",
            0,
            &[("x-ms-version", "v".to_string())],
            "/a/a/c\ncomp:list\nrestype:container",
        );
        let want = format!(
            "GET{}x-ms-version:v\n/a/a/c\ncomp:list\nrestype:container",
            "\n".repeat(12)
        );
        assert_eq!(g, want);
    }

    #[test]
    fn dates_names_lists_and_tokens_are_read_as_documented() {
        assert_eq!(rfc1123(784_111_777), "Sun, 06 Nov 1994 08:49:37 GMT");
        assert_eq!(rfc1123(0), "Thu, 01 Jan 1970 00:00:00 GMT");
        assert_eq!(rfc1123(1_709_164_800), "Thu, 29 Feb 2024 00:00:00 GMT");

        assert_eq!(pct("seg/e01/s 1.gz", true), "seg/e01/s%201.gz");
        assert_eq!(pct("a/b&c", false), "a%2Fb%26c");
        assert!(name_ok("seg/e0000000001/s0000000001.prec.gz"));
        for bad in ["", "/abs", "../x", "a/../b", "a/./b", "a/", "a\nb"] {
            assert!(!name_ok(bad), "{bad:?}");
        }

        let xml = "<?xml version=\"1.0\"?><EnumerationResults><Blobs>\
<Blob><Name>seg/a</Name><Properties><Etag>x</Etag></Properties></Blob>\
<Blob><Name>seg/b&amp;c</Name></Blob></Blobs>\
<NextMarker>m1</NextMarker></EnumerationResults>";
        assert_eq!(
            parse_list(xml),
            Ok((
                vec!["seg/a".to_string(), "seg/b&c".to_string()],
                Some("m1".to_string())
            ))
        );
        let last = "<EnumerationResults><Blobs /><NextMarker /></EnumerationResults>";
        assert_eq!(parse_list(last), Ok((Vec::new(), None)));
        // A cut-off page is an error, not a short list.
        assert!(parse_list("<EnumerationResults><Blobs><Blob><Name>a</Name>").is_err());
        assert!(parse_list("<html>busy</html>").is_err());

        assert_eq!(
            parse_token(r#"{"access_token":"t","expires_in":"3599"}"#),
            Ok(("t".to_string(), 3_599))
        );
        assert_eq!(
            parse_token(r#"{"access_token":"t","expires_in":60}"#),
            Ok(("t".to_string(), 60))
        );
        assert!(parse_token(r#"{"error":"invalid_request"}"#).is_err());
        assert!(parse_token("<html>").is_err());
    }

    #[test]
    fn a_key_is_refused_off_loopback_and_plain_http_off_loopback() {
        let key = Auth::EmulatorKey {
            account: "a".into(),
            key: vec![1, 2, 3],
        };
        let mut c = AzureConfig::new("https://acct.blob.core.windows.net", None);
        assert_eq!(
            c.validate(),
            Ok((
                "https://acct.blob.core.windows.net".to_string(),
                String::new()
            ))
        );
        c.auth = key.clone();
        assert!(c.validate().is_err(), "a key against a real account");
        let plain = AzureConfig::new("http://acct.blob.core.windows.net", None);
        assert!(plain.validate().is_err());
        let mut e = AzureConfig::new("http://127.0.0.1:10000/devacct/", None);
        e.auth = key;
        assert_eq!(
            e.validate(),
            Ok((
                "http://127.0.0.1:10000/devacct".to_string(),
                "/devacct".to_string()
            ))
        );
        e.head_container = "Lease".into();
        assert!(e.validate().is_err());
        assert!(!format!("{:?}", e.auth).contains("1, 2, 3"), "key printed");
    }

    // The tests below talk to the Azurite emulator and are ignored unless
    // asked for. CI runs them with `--ignored` beside an Azurite service
    // container, where a missing emulator fails them.

    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn env(name: &str) -> String {
        std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set for the emulator tests"))
    }

    /// A store on two fresh containers.
    fn fresh(tag: &str) -> AzureStore {
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_micros());
        let id = format!("{tag}-{}-{n}-{t}", std::process::id());
        let (lease, archive) = (format!("l-{id}"), format!("a-{id}"));
        let s = AzureStore::emulator(
            &env("POLY_AZURITE_URL"),
            &env("POLY_AZURITE_ACCOUNT"),
            &env("POLY_AZURITE_KEY"),
            &lease,
            &archive,
        )
        .expect("emulator store");
        s.create_container(&lease).expect("lease container");
        s.create_container(&archive).expect("archive container");
        s
    }

    fn scratch(tag: &str) -> std::path::PathBuf {
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let d = std::env::temp_dir().join(format!("ha-azure-{}-{tag}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// Records every applied HEAD write, the way the directory store's
    /// journal does, so a run can be audited.
    struct Journaled<'a> {
        store: &'a AzureStore,
        journal: &'a Mutex<Vec<Head>>,
    }

    impl HeadStore for Journaled<'_> {
        fn read_head(&self) -> HeadRead {
            self.store.read_head()
        }
        fn write_head(&self, body: &[u8], cond: &Cond) -> WriteOutcome {
            // One lock around the write and the append keeps the journal
            // in the order the store applied the writes.
            let mut j = self.journal.lock().unwrap();
            let out = self.store.write_head(body, cond);
            if matches!(out, WriteOutcome::Done(_)) {
                j.push(Head::decode(body).expect("a head"));
            }
            out
        }
    }

    fn step<S: HeadStore>(m: &mut CasLease, s: &S, now: u64) {
        crate::caslease::drive(m, s, &mut || now);
    }

    fn head(s: &AzureStore) -> Head {
        Head::decode(&s.read_head().unwrap().expect("a head").0).expect("a readable head")
    }

    #[test]
    #[ignore = "needs the Azurite emulator"]
    fn emulator_the_contract_holds() {
        let s = fresh("contract");
        let d = scratch("contract");
        contract_suite(&s, &d, "selftest/").unwrap();
        // The same suite with HEAD and the objects in one container, as the
        // deploy self test runs it: HEAD is never listed as an object.
        let mut cfg = s.config().clone();
        cfg.head_container = cfg.object_container.clone();
        cfg.head_blob = "selftest2/HEAD.json".into();
        let one = AzureStore::new(cfg).unwrap();
        contract_suite(&one, &d, "selftest2/").unwrap();
        assert_eq!(
            one.put_new("selftest2/HEAD.json", &d.join("nothing")),
            PutOutcome::Unreachable
        );
        let _ = std::fs::remove_dir_all(d);
    }

    #[test]
    #[ignore = "needs the Azurite emulator"]
    fn emulator_a_stale_etag_never_lands_and_an_unconditional_write_does() {
        let s = fresh("stale");
        let WriteOutcome::Done(e1) = s.write_head(b"one", &Cond::IfNoneMatch) else {
            panic!("create");
        };
        let WriteOutcome::Done(e2) = s.write_head(b"two", &Cond::IfMatch(e1.clone())) else {
            panic!("update");
        };
        // The fence: a writer still holding the old ETag is refused and
        // HEAD keeps the newer body, however often it tries.
        for _ in 0..3 {
            assert_eq!(
                s.write_head(b"zombie", &Cond::IfMatch(e1.clone())),
                WriteOutcome::PreconditionFailed
            );
        }
        assert_eq!(s.read_head().unwrap(), Some((b"two".to_vec(), e2.clone())));
        // What the condition is for: without it the same write lands.
        let WriteOutcome::Done(e3) = s.write_head(b"zombie", &Cond::Unconditional) else {
            panic!("unconditional");
        };
        assert_ne!(e3, e2);
        assert_eq!(s.read_head().unwrap().unwrap().0, b"zombie".to_vec());
        assert_eq!(
            s.write_head(b"a\nb", &Cond::IfMatch(e3)),
            WriteOutcome::Unreachable
        );
    }

    #[test]
    #[ignore = "needs the Azurite emulator"]
    fn emulator_racing_writers_lose_no_update() {
        let s = fresh("race");
        let wins = Arc::new(AtomicU64::new(0));
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let (s, wins) = (s.clone(), wins.clone());
                std::thread::spawn(move || {
                    for _ in 0..40 {
                        let (n, cond) = match s.read_head().unwrap() {
                            None => (0u64, Cond::IfNoneMatch),
                            Some((b, e)) => (
                                String::from_utf8(b).unwrap().parse::<u64>().unwrap(),
                                Cond::IfMatch(e),
                            ),
                        };
                        let body = (n + 1).to_string();
                        if let WriteOutcome::Done(_) = s.write_head(body.as_bytes(), &cond) {
                            wins.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        // Every applied write was based on the one before it, so the
        // counter in HEAD equals the number of writes that were applied.
        let (b, _) = s.read_head().unwrap().unwrap();
        let n: u64 = String::from_utf8(b).unwrap().parse().unwrap();
        assert_eq!(n, wins.load(Ordering::Relaxed));
        assert!(n >= 40, "only {n} writes landed");
    }

    #[test]
    #[ignore = "needs the Azurite emulator"]
    fn emulator_objects_are_written_once_and_listed_across_pages() {
        let s = fresh("objects");
        let d = scratch("objects");
        let src = d.join("src");
        std::fs::write(&src, b"first").unwrap();
        let other = d.join("other");
        std::fs::write(&other, b"second").unwrap();
        for bad in ["../x", "/abs", "", "seg/../x"] {
            assert_eq!(s.put_new(bad, &src), PutOutcome::Unreachable, "{bad:?}");
        }
        assert_eq!(
            s.put_new("seg/a", &d.join("missing")),
            PutOutcome::Unreachable
        );
        let names: Vec<String> = (0..5).map(|i| format!("seg/e1/s{i}.prec.gz")).collect();
        for n in &names {
            assert_eq!(s.put_new(n, &src), PutOutcome::Created);
        }
        // A second writer cannot replace an object.
        assert_eq!(s.put_new(&names[0], &other), PutOutcome::PreconditionFailed);
        assert_eq!(s.put_new("drill/x", &src), PutOutcome::Created);
        // Two names a page, so the listing takes three requests.
        let inner = &s.0;
        let paged = AzureStore(Arc::new(Inner {
            cfg: inner.cfg.clone(),
            base: inner.base.clone(),
            base_path: inner.base_path.clone(),
            http: inner.http.clone(),
            token: Mutex::new(None),
            page: 2,
        }));
        assert_eq!(paged.list("seg/").unwrap(), names);
        assert_eq!(s.list("seg/").unwrap(), names);
        assert_eq!(s.list("").unwrap().len(), 6);
        assert!(s.list("nothing/").unwrap().is_empty());
        let _ = std::fs::remove_dir_all(d);
    }

    #[test]
    #[ignore = "needs the Azurite emulator"]
    fn emulator_a_missing_container_is_an_error_never_a_missing_head() {
        let s = fresh("nocontainer");
        let mut cfg = s.config().clone();
        cfg.head_container = "no-such-container".into();
        cfg.object_container = "no-such-container".into();
        let gone = AzureStore::new(cfg).unwrap();
        assert!(gone.read_head().is_err(), "read as a missing HEAD");
        assert_eq!(
            gone.write_head(b"x", &Cond::IfNoneMatch),
            WriteOutcome::Unreachable
        );
        assert_eq!(
            gone.write_head(b"x", &etag("\"0x1\"")),
            WriteOutcome::Unreachable
        );
        assert!(gone.list("").is_err());
        // The same through the lease: it must not believe it leads.
        let mut a = CasLease::new(Config::new(1, 8));
        step(&mut a, &gone, 0);
        assert!(!a.leads(0));
        assert_eq!(a.stats().grants, 0);
    }

    #[test]
    #[ignore = "needs the Azurite emulator"]
    fn emulator_a_wrong_key_writes_nothing() {
        let s = fresh("wrongkey");
        let mut cfg = s.config().clone();
        if let Auth::EmulatorKey { key, .. } = &mut cfg.auth {
            key[0] ^= 0xff;
        }
        let bad = AzureStore::new(cfg).unwrap();
        assert!(bad.read_head().is_err());
        assert_eq!(
            bad.write_head(b"x", &Cond::IfNoneMatch),
            WriteOutcome::Unreachable
        );
        assert_eq!(s.read_head(), Ok(None));
    }

    /// a leads and commits, freezes past its deadline, b takes over, a
    /// wakes. The lease runs on its own clock, so the test passes the times
    /// in and nothing waits.
    fn zombie(tag: &str, m: LeaseMutant) -> (AzureStore, CasLease, Vec<Head>) {
        let s = fresh(tag);
        let journal = Mutex::new(Vec::new());
        // Two handles on one account, as two nodes have.
        let (sa, sb) = (s.clone(), AzureStore::new(s.config().clone()).unwrap());
        let (ja, jb) = (
            Journaled {
                store: &sa,
                journal: &journal,
            },
            Journaled {
                store: &sb,
                journal: &journal,
            },
        );
        let mut a = CasLease::new(Config::new(1, 8).with_mutant(m));
        let mut b = CasLease::new(Config::new(2, 9));
        step(&mut a, &ja, 0);
        assert!(a.leads(0));
        a.commit(5);
        step(&mut a, &ja, 1);
        for t in (0..=35_000).step_by(5_000) {
            assert_eq!(head(&s).holder, Some(1), "taken over early at {t}");
            step(&mut b, &jb, t);
        }
        assert!(b.leads(35_000));
        assert_eq!(head(&s).holder, Some(2));
        assert_eq!(head(&s).epoch, 2);
        // The zombie wakes with a slot to commit.
        a.commit(6);
        step(&mut a, &ja, 40_000);
        let j = journal.lock().unwrap().clone();
        (s, a, j)
    }

    #[test]
    #[ignore = "needs the Azurite emulator"]
    fn emulator_a_zombie_leader_is_fenced_by_the_conditional_write() {
        let (s, a, journal) = zombie("zombie", LeaseMutant::None);
        assert_eq!(a.stats().fence_rejected, 1);
        assert!(!a.leads(40_000));
        assert_eq!(a.held_epoch(), None);
        let h = head(&s);
        assert_eq!((h.epoch, h.holder), (2, Some(2)));
        assert_eq!(h.committed_to(), Some(5), "the zombie's slot 6 landed");
        // Create, commit of slot 5, takeover. Nothing of the zombie's.
        assert_eq!(journal.len(), 3);
        let au = audit(&journal);
        assert_eq!(au.reason(), None);
        assert_eq!(au.grants, 2);
    }

    #[test]
    #[ignore = "needs the Azurite emulator"]
    fn emulator_without_if_match_the_zombie_lands_and_the_audit_sees_it() {
        let (s, _, journal) = zombie("nofence", LeaseMutant::Unconditional);
        assert_eq!(head(&s).holder, Some(1), "the zombie's write landed");
        let au = audit(&journal);
        assert!(au.stale_epoch_commits >= 1);
        assert_eq!(au.reason(), Some("stale_epoch_commit"));
    }
}
