//! `http_seek::Seekable` resume behaviour against a local HTTP/1.1 server that
//! truncates `Range` bodies on purpose.
//!
//! # Test runtime (compile excluded, measured 2026-08-23)
//!
//! | test | ~runtime | 원인 |
//! |---|---|---|
//! | `gives_up_after_repeated_failures_without_progress` | ~4s | reopen backoff 합계 (0+100+200+400+800+1600ms) |
//! | 나머지 | <1s | loopback 전송만 |
//!
//! 실행:
//!   cargo test --no-run --test http_seek_resume                 # compile — timeout 없음
//!   cargo test --test http_seek_resume -- --nocapture           # runtime — 실행 제한 10s

use std::io::ErrorKind;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use jetstreamer_firehose::http_seek::Seekable;
use jetstreamer_firehose::network::create_http_client;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader, SeekFrom};
use tokio::net::{TcpListener, TcpStream};

const PAYLOAD_LEN: usize = 1 << 20;

/// How the server answers one data `Range` request (size probes `bytes=0-0` are always served in full).
#[derive(Clone, Copy, Debug)]
enum Reply {
    /// `206` with `Content-Length`, whole range.
    Full,
    /// `206` with `Content-Length` of the whole range, but only `n` bytes are written before the socket closes.
    TruncateAfter(usize),
    /// `206` without `Content-Length` (`Connection: close`), only `n` bytes written, clean FIN.
    NoLengthTruncateAfter(usize),
    /// Empty response with this status.
    Status(u16),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct RangeRequest {
    start: u64,
    end: Option<u64>,
}

struct Server {
    addr: SocketAddr,
    /// Data requests in arrival order (size probes excluded).
    requests: Arc<Mutex<Vec<RangeRequest>>>,
}

impl Server {
    fn url(&self) -> String {
        format!("http://{}/epoch.car", self.addr)
    }

    fn request_starts(&self) -> Vec<u64> {
        self.requests
            .lock()
            .expect("request log poisoned")
            .iter()
            .map(|r| r.start)
            .collect()
    }
}

fn payload() -> Arc<Vec<u8>> {
    // xorshift so truncation boundaries cannot line up with a repeating pattern.
    let mut x: u32 = 0x9E37_79B9;
    let bytes = (0..PAYLOAD_LEN)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            (x & 0xFF) as u8
        })
        .collect();
    Arc::new(bytes)
}

async fn spawn_server<P>(payload: Arc<Vec<u8>>, plan: P) -> Server
where
    P: Fn(usize) -> Reply + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback listener");
    let addr = listener.local_addr().expect("listener addr");
    let requests: Arc<Mutex<Vec<RangeRequest>>> = Arc::new(Mutex::new(Vec::new()));
    let plan = Arc::new(plan);
    let log = requests.clone();
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                break;
            };
            let payload = payload.clone();
            let plan = plan.clone();
            let log = log.clone();
            tokio::spawn(async move {
                let _ = serve_connection(socket, payload, plan, log).await;
            });
        }
    });
    Server { addr, requests }
}

async fn serve_connection(
    socket: TcpStream,
    payload: Arc<Vec<u8>>,
    plan: Arc<dyn Fn(usize) -> Reply + Send + Sync>,
    log: Arc<Mutex<Vec<RangeRequest>>>,
) -> std::io::Result<()> {
    let (read_half, mut write_half) = socket.into_split();
    let mut lines = BufReader::new(read_half).lines();
    let mut range: Option<RangeRequest> = None;
    while let Some(line) = lines.next_line().await? {
        if line.is_empty() {
            break;
        }
        if let Some(value) = line
            .split_once(':')
            .filter(|(name, _)| name.eq_ignore_ascii_case("range"))
            .map(|(_, v)| v.trim())
        {
            range = parse_range(value);
        }
    }
    let total = payload.len() as u64;
    let Some(range) = range else {
        write_half
            .write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
            .await?;
        return Ok(());
    };
    let is_probe = range.start == 0 && range.end == Some(0);
    let reply = if is_probe {
        Reply::Full
    } else {
        let index = {
            let mut log = log.lock().expect("request log poisoned");
            log.push(range.clone());
            log.len() - 1
        };
        plan(index)
    };
    let start = range.start.min(total);
    let end_inclusive = range.end.map_or(total - 1, |e| e.min(total - 1));
    let body = &payload[start as usize..=end_inclusive as usize];
    let content_range = format!("bytes {start}-{end_inclusive}/{total}");
    match reply {
        Reply::Full => {
            let head = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Range: {content_range}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\n\r\n",
                body.len()
            );
            write_half.write_all(head.as_bytes()).await?;
            write_half.write_all(body).await?;
        }
        Reply::TruncateAfter(n) => {
            let head = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Range: {content_range}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\n\r\n",
                body.len()
            );
            write_half.write_all(head.as_bytes()).await?;
            write_half.write_all(&body[..n.min(body.len())]).await?;
        }
        Reply::NoLengthTruncateAfter(n) => {
            let head = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Range: {content_range}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n"
            );
            write_half.write_all(head.as_bytes()).await?;
            write_half.write_all(&body[..n.min(body.len())]).await?;
        }
        Reply::Status(code) => {
            let head = format!("HTTP/1.1 {code} Synthetic\r\nContent-Length: 0\r\n\r\n");
            write_half.write_all(head.as_bytes()).await?;
        }
    }
    write_half.shutdown().await?;
    Ok(())
}

fn parse_range(value: &str) -> Option<RangeRequest> {
    let spec = value.strip_prefix("bytes=")?;
    let (start, end) = spec.split_once('-')?;
    Some(RangeRequest {
        start: start.parse().ok()?,
        end: if end.is_empty() {
            None
        } else {
            Some(end.parse().ok()?)
        },
    })
}

async fn open(
    server: &Server,
) -> Seekable<impl Fn() -> reqwest::RequestBuilder + Send + Sync + 'static> {
    let client = create_http_client();
    let url = server.url();
    Seekable::new(move || client.get(url.clone())).await
}

#[tokio::test]
async fn resumes_at_current_offset_after_content_length_body_is_cut() {
    let payload = payload();
    let cuts = [100_000usize, 250_000, 7];
    let server = spawn_server(payload.clone(), move |i| {
        cuts.get(i)
            .map_or(Reply::Full, |n| Reply::TruncateAfter(*n))
    })
    .await;

    let mut reader = open(&server).await;
    assert_eq!(reader.file_size, Some(PAYLOAD_LEN as u64));
    let mut out = vec![0u8; PAYLOAD_LEN];
    reader
        .read_exact(&mut out)
        .await
        .expect("full payload despite three cuts");

    assert_eq!(out, *payload);
    assert_eq!(reader.position, PAYLOAD_LEN as u64);
    assert_eq!(server.request_starts(), vec![0, 100_000, 350_000, 350_007]);
}

#[tokio::test]
async fn resumes_when_body_ends_cleanly_before_range_end() {
    let payload = payload();
    let server = spawn_server(payload.clone(), |i| {
        if i == 0 {
            Reply::NoLengthTruncateAfter(65_536)
        } else {
            Reply::Full
        }
    })
    .await;

    let mut reader = open(&server).await;
    let mut out = vec![0u8; PAYLOAD_LEN];
    reader
        .read_exact(&mut out)
        .await
        .expect("full payload after clean early EOF");

    assert_eq!(out, *payload);
    assert_eq!(server.request_starts(), vec![0, 65_536]);
}

#[tokio::test]
async fn retries_server_error_on_open() {
    let payload = payload();
    let server = spawn_server(payload.clone(), |i| {
        if i == 0 {
            Reply::Status(503)
        } else {
            Reply::Full
        }
    })
    .await;

    let mut reader = open(&server).await;
    let mut out = vec![0u8; 4096];
    reader
        .read_exact(&mut out)
        .await
        .expect("503 on open is retried");

    assert_eq!(out, payload[..4096]);
    assert_eq!(server.request_starts(), vec![0, 0]);
}

#[tokio::test]
async fn client_error_on_open_is_not_retried() {
    let payload = payload();
    let server = spawn_server(payload, |_| Reply::Status(404)).await;

    let mut reader = open(&server).await;
    let mut out = vec![0u8; 16];
    let err = reader
        .read_exact(&mut out)
        .await
        .expect_err("404 must surface");

    assert_ne!(err.kind(), ErrorKind::UnexpectedEof, "{err}");
    assert_eq!(server.request_starts(), vec![0]);
}

#[tokio::test]
async fn gives_up_after_repeated_failures_without_progress() {
    let payload = payload();
    let server = spawn_server(payload, |_| Reply::TruncateAfter(0)).await;

    let mut reader = open(&server).await;
    let mut out = vec![0u8; 16];
    let started = Instant::now();
    let err = reader
        .read_exact(&mut out)
        .await
        .expect_err("no progress must surface");
    let elapsed = started.elapsed();

    assert_ne!(
        err.kind(),
        ErrorKind::UnexpectedEof,
        "a failed resume must not look like end of stream: {err}"
    );
    let attempts = server.request_starts().len();
    assert!((3..=8).contains(&attempts), "attempts={attempts}");
    assert!(elapsed < Duration::from_secs(10), "elapsed={elapsed:?}");
    // The failure is sticky until the caller seeks again.
    let again = reader
        .read_exact(&mut out)
        .await
        .expect_err("sticky failure");
    assert_ne!(again.kind(), ErrorKind::UnexpectedEof);
    assert_eq!(server.request_starts().len(), attempts);
}

#[tokio::test]
async fn seek_reopens_range_at_new_offset_and_resets_failures() {
    let payload = payload();
    let server = spawn_server(payload.clone(), |i| {
        if i == 0 {
            Reply::Status(404)
        } else {
            Reply::Full
        }
    })
    .await;

    let mut reader = open(&server).await;
    let mut out = vec![0u8; 16];
    reader
        .read_exact(&mut out)
        .await
        .expect_err("first open fails");

    reader.seek(SeekFrom::Start(300_000)).await.expect("seek");
    reader.read_exact(&mut out).await.expect("read after seek");
    assert_eq!(out, payload[300_000..300_016]);
    assert_eq!(reader.position, 300_016);

    reader
        .seek(SeekFrom::Current(-16))
        .await
        .expect("seek back");
    let mut again = vec![0u8; 16];
    reader.read_exact(&mut again).await.expect("re-read");
    assert_eq!(again, out);
    assert_eq!(server.request_starts(), vec![0, 300_000, 300_000]);
}

#[tokio::test]
async fn read_at_end_reports_unexpected_eof() {
    let payload = payload();
    let server = spawn_server(payload, |_| Reply::Full).await;

    let mut reader = open(&server).await;
    reader
        .seek(SeekFrom::Start(PAYLOAD_LEN as u64))
        .await
        .expect("seek to end");
    let mut out = vec![0u8; 16];
    let err = reader.read_exact(&mut out).await.expect_err("eof");
    assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
}
