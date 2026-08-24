//! Seekable HTTP reader over `Range` requests that resumes a truncated body.
//!
//! Differences from crates.io `rseek`:
//! - the reqwest error source chain is kept (`rseek` maps errors through `to_string()`,
//!   which hides `hyper::Error(Body, IncompleteBody)` behind `error decoding response body`)
//! - the streaming GET status is checked; `200` is accepted only for a range starting at `0`
//! - when the body ends before the requested range (connection cut, `IncompleteBody`, clean
//!   EOF before `Content-Length`) the range is reopened at the current byte offset instead of
//!   surfacing the error to the CAR parser
//! - reopen attempts are bounded per read so a dead origin still surfaces (kind `Other`,
//!   never `UnexpectedEof`, which `NodeReader` treats as end of stream)

use std::future::Future;
use std::io::{Error as IoError, ErrorKind, Result as IoResult};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures_util::TryStreamExt;
use reqwest::{RequestBuilder, Response, StatusCode};
use tokio::io::{AsyncRead, AsyncSeek, ReadBuf, SeekFrom};
use tokio_util::io::StreamReader;

use crate::LOG_MODULE;

mod errors;

use errors::{RangeMismatch, RangeStatus, ReopenExhausted, ShortBody, error_chain};

/// Consecutive reopen attempts without reading a single byte before the error is surfaced.
/// Together with the delays below this stays well inside the firehose `OP_TIMEOUT` (15s).
const MAX_REOPEN_ATTEMPTS: u32 = 6;
/// Delay before the second reopen attempt; doubles per attempt up to `REOPEN_DELAY_MAX`.
const REOPEN_DELAY_BASE: Duration = Duration::from_millis(100);
const REOPEN_DELAY_MAX: Duration = Duration::from_millis(1_600);

type BodyStream = Pin<Box<dyn futures_util::Stream<Item = Result<Bytes, IoError>> + Send + Sync>>;
type OpenFuture = Pin<Box<dyn Future<Output = IoResult<Response>> + Send + Sync>>;

enum State {
    Opening(OpenFuture),
    Streaming(StreamReader<BodyStream, Bytes>),
    /// Reopen budget exhausted or non-retryable failure; sticky until the next seek.
    Failed,
}

/// Seekable HTTP range reader; each seek (and each resume) issues a new `Range` GET.
pub struct Seekable<F>
where
    F: Fn() -> RequestBuilder + Send + Sync + 'static,
{
    factory: F,
    /// Total file size from the `bytes=0-0` probe, `None` when the probe failed.
    pub file_size: Option<u64>,
    /// Current byte offset; the next read starts here.
    pub position: u64,
    state: State,
    /// Open/stream failures since the last byte was read.
    reopen_failures: u32,
}

impl<F> Unpin for Seekable<F> where F: Fn() -> RequestBuilder + Send + Sync + 'static {}

impl<F> Seekable<F>
where
    F: Fn() -> RequestBuilder + Send + Sync + 'static,
{
    /// Probes the file size and schedules the first range GET at offset `0`.
    pub async fn new(factory: F) -> Self {
        let mut s = Seekable {
            factory,
            file_size: None,
            position: 0,
            state: State::Failed,
            reopen_failures: 0,
        };
        if let Ok(sz) = s.fetch_file_size().await {
            s.file_size = Some(sz);
        }
        s.state = State::Opening(s.open_range(Duration::ZERO));
        s
    }

    /// Returns the total size reported by a `bytes=0-0` range probe.
    pub async fn fetch_file_size(&self) -> IoResult<u64> {
        let req = (self.factory)().header("Range", "bytes=0-0");
        let resp = req.send().await.map_err(io_from_reqwest)?;
        if resp.status() != StatusCode::PARTIAL_CONTENT {
            return Err(IoError::new(
                ErrorKind::Unsupported,
                format!("size probe status {}", resp.status()),
            ));
        }
        content_range_total(&resp).ok_or_else(|| IoError::other("failed to determine file size"))
    }

    fn open_range(&self, delay: Duration) -> OpenFuture {
        let pos = self.position;
        let range = match self.file_size {
            Some(sz) => format!("bytes={}-{}", pos, sz.saturating_sub(1)),
            None => format!("bytes={pos}-"),
        };
        let builder = (self.factory)().header("Range", range);
        Box::pin(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            let resp = builder.send().await.map_err(io_from_reqwest)?;
            check_range_response(&resp, pos)?;
            log::debug!(
                target: LOG_MODULE,
                "of http range opened pos={pos} status={} version={:?} len={:?}",
                resp.status(),
                resp.version(),
                resp.content_length()
            );
            Ok(resp)
        })
    }

    fn at_end(&self) -> bool {
        self.file_size.is_some_and(|sz| self.position >= sz)
    }

    /// Records a failure and either schedules a reopen at `position` or parks in `Failed`.
    fn reopen_or_fail(&mut self, cause: IoError) -> IoResult<()> {
        self.reopen_failures += 1;
        let retryable = is_retryable(&cause);
        if !retryable || self.reopen_failures > MAX_REOPEN_ATTEMPTS {
            log::error!(
                target: LOG_MODULE,
                "of http range pos={} giving up after {} failure(s) (retryable={retryable}): {}",
                self.position,
                self.reopen_failures,
                error_chain(&cause)
            );
            self.state = State::Failed;
            return Err(IoError::other(ReopenExhausted {
                attempts: self.reopen_failures,
                position: self.position,
                source: cause,
            }));
        }
        let delay = reopen_delay(self.reopen_failures);
        log::warn!(
            target: LOG_MODULE,
            "of http range interrupted pos={} attempt={}/{MAX_REOPEN_ATTEMPTS} reopen_in={delay:?}: {}",
            self.position,
            self.reopen_failures,
            error_chain(&cause)
        );
        self.state = State::Opening(self.open_range(delay));
        Ok(())
    }

    fn start_streaming(&mut self, resp: Response) {
        let stream = resp.bytes_stream().map_err(io_from_reqwest);
        self.state = State::Streaming(StreamReader::new(Box::pin(stream)));
    }

    /// Called once bytes flow again; a reopen only counts as resumed when the body yields data.
    fn note_progress(&mut self) {
        if self.reopen_failures > 0 {
            log::info!(
                target: LOG_MODULE,
                "of http range resumed pos={} after {} failure(s)",
                self.position,
                self.reopen_failures
            );
            self.reopen_failures = 0;
        }
    }
}

impl<F> AsyncRead for Seekable<F>
where
    F: Fn() -> RequestBuilder + Send + Sync + 'static,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<IoResult<()>> {
        let this = self.get_mut();
        if this.at_end() {
            return Poll::Ready(Err(IoError::new(ErrorKind::UnexpectedEof, "EOF reached")));
        }
        loop {
            match &mut this.state {
                State::Streaming(reader) => {
                    let before = buf.filled().len();
                    match Pin::new(reader).poll_read(cx, buf) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Ok(())) => {
                            let read = (buf.filled().len() - before) as u64;
                            if read > 0 {
                                this.note_progress();
                                this.position += read;
                                return Poll::Ready(Ok(()));
                            }
                            if buf.remaining() == 0 || this.file_size.is_none() {
                                return Poll::Ready(Ok(()));
                            }
                            // Clean EOF before the requested range end: same as a cut body.
                            this.reopen_or_fail(IoError::other(ShortBody {
                                position: this.position,
                                expected_end: this.file_size,
                            }))?;
                        }
                        Poll::Ready(Err(err)) => this.reopen_or_fail(err)?,
                    }
                }
                State::Opening(fut) => match fut.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(resp)) => this.start_streaming(resp),
                    Poll::Ready(Err(err)) => this.reopen_or_fail(err)?,
                },
                State::Failed => {
                    return Poll::Ready(Err(IoError::other(format!(
                        "http range stream closed after repeated failures at pos={}; seek to reopen",
                        this.position
                    ))));
                }
            }
        }
    }
}

impl<F> AsyncSeek for Seekable<F>
where
    F: Fn() -> RequestBuilder + Send + Sync + 'static,
{
    fn start_seek(self: Pin<&mut Self>, position: SeekFrom) -> IoResult<()> {
        let this = self.get_mut();
        let new_pos = match position {
            SeekFrom::Start(n) => n,
            SeekFrom::Current(off) => {
                let tmp = this.position as i64 + off;
                if tmp < 0 {
                    return Err(IoError::new(ErrorKind::InvalidInput, "negative seek"));
                }
                tmp as u64
            }
            SeekFrom::End(off) => {
                let sz = this
                    .file_size
                    .ok_or_else(|| IoError::new(ErrorKind::Unsupported, "length unknown"))?;
                let tmp = sz as i64 + off;
                if tmp < 0 {
                    return Err(IoError::new(ErrorKind::InvalidInput, "negative seek"));
                }
                tmp as u64
            }
        };
        this.position = new_pos.min(this.file_size.unwrap_or(u64::MAX));
        this.reopen_failures = 0;
        this.state = State::Opening(this.open_range(Duration::ZERO));
        Ok(())
    }

    fn poll_complete(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<IoResult<u64>> {
        Poll::Ready(Ok(self.get_mut().position))
    }
}

fn reopen_delay(failures: u32) -> Duration {
    match failures {
        0 | 1 => Duration::ZERO,
        n => REOPEN_DELAY_BASE
            .saturating_mul(1u32 << (n - 2).min(16))
            .min(REOPEN_DELAY_MAX),
    }
}

/// Validates the streaming GET: `206` always, `200` only for a range starting at `0`,
/// and the `Content-Range` start must match the requested offset.
fn check_range_response(resp: &Response, pos: u64) -> IoResult<()> {
    let status = resp.status();
    let acceptable =
        status == StatusCode::PARTIAL_CONTENT || (status == StatusCode::OK && pos == 0);
    if !acceptable {
        return Err(IoError::other(RangeStatus {
            status,
            position: pos,
        }));
    }
    if status == StatusCode::PARTIAL_CONTENT
        && let Some(start) = content_range_start(resp)
        && start != pos
    {
        return Err(IoError::other(RangeMismatch {
            requested: pos,
            served: start,
        }));
    }
    Ok(())
}

fn content_range_header(resp: &Response) -> Option<&str> {
    resp.headers().get("content-range")?.to_str().ok()
}

/// `bytes START-END/TOTAL` → `START`.
fn content_range_start(resp: &Response) -> Option<u64> {
    let spec = content_range_header(resp)?.trim().strip_prefix("bytes ")?;
    spec.split_once('-')?.0.trim().parse().ok()
}

/// `bytes START-END/TOTAL` → `TOTAL`.
fn content_range_total(resp: &Response) -> Option<u64> {
    content_range_header(resp)?
        .split('/')
        .nth(1)?
        .trim()
        .parse()
        .ok()
}

fn is_retryable(err: &IoError) -> bool {
    err.get_ref()
        .and_then(|inner| inner.downcast_ref::<RangeStatus>())
        .is_none_or(|status| status.retryable())
}

fn io_from_reqwest(err: reqwest::Error) -> IoError {
    IoError::other(err)
}
