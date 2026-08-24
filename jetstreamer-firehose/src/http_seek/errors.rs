//! Error types surfaced by [`Seekable`](super::Seekable); each keeps its cause so the
//! firehose log shows the full `reqwest -> hyper` chain instead of `error decoding response body`.

use std::error::Error as StdError;
use std::fmt::{self, Display};
use std::io::Error as IoError;

use reqwest::StatusCode;

/// `Display` of `err` followed by every `source()` joined with ` -> `.
pub(super) fn error_chain(err: &dyn StdError) -> String {
    let mut out = err.to_string();
    let mut src = err.source();
    while let Some(e) = src {
        out.push_str(" -> ");
        out.push_str(&e.to_string());
        src = e.source();
    }
    out
}

#[derive(Debug)]
pub(super) struct RangeStatus {
    pub(super) status: StatusCode,
    pub(super) position: u64,
}

impl RangeStatus {
    pub(super) fn retryable(&self) -> bool {
        self.status.is_server_error() || self.status == StatusCode::TOO_MANY_REQUESTS
    }
}

impl Display for RangeStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "range GET pos={} returned {}",
            self.position, self.status
        )
    }
}

impl StdError for RangeStatus {}

#[derive(Debug)]
pub(super) struct RangeMismatch {
    pub(super) requested: u64,
    pub(super) served: u64,
}

impl Display for RangeMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "range GET requested pos={} but Content-Range starts at {}",
            self.requested, self.served
        )
    }
}

impl StdError for RangeMismatch {}

#[derive(Debug)]
pub(super) struct ShortBody {
    pub(super) position: u64,
    pub(super) expected_end: Option<u64>,
}

impl Display for ShortBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "body ended at pos={} before range end {:?}",
            self.position, self.expected_end
        )
    }
}

impl StdError for ShortBody {}

#[derive(Debug)]
pub(super) struct ReopenExhausted {
    pub(super) attempts: u32,
    pub(super) position: u64,
    pub(super) source: IoError,
}

impl Display for ReopenExhausted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "http range reopen gave up after {} failure(s) at pos={}: {}",
            self.attempts,
            self.position,
            error_chain(&self.source)
        )
    }
}

impl StdError for ReopenExhausted {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        Some(&self.source)
    }
}
