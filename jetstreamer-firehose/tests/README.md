# jetstreamer-firehose tests

| 파일 | 검증 대상 | 목적 | ~runtime | approved_by |
|---|---|---|---|---|
| `http_seek_resume.rs` | `http_seek::Seekable` (`AsyncRead` + `AsyncSeek`) | Old Faithful HTTP/1.1 `Range` 바디가 끊겼을 때 (`hyper` `IncompleteBody`, `Content-Length` 없는 clean EOF, 5xx on open) 현재 byte offset에서 다시 열고 같은 바이트를 이어 읽는지. 재시도 소진·4xx는 `UnexpectedEof`가 아닌 에러로 드러나는지 (`NodeReader`가 EOF로 오인하지 않도록). | ~4s (`gives_up_after_repeated_failures_without_progress`, reopen backoff 합계), 나머지 <1s | — |

## 작성 이유

- 2026-08-23 epoch 1018 ingest(tsw-am-1)에서 Cloudflare가 Range 스트림을 중간에 끊어 `reqwest Decode -> hyper IncompleteBody`가 firehose까지 올라가 32초 backoff·재시작(또는 fail-fast exit)으로 이어졌다. 파서가 아니라 HTTP 리더에서 흡수해야 하므로 리더 단위 integration test로 고정한다.
- 외부 네트워크 없이 재현하기 위해 loopback에 의도적으로 바디를 자르는 HTTP/1.1 서버를 띄운다. 서버는 data 요청마다 `Reply`(Full / TruncateAfter / NoLengthTruncateAfter / Status)를 고르고, 요청 offset을 기록해 "현재 offset에서 다시 열었는지"를 단언한다.

## 실행

```bash
cargo test -p jetstreamer-firehose --no-run --test http_seek_resume   # compile — timeout 없음
cargo test -p jetstreamer-firehose --test http_seek_resume            # runtime — 실행 제한 10s
```
