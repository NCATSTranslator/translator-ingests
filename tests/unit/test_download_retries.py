"""Download retries for sources listed in DOWNLOAD_ATTEMPTS_OVERRIDES, against a real local HTTP server."""

from collections import Counter
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread

import pytest

from translator_ingest.pipeline import DOWNLOAD_ATTEMPTS_OVERRIDES, download_with_retries


class FlakyServer(ThreadingHTTPServer):
    """Serve fixed content, failing each path's first `failures` requests with HTTP 503."""

    def __init__(self, failures: int):
        super().__init__(("127.0.0.1", 0), FlakyHandler)
        self.failures = failures
        self.requests: Counter[str] = Counter()


class FlakyHandler(BaseHTTPRequestHandler):
    server: FlakyServer

    def do_GET(self) -> None:
        self.server.requests[self.path] += 1
        if self.server.requests[self.path] <= self.server.failures:
            self.send_response(503)
            self.end_headers()
            return
        body = b"col\nvalue\n"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args) -> None:
        pass


@pytest.fixture
def serve(request) -> Iterator[FlakyServer]:
    server = FlakyServer(failures=request.param)
    Thread(target=server.serve_forever, daemon=True).start()
    yield server
    server.shutdown()


def _download_yaml(tmp_path: Path, server: FlakyServer) -> str:
    port = server.server_address[1]
    yaml_file = tmp_path / "download.yaml"
    yaml_file.write_text(
        f"- url: http://127.0.0.1:{port}/complexportal_9606.tsv\n  local_name: complexportal_9606.tsv\n"
        f"- url: http://127.0.0.1:{port}/complexportal_10090.tsv\n  local_name: complexportal_10090.tsv\n"
    )
    return str(yaml_file)


@pytest.mark.parametrize("serve", [1], indirect=True)
def test_failed_files_are_retried_until_they_succeed(tmp_path: Path, serve: FlakyServer) -> None:
    output = tmp_path / "source_data"
    output.mkdir()

    report = download_with_retries(_download_yaml(tmp_path, serve), str(output), attempts=3, retry_delay_seconds=0)

    assert sorted(path.name for path in report.downloaded) == ["complexportal_10090.tsv", "complexportal_9606.tsv"]
    assert report.failed == [] and report.skipped == []
    assert (output / "complexportal_9606.tsv").read_text() == "col\nvalue\n"
    assert serve.requests == {"/complexportal_9606.tsv": 2, "/complexportal_10090.tsv": 2}


@pytest.mark.parametrize("serve", [5], indirect=True)
def test_persistent_failure_raises_after_the_last_attempt(tmp_path: Path, serve: FlakyServer) -> None:
    output = tmp_path / "source_data"
    output.mkdir()

    with pytest.raises(RuntimeError, match="after 2 attempt"):
        download_with_retries(_download_yaml(tmp_path, serve), str(output), attempts=2, retry_delay_seconds=0)
    assert serve.requests == {"/complexportal_9606.tsv": 2, "/complexportal_10090.tsv": 2}


def test_only_gtopdb_retries_downloads() -> None:
    """The retry override is the gtopdb stopgap for Complex Portal (NCATSTranslator/Babel#1277)."""
    assert DOWNLOAD_ATTEMPTS_OVERRIDES == {"gtopdb": 3}
