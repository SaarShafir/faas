"""The stub and the real client, talking to each other over a real socket.

The stub exists to feed the local stack, so the thing worth testing is not the
stub in isolation but that `AudioApiClient` -- unmodified, over HTTP -- reads
its responses the way the hydrator needs. The status-code mapping is the whole
contract: 404/410 is poison and must never be retried, everything else is
transient and must be.
"""

import threading
from http.server import ThreadingHTTPServer

import pytest

from faas_sdk.errors import PoisonMessageError, TransientError

pytest.importorskip("requests", reason="requests is a [hydrator] extra")

from faas_hydrator.audioapi import AudioApiClient  # noqa: E402
from stress.audio_api import Corpus, Handler  # noqa: E402


@pytest.fixture
def corpus_dir(tmp_path):
    """A miniature corpus -- the manifest format, not real audio, since what is
    under test is the HTTP contract."""
    import json

    (tmp_path / "hello.mp3").write_bytes(b"ID3\x04\x00pretend-this-is-audio")
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            [
                {
                    "audio_id": "hello",
                    "filename": "hello.mp3",
                    "content_type": "audio/mpeg",
                    "description": "present",
                },
                {
                    "audio_id": "deleted-recording",
                    "filename": "",
                    "content_type": "",
                    "description": "declared but absent",
                    "expect": "missing",
                },
            ]
        )
    )
    return tmp_path


@pytest.fixture
def stub(corpus_dir, request):
    Handler.corpus = Corpus(corpus_dir)
    Handler.error_rate = getattr(request, "param", {}).get("error_rate", 0.0)
    Handler.latency_ms = 0.0
    Handler.gone = frozenset(getattr(request, "param", {}).get("gone", ()))

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()


def test_a_present_recording_comes_back_verbatim(stub):
    client = AudioApiClient(base_url=stub)
    assert client.get_audio("hello") == b"ID3\x04\x00pretend-this-is-audio"


def test_an_absent_recording_is_poison_not_a_retry(stub):
    """404 means no retry will conjure it. Classifying this as transient would
    burn the whole retry budget on every call for a recording that is gone."""
    client = AudioApiClient(base_url=stub)
    with pytest.raises(PoisonMessageError):
        client.get_audio("deleted-recording")


@pytest.mark.parametrize("stub", [{"gone": ["hello"]}], indirect=True)
def test_a_deleted_recording_is_poison_too(stub):
    """410 is the §12 deletion case: the object existed and no longer does."""
    client = AudioApiClient(base_url=stub)
    with pytest.raises(PoisonMessageError):
        client.get_audio("hello")


@pytest.mark.parametrize("stub", [{"error_rate": 1.0}], indirect=True)
def test_an_upstream_error_is_transient_and_will_be_retried(stub):
    """The injected 503 is the point of the stub: without it a stress run never
    exercises retry or backoff at all."""
    client = AudioApiClient(base_url=stub)
    with pytest.raises(TransientError):
        client.get_audio("hello")


def test_an_unreachable_api_is_transient():
    """Nothing listening on the port -- the client must not treat a network
    failure as a permanently dead recording."""
    client = AudioApiClient(base_url="http://127.0.0.1:1", timeout=0.5)
    with pytest.raises(TransientError):
        client.get_audio("hello")
