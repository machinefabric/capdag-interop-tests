"""Incoming request chunking interoperability tests.

Tests host sending large payloads TO plugins via chunked requests.
The relay host's PluginHost automatically chunks large arguments into
REQ(empty) + CHUNK* + END sequences.

Architecture:
    Test (Engine) → Router (RelaySwitch) → Host (PluginHost) → Plugin
"""

import hashlib
import json
import pytest

from capdag.bifaci.frame import FrameType
from capdag_interop import TEST_CAPS
from capdag_interop.framework.frame_test_helper import (
    make_req_id,
    send_request,
    read_response,
    decode_cbor_response,
)
from capdag_interop.framework.test_topology import TestTopology

SUPPORTED_ROUTER_LANGS = ["rust"]
SUPPORTED_HOST_LANGS = ["rust"]
SUPPORTED_PLUGIN_LANGS = ["rust", "go", "python", "swift"]


@pytest.mark.timeout(60)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_large_incoming_payload(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Test plugin receiving 1MB payload: host sends large data, plugin returns size + checksum."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()
        size = 1024 * 1024
        pattern = b"ABCDEFGH"
        test_data = (pattern * (size // len(pattern) + 1))[:size]

        req_id = make_req_id()
        send_request(writer, req_id, TEST_CAPS["process_large"], test_data)
        output, frames = read_response(reader, timeout_frames=500)

        if isinstance(output, bytes):
            result = json.loads(output)
        else:
            result = output

        assert result["size"] == size, (
            f"[{router_lang}/{host_lang}/{plugin_lang}] size mismatch: {result['size']} vs {size}"
        )

        expected_checksum = hashlib.sha256(test_data).hexdigest()
        assert result["checksum"] == expected_checksum, (
            f"[{router_lang}/{host_lang}/{plugin_lang}] checksum mismatch"
        )



@pytest.mark.timeout(120)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_massive_incoming_payload(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Test plugin receiving 10MB payload with heavy chunking."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()
        size = 10 * 1024 * 1024
        pattern = b"0123456789ABCDEF"
        test_data = (pattern * (size // len(pattern) + 1))[:size]

        req_id = make_req_id()
        send_request(writer, req_id, TEST_CAPS["process_large"], test_data)
        output, frames = read_response(reader, timeout_frames=5000)

        if isinstance(output, bytes):
            result = json.loads(output)
        else:
            result = output

        assert result["size"] == size, (
            f"[{router_lang}/{host_lang}/{plugin_lang}] size mismatch: {result['size']} vs {size}"
        )

        expected_checksum = hashlib.sha256(test_data).hexdigest()
        assert result["checksum"] == expected_checksum, (
            f"[{router_lang}/{host_lang}/{plugin_lang}] checksum mismatch"
        )



@pytest.mark.timeout(60)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_binary_incoming(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Test plugin receiving binary data with all byte values."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()
        test_data = bytes(range(256)) * 1024  # 256 KB
        req_id = make_req_id()
        send_request(writer, req_id, TEST_CAPS["verify_binary"], test_data)
        output, frames = read_response(reader, timeout_frames=500)

        if isinstance(output, bytes):
            output_str = output.decode("utf-8", errors="replace")
        else:
            output_str = str(output)

        assert output_str == "ok", (
            f"[{router_lang}/{host_lang}/{plugin_lang}] binary verification failed: {output_str}"
        )



@pytest.mark.timeout(90)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_hash_incoming(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Test plugin hashing 5MB incoming payload."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()
        size = 5 * 1024 * 1024
        test_data = bytes([i % 256 for i in range(size)])
        req_id = make_req_id()
        send_request(writer, req_id, TEST_CAPS["hash_incoming"], test_data)
        output, frames = read_response(reader, timeout_frames=2000)

        if isinstance(output, bytes):
            output_str = output.decode("utf-8", errors="replace")
        else:
            output_str = str(output)

        expected_hash = hashlib.sha256(test_data).hexdigest()
        assert output_str == expected_hash, (
            f"[{router_lang}/{host_lang}/{plugin_lang}] hash mismatch: {output_str} vs {expected_hash}"
        )



@pytest.mark.timeout(120)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_multiple_incoming(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Test multiple large incoming requests in sequence (3 x 1MB)."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()
        size = 1024 * 1024
        for i in range(3):
            test_data = bytes([i] * size)
            req_id = make_req_id()
            send_request(writer, req_id, TEST_CAPS["process_large"], test_data)
            output, frames = read_response(reader, timeout_frames=500)

            if isinstance(output, bytes):
                result = json.loads(output)
            else:
                result = output

            assert result["size"] == size, (
                f"[{router_lang}/{host_lang}/{plugin_lang}] request {i}: size mismatch"
            )

            expected_checksum = hashlib.sha256(test_data).hexdigest()
            assert result["checksum"] == expected_checksum, (
                f"[{router_lang}/{host_lang}/{plugin_lang}] request {i}: checksum mismatch"
            )



@pytest.mark.timeout(30)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_zero_length_incoming(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Test plugin receiving empty payload."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()
        req_id = make_req_id()
        send_request(writer, req_id, TEST_CAPS["process_large"], b"")
        output, frames = read_response(reader)

        if isinstance(output, bytes):
            result = json.loads(output)
        else:
            result = output

        assert result["size"] == 0, (
            f"[{router_lang}/{host_lang}/{plugin_lang}] size should be 0, got {result['size']}"
        )

        expected_checksum = hashlib.sha256(b"").hexdigest()
        assert result["checksum"] == expected_checksum, (
            f"[{router_lang}/{host_lang}/{plugin_lang}] empty checksum mismatch"
        )

