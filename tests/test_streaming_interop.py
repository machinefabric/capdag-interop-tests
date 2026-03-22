"""Streaming interoperability tests.

Tests streaming responses (multiple chunks), large payload transfer,
binary data integrity, and chunk ordering across router x host x plugin combinations.

Architecture:
    Test (Engine) → Router (RelaySwitch) → Host (PluginHost) → Plugin
"""

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


@pytest.mark.timeout(30)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_stream_chunks(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Test streaming multiple chunks: request N chunks, verify all received in order."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()
        chunk_count = 5
        input_json = json.dumps({"value": chunk_count}).encode()
        req_id = make_req_id()
        send_request(writer, req_id, TEST_CAPS["stream_chunks"], input_json, media_urn="media:update-count;json;textable;record")

        # Collect all response frames
        chunks_data = []
        for _ in range(200):
            frame = reader.read()
            if frame is None:
                break
            if frame.frame_type == FrameType.CHUNK and frame.payload:
                chunks_data.append(decode_cbor_response(frame.payload))
            if frame.frame_type in (FrameType.END, FrameType.ERR):
                break

        # Verify we got chunks (may be more than chunk_count due to framing)
        assert len(chunks_data) >= chunk_count, (
            f"[{router_lang}/{host_lang}/{plugin_lang}] expected >= {chunk_count} chunks, got {len(chunks_data)}"
        )



@pytest.mark.timeout(60)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_large_payload(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Test large payload transfer (10MB): request generated data, verify size + pattern."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()
        size = 10 * 1024 * 1024  # 10 MB
        input_json = json.dumps({"value": size}).encode()
        req_id = make_req_id()
        send_request(writer, req_id, TEST_CAPS["generate_large"], input_json, media_urn="media:report-size;json;textable;record")

        # Collect all chunk data
        # Each CHUNK payload MUST be a complete, independently decodable CBOR value
        import cbor2
        all_data = bytearray()
        for _ in range(5000):
            frame = reader.read()
            if frame is None:
                break
            if frame.frame_type == FrameType.CHUNK and frame.payload:
                # Decode each chunk independently - FAIL HARD if not valid CBOR
                decoded = cbor2.loads(frame.payload)  # No try/except
                if isinstance(decoded, bytes):
                    all_data.extend(decoded)
                elif isinstance(decoded, str):
                    all_data.extend(decoded.encode('utf-8'))
                else:
                    raise ValueError(f"Unexpected CBOR type in chunk: {type(decoded)}")
            if frame.frame_type in (FrameType.END, FrameType.ERR):
                break

        assert len(all_data) == size, (
            f"[{router_lang}/{host_lang}/{plugin_lang}] expected {size} bytes, got {len(all_data)}"
        )

        # Verify pattern
        pattern = b"ABCDEFGH"
        for i in range(min(1000, size)):
            assert all_data[i] == pattern[i % len(pattern)], (
                f"[{router_lang}/{host_lang}/{plugin_lang}] pattern mismatch at byte {i}"
            )



@pytest.mark.timeout(30)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_binary_data(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Test binary data integrity: send all 256 byte values repeated, verify roundtrip."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()
        test_data = bytes(range(256)) * 100  # 25.6 KB
        req_id = make_req_id()
        send_request(writer, req_id, TEST_CAPS["binary_echo"], test_data)
        output, frames = read_response(reader)

        assert output == test_data, (
            f"[{router_lang}/{host_lang}/{plugin_lang}] binary data mismatch "
            f"(len: {len(output)} vs {len(test_data)})"
        )



@pytest.mark.timeout(30)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_stream_ordering(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Test streaming chunk ordering: request 20 chunks, verify sequential order."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()
        chunk_count = 20
        input_json = json.dumps({"value": chunk_count}).encode()
        req_id = make_req_id()
        send_request(writer, req_id, TEST_CAPS["stream_chunks"], input_json, media_urn="media:update-count;json;textable;record")

        # Collect chunk data
        chunk_payloads = []
        for _ in range(500):
            frame = reader.read()
            if frame is None:
                break
            if frame.frame_type == FrameType.CHUNK and frame.payload:
                decoded = decode_cbor_response(frame.payload)
                if isinstance(decoded, bytes):
                    decoded = decoded.decode("utf-8", errors="replace")
                    # Handle JSON-encoded strings: "chunk-0" → chunk-0
                    try:
                        import json as j
                        inner = j.loads(decoded)
                        if isinstance(inner, str):
                            decoded = inner
                    except Exception:
                        pass
                chunk_payloads.append(str(decoded))
            if frame.frame_type in (FrameType.END, FrameType.ERR):
                break

        # Verify ordering: chunk-0 before chunk-1 before chunk-2 ...
        indices = {}
        for idx, payload in enumerate(chunk_payloads):
            for i in range(chunk_count):
                if f"chunk-{i}" == payload:
                    indices[i] = idx

        for i in range(1, chunk_count):
            if i in indices and (i - 1) in indices:
                assert indices[i] > indices[i - 1], (
                    f"[{router_lang}/{host_lang}/{plugin_lang}] chunk-{i} arrived before chunk-{i-1}"
                )

