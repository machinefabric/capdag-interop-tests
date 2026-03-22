"""Basic interoperability tests.

Tests echo, double, binary_echo, and get_manifest capabilities across all
router x host x plugin language combinations using raw CBOR frames.

Architecture:
    Test (Engine) → Router (RelaySwitch) → Host (PluginHost) → Plugin

Updated to use TestTopology declarative API (no backward compatibility).
"""

import json
import pytest

from capdag_interop import TEST_CAPS
from capdag_interop.framework.frame_test_helper import (
    make_req_id,
    send_request,
    read_response,
    decode_cbor_response,
    FrameType,
)
from capdag_interop.framework.test_topology import TestTopology

SUPPORTED_ROUTER_LANGS = ["rust", "swift"]
SUPPORTED_HOST_LANGS = ["rust", "swift"]
SUPPORTED_PLUGIN_LANGS = ["rust", "go", "python", "swift"]


@pytest.mark.timeout(30)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
def test_echo(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Test echo capability: send bytes, receive identical bytes back."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()

        test_input = b"Hello, World!"
        req_id = make_req_id()
        send_request(writer, req_id, TEST_CAPS["echo"], test_input)
        output, frames = read_response(reader)

        assert output == test_input, (
            f"[{router_lang}/{host_lang}/{plugin_lang}] echo mismatch: expected {test_input!r}, got {output!r}"
        )


@pytest.mark.timeout(30)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
def test_double(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Test double capability: send number, receive doubled result."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()

        test_value = 42
        input_json = json.dumps({"value": test_value}).encode()
        req_id = make_req_id()
        send_request(writer, req_id, TEST_CAPS["double"], input_json, media_urn="media:order-value;json;textable;record")
        output, frames = read_response(reader)

        # Plugin can return integer directly (CBOR) or JSON bytes
        if isinstance(output, int):
            result = output
        elif isinstance(output, bytes):
            result = json.loads(output)
        else:
            result = output

        expected = test_value * 2
        assert result == expected, (
            f"[{router_lang}/{host_lang}/{plugin_lang}] double mismatch: expected {expected}, got {result}"
        )


@pytest.mark.timeout(30)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
def test_binary_echo(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Test binary echo: send all 256 byte values, receive identical data back."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()

        test_data = bytes(range(256))
        req_id = make_req_id()
        send_request(writer, req_id, TEST_CAPS["binary_echo"], test_data)
        output, frames = read_response(reader)

        assert output == test_data, (
            f"[{router_lang}/{host_lang}/{plugin_lang}] binary echo mismatch (len: {len(output)} vs {len(test_data)})"
        )


@pytest.mark.timeout(30)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
def test_get_manifest(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Test manifest retrieval via get_manifest cap."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()
        req_id = make_req_id()
        send_request(writer, req_id, TEST_CAPS["get_manifest"], b"", media_urn="media:void")

        # DEBUG: Manually read raw frames to trace what's on the pipe
        import sys, struct, cbor2
        from capdag.bifaci.io import decode_frame as df2
        raw_stream = reader.inner_mut()
        debug_frames = []
        for frame_num in range(20):
            raw_len = raw_stream.read(4)
            if len(raw_len) < 4:
                print(f"[RAW_DEBUG] frame#{frame_num}: EOF (got {len(raw_len)} bytes)", file=sys.stderr)
                break
            frame_len = struct.unpack('>I', raw_len)[0]
            raw_data = raw_stream.read(frame_len)
            if len(raw_data) < frame_len:
                print(f"[RAW_DEBUG] frame#{frame_num}: TRUNCATED (expected {frame_len}, got {len(raw_data)})", file=sys.stderr)
                break
            try:
                decoded_map = cbor2.loads(raw_data)
                ft = decoded_map.get(1, '?')  # key 1 = FRAME_TYPE
                has_payload = 6 in decoded_map and decoded_map[6] is not None  # key 6 = PAYLOAD
                payload_len = len(decoded_map[6]) if has_payload else 0
                print(f"[RAW_DEBUG] frame#{frame_num}: frame_type={ft} cbor_len={frame_len} payload_len={payload_len} keys={sorted(decoded_map.keys())}", file=sys.stderr)
            except Exception as e:
                print(f"[RAW_DEBUG] frame#{frame_num}: CBOR decode error: {e}, raw_len={frame_len}, first_20={raw_data[:20].hex()}", file=sys.stderr)

            frame = df2(raw_data)
            debug_frames.append(frame)
            if frame.frame_type == FrameType.END or frame.frame_type == FrameType.ERR:
                break

        # Now use the debug_frames as if read_response had read them
        chunks = []
        for frame in debug_frames:
            if frame.frame_type == FrameType.CHUNK and frame.payload:
                decoded = cbor2.loads(frame.payload)
                chunks.append(decoded)
        if not chunks:
            output = b''
        elif len(chunks) == 1:
            output = chunks[0]
        else:
            first = chunks[0]
            if isinstance(first, bytes):
                output = b''.join(c for c in chunks if isinstance(c, bytes))
            else:
                output = chunks
        frames = debug_frames

        # DEBUG: Print what we received
        import sys
        print(f"[DEBUG] output type={type(output).__name__} len={len(output) if hasattr(output, '__len__') else 'N/A'} repr={repr(output)[:200]}", file=sys.stderr)
        for i, f in enumerate(frames):
            print(f"[DEBUG] frame[{i}] type={f.frame_type} id={f.id} payload_len={len(f.payload) if f.payload else 0}", file=sys.stderr)

        # Parse the manifest JSON
        manifest = json.loads(output)

        assert "name" in manifest, f"[{router_lang}/{host_lang}/{plugin_lang}] manifest missing 'name'"
        assert "version" in manifest, f"[{router_lang}/{host_lang}/{plugin_lang}] manifest missing 'version'"
        assert "caps" in manifest, f"[{router_lang}/{host_lang}/{plugin_lang}] manifest missing 'caps'"
        assert len(manifest["caps"]) >= 10, (
            f"[{router_lang}/{host_lang}/{plugin_lang}] manifest has {len(manifest['caps'])} caps, expected >= 10"
        )
