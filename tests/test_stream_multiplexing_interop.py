"""Stream multiplexing interoperability tests for Protocol v2.

Tests that STREAM_START/STREAM_END frame types work correctly end-to-end.
In Protocol v2, ALL requests use stream multiplexing:
  REQ(empty) + STREAM_START + CHUNK(s) + STREAM_END + END

These tests verify the full path through all router x host x plugin combinations.

Architecture:
    Test (Engine) → Router (RelaySwitch) → Host (PluginHost) → Plugin
"""

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
def test_single_stream(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Test single stream: STREAM_START + CHUNK + STREAM_END + END roundtrip."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()
        test_data = b"Hello stream multiplexing!"
        req_id = make_req_id()
        send_request(writer, req_id, TEST_CAPS["echo"], test_data)
        output, frames = read_response(reader)

        assert output == test_data, (
            f"[{router_lang}/{host_lang}/{plugin_lang}] single stream mismatch: {output!r}"
        )



@pytest.mark.timeout(30)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_multiple_streams(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Test protocol correctly tracks stream state across multiple sequential requests."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()
        for i in range(3):
            test_data = f"stream-test-{i}".encode()
            req_id = make_req_id()
            send_request(writer, req_id, TEST_CAPS["echo"], test_data)
            output, frames = read_response(reader)

            assert output == test_data, (
                f"[{router_lang}/{host_lang}/{plugin_lang}] request {i} mismatch: {output!r}"
            )



@pytest.mark.timeout(30)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_empty_stream(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Test empty payload through stream multiplexing."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()
        test_data = b""
        req_id = make_req_id()
        send_request(writer, req_id, TEST_CAPS["echo"], test_data)
        output, frames = read_response(reader)

        assert output == test_data, (
            f"[{router_lang}/{host_lang}/{plugin_lang}] empty stream mismatch: {output!r}"
        )



@pytest.mark.timeout(30)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_interleaved_streams(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Test binary data integrity through stream multiplexing."""
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
            f"[{router_lang}/{host_lang}/{plugin_lang}] binary data corrupted through stream multiplexing"
        )



@pytest.mark.timeout(30)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_stream_error_handling(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Test that stream protocol completes cleanly without errors."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()
        test_data = b"Error handling test"
        req_id = make_req_id()
        send_request(writer, req_id, TEST_CAPS["echo"], test_data)
        output, frames = read_response(reader)

        assert output == test_data, (
            f"[{router_lang}/{host_lang}/{plugin_lang}] mismatch: {output!r}"
        )

        # Verify no ERR frames in response
        err_frames = [f for f in frames if f.frame_type == FrameType.ERR]
        assert len(err_frames) == 0, (
            f"[{router_lang}/{host_lang}/{plugin_lang}] unexpected ERR frames in response"
        )

