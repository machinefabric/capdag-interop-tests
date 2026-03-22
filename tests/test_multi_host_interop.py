"""Multi-plugin host interop tests.

Tests the PluginHost routing with multiple plugins across language combinations.
Each relay host binary manages multiple plugin binaries and routes requests by cap URN.

Architecture:
    Test (Engine) → Router (RelaySwitch) → Host (PluginHost) → plugin1
                                                               → plugin2

Updated to use TestTopology declarative API (no backward compatibility).
"""

import json
import pytest

from capdag.bifaci.frame import Frame, FrameType, MessageId
from capdag_interop import TEST_CAPS
from capdag_interop.framework.frame_test_helper import (
    make_req_id,
    send_request,
    send_simple_request,
    read_response,
    decode_cbor_response,
)
from capdag_interop.framework.test_topology import TestTopology

# E-commerce semantic cap URNs matching the test plugin's registered capabilities
ECHO_CAP = 'cap:in=media:;out=media:'
BINARY_ECHO_CAP = 'cap:in="media:product-image";op=binary_echo;out="media:product-image"'
DOUBLE_CAP = 'cap:in="media:order-value;json;textable;record";op=double;out="media:loyalty-points;integer;textable;numeric"'

SUPPORTED_ROUTER_LANGS = ["rust"]
SUPPORTED_HOST_LANGS = ["rust"]
SUPPORTED_PLUGIN_LANGS = ["rust", "go", "python", "swift"]


# ============================================================
# Test: Two plugins with distinct caps route independently
# ============================================================

@pytest.mark.timeout(30)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_two_plugin_routing(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Route requests to two instances of the same plugin, verify both respond."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang], plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()

        # Send echo request
        req_id = make_req_id()
        send_request(writer, req_id, ECHO_CAP, b"hello-routing")
        output, frames = read_response(reader)

        assert output == b"hello-routing", (
            f"[{router_lang}/{host_lang}/{plugin_lang}] echo mismatch: {output!r}"
        )

        # Send binary_echo request (different cap, same plugin)
        req_id2 = make_req_id()
        send_request(writer, req_id2, BINARY_ECHO_CAP, bytes(range(256)))
        output2, frames2 = read_response(reader)

        assert output2 == bytes(range(256)), (
            f"[{router_lang}/{host_lang}/{plugin_lang}] binary_echo data mismatch"
        )


# ============================================================
# Test: Request to unknown cap returns ERR
# ============================================================

@pytest.mark.timeout(15)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
def test_unknown_cap_returns_err(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang):
    """Request for unknown cap must return ERR frame."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries["rust"]])
        .build())

    with topology:
        reader, writer = topology.start()

        req_id = make_req_id()
        send_request(writer, req_id, "cap:op=nonexistent-cap-xyz", b"", media_urn="media:void")
        _, frames = read_response(reader)

        err_frames = [f for f in frames if f.frame_type == FrameType.ERR]
        assert len(err_frames) > 0, (
            f"[{router_lang}/{host_lang}] must receive ERR for unknown cap, got: "
            f"{[f.frame_type for f in frames]}"
        )


# ============================================================
# Test: Concurrent requests to same plugin
# ============================================================

@pytest.mark.timeout(30)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_concurrent_requests(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Two requests sent before reading any response both complete correctly."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()

        payload1 = b"first-request"
        payload2 = b"second-request"

        # Send both requests before reading
        req_id1 = make_req_id()
        send_request(writer, req_id1, ECHO_CAP, payload1)
        req_id2 = make_req_id()
        send_request(writer, req_id2, ECHO_CAP, payload2)

        # Read both responses (they may arrive interleaved)
        import cbor2
        responses = {}  # req_id_str → list of decoded chunks
        ends = 0
        for _ in range(100):
            frame = reader.read()
            if frame is None:
                break
            id_str = frame.id.to_uuid_string() if frame.id else None
            if frame.frame_type == FrameType.CHUNK and frame.payload:
                decoded_chunk = cbor2.loads(frame.payload)
                responses.setdefault(id_str, []).append(decoded_chunk)
            if frame.frame_type == FrameType.END:
                ends += 1
                if ends >= 2:
                    break

        id1_str = req_id1.to_uuid_string()
        id2_str = req_id2.to_uuid_string()

        chunks1 = responses.get(id1_str, [])
        chunks2 = responses.get(id2_str, [])
        decoded1 = chunks1[0] if len(chunks1) == 1 else b''.join(c for c in chunks1 if isinstance(c, bytes))
        decoded2 = chunks2[0] if len(chunks2) == 1 else b''.join(c for c in chunks2 if isinstance(c, bytes))

        assert decoded1 == payload1, (
            f"[{router_lang}/{host_lang}/{plugin_lang}] first request mismatch: {decoded1!r}"
        )
        assert decoded2 == payload2, (
            f"[{router_lang}/{host_lang}/{plugin_lang}] second request mismatch: {decoded2!r}"
        )
