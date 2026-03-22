"""Relay interop tests.

Tests the RelaySlave/RelayMaster protocol across language combinations.
Uses 3-tier architecture: Test → Router → Host (RelaySlave + PluginHost) → Plugin.

Architecture:
    Test (Engine) → Router (RelaySwitch) → Host (RelaySlave → PluginHost → plugin)
"""

import json
import pytest

from capdag.bifaci.frame import Frame, FrameType, Limits, MessageId
from capdag_interop import TEST_CAPS
from capdag_interop.framework.frame_test_helper import (
    make_req_id,
    send_request,
    send_simple_request,
    read_response,
    decode_cbor_response,
)
from capdag_interop.framework.test_topology import TestTopology

# E-commerce semantic cap URNs matching test plugin
ECHO_CAP = 'cap:in=media:;out=media:'
BINARY_ECHO_CAP = 'cap:in="media:product-image";op=binary_echo;out="media:product-image"'

SUPPORTED_ROUTER_LANGS = ["rust"]
SUPPORTED_HOST_LANGS = ["rust"]
SUPPORTED_PLUGIN_LANGS = ["rust", "go", "python", "swift"]


# ============================================================
# Test: Relay sends initial RelayNotify with manifest + limits
# ============================================================

@pytest.mark.timeout(15)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_relay_initial_notify(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Router receives RelayNotify from host and makes capabilities available.

    The RouterProcess.start() already waits for the RelayNotify with a non-empty
    capability list before returning. If we reach here, the relay handshake worked.
    Verify by sending a request that exercises the advertised capabilities.
    """
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()
        # If start() returned, capabilities were received. Verify they work.
        req_id = make_req_id()
        send_request(writer, req_id, ECHO_CAP, b"relay-notify-test")
        output, frames = read_response(reader)

        assert output == b"relay-notify-test", (
            f"[{router_lang}/{host_lang}/{plugin_lang}] echo after relay notify failed: {output!r}"
        )



# ============================================================
# Test: Request passes through relay transparently
# ============================================================

@pytest.mark.timeout(30)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_relay_request_passthrough(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """REQ through relay reaches plugin, response comes back through relay."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()
        req_id = make_req_id()
        send_request(writer, req_id, ECHO_CAP, b"relay-echo-test")
        output, frames = read_response(reader)

        assert output == b"relay-echo-test", (
            f"[{router_lang}/{host_lang}/{plugin_lang}] echo mismatch through relay: {output!r}"
        )



# ============================================================
# Test: RelayState from engine is forwarded through router to slave
# ============================================================

@pytest.mark.timeout(15)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_relay_state_delivery(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """RelayState sent by engine passes through router to slave. Plugin never sees it.

    We verify this indirectly: if the relay state reached the plugin as a regular
    frame, the plugin runtime would send an ERR. Since the echo still works,
    the relay correctly intercepted the RelayState.
    """
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()
        # Send RelayState through router to slave
        from capdag.bifaci.io import FrameWriter
        state_frame = Frame.relay_state(b'{"memory_mb": 1024}')
        writer.write(state_frame)

        # Now send a regular request — if RelayState leaked to the plugin,
        # the plugin runtime would error out and this request would fail
        req_id = make_req_id()
        send_request(writer, req_id, ECHO_CAP, b"after-relay-state")
        output, frames = read_response(reader)

        assert output == b"after-relay-state", (
            f"[{router_lang}/{host_lang}/{plugin_lang}] echo after RelayState failed: {output!r}"
        )



# ============================================================
# Test: Unknown cap returns ERR through relay
# ============================================================

@pytest.mark.timeout(15)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
def test_relay_unknown_cap_returns_err(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang):
    """Request for unknown cap through relay returns ERR frame."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries["rust"]])
        .build())

    with topology:
        reader, writer = topology.start()

        req_id = make_req_id()
        send_request(writer, req_id, "cap:op=nonexistent-relay-xyz", b"", media_urn="media:void")
        _, frames = read_response(reader)

        err_frames = [f for f in frames if f.frame_type == FrameType.ERR]
        assert len(err_frames) > 0, (
            f"[{router_lang}/{host_lang}] must receive ERR for unknown cap through relay, got: "
            f"{[f.frame_type for f in frames]}"
        )



# ============================================================
# Test: Mixed traffic (RelayState + requests interleaved)
# ============================================================

@pytest.mark.timeout(30)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_relay_mixed_traffic(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """RelayState frames interleaved with requests work correctly."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()
        # Interleave: RelayState, request, RelayState, request
        state_frame1 = Frame.relay_state(b'{"step": 1}')
        writer.write(state_frame1)

        req_id1 = make_req_id()
        send_request(writer, req_id1, ECHO_CAP, b"mixed-1")
        output1, _ = read_response(reader)

        state_frame2 = Frame.relay_state(b'{"step": 2}')
        writer.write(state_frame2)

        req_id2 = make_req_id()
        send_request(writer, req_id2, BINARY_ECHO_CAP, bytes(range(64)))
        output2, _ = read_response(reader)

        assert output1 == b"mixed-1", (
            f"[{router_lang}/{host_lang}/{plugin_lang}] first request after state: {output1!r}"
        )
        assert output2 == bytes(range(64)), (
            f"[{router_lang}/{host_lang}/{plugin_lang}] second request after state: {output2!r}"
        )

