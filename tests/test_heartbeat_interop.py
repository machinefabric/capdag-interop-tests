"""Heartbeat interoperability tests.

Tests heartbeat handling during operations: basic heartbeat, long-running
operations, and status updates. Heartbeats are handled internally by the
host (never forwarded to the test).

Architecture:
    Test (Engine) → Router (RelaySwitch) → Host (PluginHost) → Plugin
"""

import json
import pytest

from capdag_interop import TEST_CAPS
from capdag_interop.framework.frame_test_helper import (
    make_req_id,
    send_request,
    read_response,
)
from capdag_interop.framework.test_topology import TestTopology

SUPPORTED_ROUTER_LANGS = ["rust"]
SUPPORTED_HOST_LANGS = ["rust"]
SUPPORTED_PLUGIN_LANGS = ["rust", "go", "python", "swift"]


@pytest.mark.timeout(30)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_basic_heartbeat(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Test heartbeat during 500ms operation: plugin sends heartbeats, host handles locally."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()
        duration_ms = 500
        input_json = json.dumps({"value": duration_ms}).encode()
        req_id = make_req_id()
        send_request(writer, req_id, TEST_CAPS["heartbeat_stress"], input_json, media_urn="media:monitoring-duration-ms;json;textable;record")
        output, frames = read_response(reader)

        if isinstance(output, bytes):
            output_str = output.decode("utf-8", errors="replace")
        else:
            output_str = str(output)

        expected = f"stressed-{duration_ms}ms"
        assert output_str == expected, (
            f"[{router_lang}/{host_lang}/{plugin_lang}] expected {expected!r}, got {output_str!r}"
        )



@pytest.mark.timeout(30)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_long_operation_heartbeat(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Test heartbeat during 2-second operation: verifies no timeout/deadlock."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()
        duration_ms = 2000
        input_json = json.dumps({"value": duration_ms}).encode()
        req_id = make_req_id()
        send_request(writer, req_id, TEST_CAPS["heartbeat_stress"], input_json, media_urn="media:monitoring-duration-ms;json;textable;record")
        output, frames = read_response(reader)

        if isinstance(output, bytes):
            output_str = output.decode("utf-8", errors="replace")
        else:
            output_str = str(output)

        expected = f"stressed-{duration_ms}ms"
        assert output_str == expected, (
            f"[{router_lang}/{host_lang}/{plugin_lang}] expected {expected!r}, got {output_str!r}"
        )



@pytest.mark.timeout(30)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_status_updates(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Test status updates during processing: plugin sends LOG frames, verify response."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()
        steps = 5
        input_json = json.dumps({"value": steps}).encode()
        req_id = make_req_id()
        send_request(writer, req_id, TEST_CAPS["with_status"], input_json, media_urn="media:fulfillment-steps;json;textable;record")
        output, frames = read_response(reader)

        if isinstance(output, bytes):
            output_str = output.decode("utf-8", errors="replace")
        else:
            output_str = str(output)

        assert output_str == "completed", (
            f"[{router_lang}/{host_lang}/{plugin_lang}] expected 'completed', got {output_str!r}"
        )

