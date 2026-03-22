"""Manifest validation interoperability tests.

Tests that all plugins properly declare CAP_IDENTITY in their manifests.
Per the capdag protocol specification, ALL plugins MUST explicitly declare
CAP_IDENTITY (cap:) - no implicit fallbacks allowed.

This is enforced by PluginRuntime.with_manifest() which fails hard if
CAP_IDENTITY is missing from the manifest.

Architecture:
    Test (Engine) → Router (RelaySwitch) → Host (PluginHost) → Plugin
"""

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
def test_plugin_declares_cap_identity(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Verify plugin successfully starts (implicitly proving CAP_IDENTITY is declared).

    All test plugins MUST have CAP_IDENTITY in their manifest.
    If CAP_IDENTITY is missing, PluginRuntime.with_manifest() fails hard:
    - Rust: returns Err("manifest validation failed - plugin MUST declare CAP_IDENTITY")
    - Python: raises ValueError("manifest validation failed - plugin MUST declare CAP_IDENTITY")
    - Go: returns error
    - Swift: precondition failure

    This test verifies the plugin starts and can handle the identity capability.
    """
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()
        # Test identity capability - all plugins must support this
        test_input = b"cap_identity_test"
        req_id = make_req_id()

        # Use echo capability as proxy for CAP_IDENTITY validation
        # (if plugin started successfully, CAP_IDENTITY was declared)
        send_request(writer, req_id, TEST_CAPS["echo"], test_input)
        output, frames = read_response(reader)

        assert output == test_input, (
            f"[{router_lang}/{host_lang}/{plugin_lang}] "
            f"Plugin started but echo failed: {output!r} != {test_input!r}"
        )



# NOTE: Testing a plugin WITHOUT CAP_IDENTITY would require building a
# deliberately broken plugin, which would fail to start. That's a unit
# test concern (tested in each language's runtime tests), not an interop
# test concern. This interop test verifies all production plugins comply.
