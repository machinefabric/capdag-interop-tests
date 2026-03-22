"""Declarative test topology builder for multi-host scenarios.

Provides a fluent builder API for defining test topologies with multiple
relay hosts connected to a single router. Simplifies test setup and cleanup
with context manager support.

Example:
    topology = (TestTopology()
        .router(rust_router_bin)
        .host("master-a", rust_host_bin, [plugin1, plugin2])
        .host("master-b", swift_host_bin, [plugin3])
        .build())

    with topology:
        reader, writer = topology.start()
        # ... test code using standard frame helpers ...
        # automatic cleanup on exit
"""

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from capdag_interop.framework.router_process import RouterProcess, HostConfig


@dataclass
class HostSpec:
    """Specification for a relay host in the topology.

    Attributes:
        name: Human-readable identifier for this host (e.g., "preferred-master")
        binary_path: Path to relay host binary
        plugin_paths: List of paths to plugin binaries for this host
    """
    name: str
    binary_path: str
    plugin_paths: List[str]


class TestTopology:
    """Declarative builder for multi-host test topologies.

    Provides a fluent interface for defining test setups with multiple relay
    hosts connected to a single router. Handles process lifecycle management
    and automatic cleanup via context manager.

    Design Principles:
    - Fail hard on invalid configurations (no silent defaults)
    - No backward compatibility - new regime only
    - Production-quality error messages
    - Context manager ensures cleanup

    Example:
        topology = (TestTopology()
            .router(rust_router_bin)
            .host("master-a", rust_host_bin, [plugin1, plugin2])
            .host("master-b", swift_host_bin, [plugin3])
            .build())

        with topology:
            reader, writer = topology.start()
            send_request(writer, req_id, cap_urn, payload)
            output, frames = read_response(reader)
    """

    def __init__(self):
        """Initialize empty topology builder."""
        self._router_binary: Optional[str] = None
        self._hosts: List[HostSpec] = []
        self._router_proc: Optional[RouterProcess] = None

    def router(self, binary_path: str) -> 'TestTopology':
        """Specify router binary.

        Args:
            binary_path: Path to router binary (e.g., capdag-interop-router-rust)

        Returns:
            self for method chaining

        Raises:
            ValueError: If router already specified (no duplicate calls allowed)
        """
        if self._router_binary is not None:
            raise ValueError(
                f"Router already specified as {self._router_binary}. "
                "Cannot specify router twice."
            )

        self._router_binary = str(binary_path)
        return self

    def host(self, name: str, binary_path: str, plugins: List[str]) -> 'TestTopology':
        """Add a relay host to the topology.

        Args:
            name: Human-readable identifier for this host (e.g., "preferred-master")
            binary_path: Path to relay host binary
            plugins: List of paths to plugin binaries for this host

        Returns:
            self for method chaining

        Raises:
            ValueError: If host name is duplicate or plugins list is empty
        """
        if not name:
            raise ValueError("Host name cannot be empty")

        if any(h.name == name for h in self._hosts):
            raise ValueError(
                f"Host name '{name}' already used. Host names must be unique."
            )

        if not plugins:
            raise ValueError(
                f"Host '{name}' has no plugins. Each host must have at least one plugin."
            )

        self._hosts.append(HostSpec(name, str(binary_path), [str(p) for p in plugins]))
        return self

    def build(self) -> 'TestTopology':
        """Validate and finalize the topology.

        Returns:
            self for use as context manager

        Raises:
            ValueError: If configuration is invalid (no router, no hosts, etc.)
        """
        if self._router_binary is None:
            raise ValueError(
                "Router binary not specified. Call .router(binary_path) before .build()"
            )

        if not self._hosts:
            raise ValueError(
                "No hosts specified. Call .host(name, binary, plugins) at least once before .build()"
            )

        return self

    def start(self):
        """Spawn all processes and return (reader, writer) for engine communication.

        Creates HostConfig instances from HostSpecs, spawns all relay hosts,
        starts router with connections to all hosts, waits for aggregated
        capabilities.

        Returns:
            (reader, writer) tuple for communicating with router via CBOR frames

        Raises:
            RuntimeError: If already started or if any process fails to start
        """
        if self._router_proc is not None:
            raise RuntimeError(
                "Topology already started. Cannot call start() multiple times."
            )

        # Convert HostSpecs to HostConfigs
        host_configs = [
            HostConfig(h.binary_path, h.plugin_paths)
            for h in self._hosts
        ]

        # Create and start router process with all hosts
        self._router_proc = RouterProcess(self._router_binary, host_configs)
        return self._router_proc.start()

    def stop(self, timeout: float = 5.0):
        """Stop all processes (router and all relay hosts).

        Args:
            timeout: Maximum time in seconds to wait for each process to exit gracefully
        """
        if self._router_proc is not None:
            self._router_proc.stop(timeout)
            self._router_proc = None

    def __enter__(self):
        """Context manager entry - returns self for use in 'with' statement.

        Example:
            with topology:
                reader, writer = topology.start()
                # ... test code ...
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - automatic cleanup.

        Ensures processes are stopped even if test code raises exception.
        """
        self.stop()
        return False  # Don't suppress exceptions
