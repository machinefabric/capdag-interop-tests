"""Router process helper for 3-tier interoperability tests.

Router = RelaySwitch + RelayMaster in a subprocess.
Communicates with test orchestration via stdin/stdout.
Connects to independent relay host processes via Unix sockets.

Architecture:
  Test (Engine) → Router (subprocess) ←socket→ Host1 (subprocess) → Plugin(s)
                                       ←socket→ Host2 (subprocess) → Plugin(s)
                                       ←socket→ Host3 (subprocess) → Plugin(s)

Router and Hosts are independent siblings (not parent-child).
Relay connections are non-fatal - if broken, router continues running.
Router aggregates capabilities from all connected hosts.
"""

import os
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from capdag_interop.framework.frame_test_helper import FrameReader, FrameWriter


@dataclass
class HostConfig:
    """Configuration for a relay host in the test topology.

    Each relay host runs as an independent process listening on a Unix socket.
    The router connects to all configured hosts and aggregates their capabilities.
    """
    binary_path: str
    plugin_paths: List[str]
    socket_path: Optional[str] = None  # Assigned during RouterProcess.start()


class RouterProcess:
    """Wrapper for router subprocess (RelaySwitch + RelayMaster).

    The router connects to multiple independent relay host processes via Unix sockets.
    Test communicates with router via CBOR frames on stdin/stdout.

    Router and relay hosts run as independent processes (siblings, not parent-child).
    This decouples their lifecycles - relay connection loss is non-fatal.

    The router aggregates capabilities from all connected hosts and routes requests
    to the appropriate host based on capability URN matching. When multiple hosts
    advertise the same capability, the first host (preferred master) wins.
    """

    def __init__(self, router_binary_path: str, hosts: List[HostConfig]):
        """Create a router process with multiple relay hosts.

        Args:
            router_binary_path: Path to router binary (e.g., capdag-interop-router-rust)
            hosts: List of HostConfig defining each relay host and its plugins

        Raises:
            ValueError: If no hosts are provided
        """
        if not hosts:
            raise ValueError("At least one host must be provided")

        self.router_path = Path(router_binary_path)
        self.host_configs = hosts
        self.router_proc: Optional[subprocess.Popen] = None
        self.host_procs: List[subprocess.Popen] = []
        self.socket_paths: List[str] = []
        self.reader: Optional[FrameReader] = None
        self.writer: Optional[FrameWriter] = None

    def start(self):
        """Start N relay hosts and router subprocess.

        Spawns each configured relay host on a unique Unix socket, then starts
        the router with --connect arguments for all sockets. Waits for router
        to receive aggregated capabilities from all hosts before returning.

        Returns:
            (reader, writer) tuple for communicating with router via CBOR frames

        Raises:
            RuntimeError: If any host fails to create socket or router fails to start
        """
        temp_dir = tempfile.gettempdir()

        # Step 1: Spawn N relay hosts on N unique sockets
        for i, host_config in enumerate(self.host_configs):
            socket_path = os.path.join(temp_dir, f"capdag_relay_test_{os.getpid()}_{i}.sock")

            # Remove existing socket if present (fail hard if removal fails)
            if os.path.exists(socket_path):
                try:
                    os.unlink(socket_path)
                except OSError as e:
                    raise RuntimeError(f"Failed to remove existing socket {socket_path}: {e}")

            # Build host command: --listen <socket> --spawn <plugin1> --spawn <plugin2> ... --relay
            plugin_args = []
            for plugin_path in host_config.plugin_paths:
                plugin_args.extend(["--spawn", str(plugin_path)])

            host_cmd = [
                str(host_config.binary_path),
                "--listen", socket_path,
                *plugin_args,
                "--relay",
            ]

            print(f"[RouterProcess] Starting relay host {i}: {' '.join(host_cmd)}", file=sys.stderr)

            host_proc = subprocess.Popen(
                host_cmd,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=sys.stderr,  # Forward stderr for debugging
            )

            self.host_procs.append(host_proc)
            self.socket_paths.append(socket_path)
            host_config.socket_path = socket_path

        # Wait for ALL sockets to be created (all hosts ready)
        max_wait = 5.0
        start_time = time.time()
        for i, socket_path in enumerate(self.socket_paths):
            while not os.path.exists(socket_path):
                elapsed = time.time() - start_time
                if elapsed > max_wait:
                    self.stop()
                    raise RuntimeError(
                        f"Relay host {i} failed to create socket {socket_path} after {elapsed:.1f}s"
                    )

                # Check if host process died
                if self.host_procs[i].poll() is not None:
                    self.stop()
                    raise RuntimeError(
                        f"Relay host {i} exited prematurely (exit code: {self.host_procs[i].returncode})"
                    )

                time.sleep(0.01)

            print(f"[RouterProcess] Relay host {i} listening on {socket_path}", file=sys.stderr)

        # Step 2: Start router with N --connect arguments
        # Router command: <router-binary> --connect <socket1> --connect <socket2> ...
        router_cmd = [str(self.router_path)]
        for socket_path in self.socket_paths:
            router_cmd.extend(["--connect", socket_path])

        print(f"[RouterProcess] Starting router: {' '.join(router_cmd)}", file=sys.stderr)

        self.router_proc = subprocess.Popen(
            router_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=sys.stderr,  # Forward stderr for debugging
        )

        # Create frame reader/writer for stdin/stdout
        self.reader = FrameReader(self.router_proc.stdout)
        self.writer = FrameWriter(self.router_proc.stdin)

        # Step 3: Wait for router's aggregated RelayNotify
        # Router sends ONE RelayNotify with capabilities from ALL hosts
        print(f"[RouterProcess] Waiting for router's aggregated capabilities from {len(self.host_configs)} hosts...", file=sys.stderr)

        caps_ready = False
        while not caps_ready:
            frame = self.reader.read()
            if frame is None:
                # Router closed before sending capabilities - this is a hard failure
                self.stop()
                raise RuntimeError("Router closed before sending aggregated capabilities")

            if frame.frame_type == 10:  # RelayNotify
                # Check if this RelayNotify has capabilities in the manifest
                manifest_bytes = frame.relay_notify_manifest()
                if manifest_bytes and len(manifest_bytes) > 2:  # More than just "[]"
                    # Parse to verify it's a non-empty capability array
                    import json
                    try:
                        caps = json.loads(manifest_bytes)
                        if isinstance(caps, list) and len(caps) > 0:
                            print(
                                f"[RouterProcess] Router ready with {len(caps)} aggregate capabilities "
                                f"from {len(self.host_configs)} hosts",
                                file=sys.stderr
                            )
                            caps_ready = True
                        else:
                            print(f"[RouterProcess] Got empty RelayNotify, waiting for capabilities...", file=sys.stderr)
                    except json.JSONDecodeError as e:
                        print(f"[RouterProcess] Invalid RelayNotify manifest: {e}, ignoring", file=sys.stderr)
                else:
                    print(f"[RouterProcess] Got empty RelayNotify, waiting for capabilities...", file=sys.stderr)

        return self.reader, self.writer

    def stop(self, timeout: float = 5.0):
        """Stop router and all relay host processes.

        Stops router first (which closes all host connections), then stops
        all relay host processes. Cleans up all Unix sockets.

        Args:
            timeout: Maximum time in seconds to wait for each process to exit gracefully
        """
        # Stop router first (closes all host connections)
        if self.router_proc:
            try:
                # Close stdin to signal EOF
                if self.router_proc.stdin:
                    self.router_proc.stdin.close()

                # Wait for graceful shutdown
                self.router_proc.wait(timeout=timeout)
                print(f"[RouterProcess] Router stopped gracefully", file=sys.stderr)
            except subprocess.TimeoutExpired:
                print(f"[RouterProcess] Timeout waiting for router, killing", file=sys.stderr)
                self.router_proc.kill()
                self.router_proc.wait()
            except Exception as e:
                print(f"[RouterProcess] Error stopping router: {e}", file=sys.stderr)
                if self.router_proc.poll() is None:
                    self.router_proc.kill()
                    self.router_proc.wait()

        # Stop all relay hosts (should exit when router closes connections)
        for i, host_proc in enumerate(self.host_procs):
            try:
                host_proc.wait(timeout=timeout)
                print(f"[RouterProcess] Relay host {i} stopped gracefully", file=sys.stderr)
            except subprocess.TimeoutExpired:
                print(f"[RouterProcess] Timeout waiting for relay host {i}, killing", file=sys.stderr)
                host_proc.kill()
                host_proc.wait()
            except Exception as e:
                print(f"[RouterProcess] Error stopping relay host {i}: {e}", file=sys.stderr)
                if host_proc.poll() is None:
                    host_proc.kill()
                    host_proc.wait()

        # Clean up all sockets
        for i, socket_path in enumerate(self.socket_paths):
            if os.path.exists(socket_path):
                try:
                    os.unlink(socket_path)
                    print(f"[RouterProcess] Cleaned up socket {socket_path}", file=sys.stderr)
                except OSError as e:
                    print(f"[RouterProcess] Failed to clean up socket {socket_path}: {e}", file=sys.stderr)
