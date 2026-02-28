#!/opt/homebrew/Caskroom/miniforge/base/bin/python
"""
Interoperability test plugin (Python)

Implements all 13 standard test capabilities for cross-language protocol testing.
"""

import sys
import json
import time
import hashlib
import asyncio
import os
from pathlib import Path

# Add capns-py and tagged-urn-py to path - try multiple strategies
def add_module_paths():
    """Find and add capns-py and tagged-urn-py to sys.path."""
    paths_added = set()

    # Try environment variables first
    if "CAPNS_PY_PATH" in os.environ:
        sys.path.insert(0, os.environ["CAPNS_PY_PATH"])
        paths_added.add("capns")
    if "TAGGED_URN_PY_PATH" in os.environ:
        sys.path.insert(0, os.environ["TAGGED_URN_PY_PATH"])
        paths_added.add("tagged_urn")
    if "OPS_PY_PATH" in os.environ:
        sys.path.insert(0, os.environ["OPS_PY_PATH"])
        paths_added.add("ops")

    if len(paths_added) == 3:
        return  # All found via env vars

    # Try to find modules relative to this file
    current = Path(__file__).resolve().parent
    for _ in range(10):  # Search up to 10 levels
        if "capns" not in paths_added:
            capns_py = current / "capns-py" / "src"
            if capns_py.exists():
                sys.path.insert(0, str(capns_py))
                paths_added.add("capns")

        if "tagged_urn" not in paths_added:
            tagged_urn_py = current / "tagged-urn-py" / "src"
            if tagged_urn_py.exists():
                sys.path.insert(0, str(tagged_urn_py))
                paths_added.add("tagged_urn")

        if "ops" not in paths_added:
            ops_py = current / "ops-py" / "src"
            if ops_py.exists():
                sys.path.insert(0, str(ops_py))
                paths_added.add("ops")

        if len(paths_added) == 3:
            return  # All found

        current = current.parent
        if current == current.parent:  # Reached filesystem root
            break

    # Last resort: assume we're in filegrind project
    if "capns" not in paths_added:
        capns_path = Path.home() / "ws" / "prj" / "filegrind" / "capns-py" / "src"
        if capns_path.exists():
            sys.path.insert(0, str(capns_path))

    if "tagged_urn" not in paths_added:
        tagged_urn_path = Path.home() / "ws" / "prj" / "filegrind" / "tagged-urn-py" / "src"
        if tagged_urn_path.exists():
            sys.path.insert(0, str(tagged_urn_path))

    if "ops" not in paths_added:
        ops_path = Path.home() / "ws" / "prj" / "filegrind" / "ops-py" / "src"
        if ops_path.exists():
            sys.path.insert(0, str(ops_path))

add_module_paths()

from capns.bifaci.plugin_runtime import PluginRuntime, Request, WET_KEY_REQUEST
from capns.bifaci.manifest import CapManifest, Cap
from capns.urn.cap_urn import CapUrn, CapUrnBuilder
from capns.cap.caller import CapArgumentValue
from capns.cap.definition import CapArg, CapOutput, StdinSource, PositionSource
from capns.bifaci.frame import Frame, FrameType
from capns.standard.caps import CAP_IDENTITY
from ops import Op, OpMetadata, DryContext, WetContext
import queue


# =============================================================================
# Helper functions
# =============================================================================

def cbor_value_to_bytes(value) -> bytes:
    """Convert CBOR value to bytes."""
    if isinstance(value, bytes):
        return value
    elif isinstance(value, str):
        return value.encode('utf-8')
    else:
        raise ValueError(f"Expected bytes or str, got {type(value)}")


def collect_payload(frames: queue.Queue):
    """Collect all CHUNK frames, decode each as CBOR, and return the reconstructed value.
    PROTOCOL: Each CHUNK payload is a complete, independently decodable CBOR value.
    Returns the decoded CBOR value (bytes, str, dict, list, int, etc.).
    """
    import cbor2
    chunks = []
    while True:
        try:
            frame = frames.get(timeout=30)
            if frame.frame_type == FrameType.CHUNK:
                if frame.payload:
                    value = cbor2.loads(frame.payload)
                    chunks.append(value)
            elif frame.frame_type == FrameType.END:
                break
        except queue.Empty:
            break

    if not chunks:
        return None
    elif len(chunks) == 1:
        return chunks[0]
    else:
        first = chunks[0]
        if isinstance(first, bytes):
            return b''.join(c for c in chunks if isinstance(c, bytes))
        elif isinstance(first, str):
            return ''.join(c for c in chunks if isinstance(c, str))
        else:
            return chunks


def collect_peer_response(peer_frames: queue.Queue):
    """Collect peer response frames, decode each CHUNK as CBOR, and reconstruct value."""
    import cbor2
    chunks = []
    while True:
        try:
            frame = peer_frames.get(timeout=30)
            if frame.frame_type == FrameType.CHUNK:
                if frame.payload:
                    value = cbor2.loads(frame.payload)
                    chunks.append(value)
            elif frame.frame_type == FrameType.END:
                break
            elif frame.frame_type == FrameType.ERR:
                code = frame.error_code() or "UNKNOWN"
                message = frame.error_message() or "Unknown error"
                raise RuntimeError(f"[{code}] {message}")
        except queue.Empty:
            break

    if not chunks:
        raise RuntimeError("No chunks received")
    elif len(chunks) == 1:
        return chunks[0]
    else:
        first = chunks[0]
        if isinstance(first, bytes):
            return b''.join(c for c in chunks if isinstance(c, bytes))
        elif isinstance(first, str):
            return ''.join(c for c in chunks if isinstance(c, str))
        else:
            return chunks


def build_manifest() -> CapManifest:
    """Build manifest with all test capabilities."""

    # Build read_file_info cap with args structure
    read_file_info_cap = Cap(
        urn=CapUrnBuilder()
            .tag("op", "read_file_info")
            .in_spec("media:file-path;invoice;textable")
            .out_spec("media:invoice-metadata;json;record;textable")
            .build(),
        title="Read File Info",
        command="read_file_info",
    )
    read_file_info_cap.args = [
        CapArg(
            media_urn="media:file-path;invoice;textable",
            required=True,
            sources=[
                StdinSource("media:"),
                PositionSource(0),
            ],
            arg_description="Path to invoice file to read",
        )
    ]
    read_file_info_cap.output = CapOutput(
        media_urn="media:invoice-metadata;json;record;textable",
        output_description="Invoice file size and SHA256 checksum",
    )

    caps = [
        # CAP_IDENTITY (required) - use from_string to parse the bare "cap:" constant
        Cap(
            urn=CapUrn.from_string(CAP_IDENTITY),
            title="Identity",
            command="identity",
        ),
        Cap(
            urn=CapUrnBuilder()
                .tag("op", "echo")
                .in_spec("media:")
                .out_spec("media:")
                .build(),
            title="Echo",
            command="echo",
        ),
        Cap(
            urn=CapUrnBuilder()
                .tag("op", "double")
                .in_spec("media:json;order-value;record;textable")
                .out_spec("media:integer;loyalty-points;numeric;textable")
                .build(),
            title="Double",
            command="double",
        ),
        Cap(
            urn=CapUrnBuilder()
                .tag("op", "stream_chunks")
                .in_spec("media:json;record;textable;update-count")
                .out_spec("media:order-updates;textable")
                .build(),
            title="Stream Chunks",
            command="stream_chunks",
        ),
        Cap(
            urn=CapUrnBuilder()
                .tag("op", "binary_echo")
                .in_spec("media:product-image")
                .out_spec("media:product-image")
                .build(),
            title="Binary Echo",
            command="binary_echo",
        ),
        Cap(
            urn=CapUrnBuilder()
                .tag("op", "slow_response")
                .in_spec("media:json;payment-delay-ms;record;textable")
                .out_spec("media:payment-result;textable")
                .build(),
            title="Slow Response",
            command="slow_response",
        ),
        Cap(
            urn=CapUrnBuilder()
                .tag("op", "generate_large")
                .in_spec("media:json;record;report-size;textable")
                .out_spec("media:sales-report")
                .build(),
            title="Generate Large",
            command="generate_large",
        ),
        Cap(
            urn=CapUrnBuilder()
                .tag("op", "with_status")
                .in_spec("media:fulfillment-steps;json;record;textable")
                .out_spec("media:fulfillment-status;textable")
                .build(),
            title="With Status",
            command="with_status",
        ),
        Cap(
            urn=CapUrnBuilder()
                .tag("op", "throw_error")
                .in_spec("media:json;payment-error;record;textable")
                .out_spec("media:void")
                .build(),
            title="Throw Error",
            command="throw_error",
        ),
        Cap(
            urn=CapUrnBuilder()
                .tag("op", "peer_echo")
                .in_spec("media:customer-message;textable")
                .out_spec("media:customer-message;textable")
                .build(),
            title="Peer Echo",
            command="peer_echo",
        ),
        Cap(
            urn=CapUrnBuilder()
                .tag("op", "nested_call")
                .in_spec("media:json;order-value;record;textable")
                .out_spec("media:final-price;integer;numeric;textable")
                .build(),
            title="Nested Call",
            command="nested_call",
        ),
        Cap(
            urn=CapUrnBuilder()
                .tag("op", "heartbeat_stress")
                .in_spec("media:json;monitoring-duration-ms;record;textable")
                .out_spec("media:health-status;textable")
                .build(),
            title="Heartbeat Stress",
            command="heartbeat_stress",
        ),
        Cap(
            urn=CapUrnBuilder()
                .tag("op", "concurrent_stress")
                .in_spec("media:json;order-batch-size;record;textable")
                .out_spec("media:batch-result;textable")
                .build(),
            title="Concurrent Stress",
            command="concurrent_stress",
        ),
        Cap(
            urn=CapUrnBuilder()
                .tag("op", "get_manifest")
                .in_spec("media:void")
                .out_spec("media:json;record;service-capabilities;textable")
                .build(),
            title="Get Manifest",
            command="get_manifest",
        ),
        Cap(
            urn=CapUrnBuilder()
                .tag("op", "process_large")
                .in_spec("media:uploaded-document")
                .out_spec("media:document-info;json;record;textable")
                .build(),
            title="Process Large",
            command="process_large",
        ),
        Cap(
            urn=CapUrnBuilder()
                .tag("op", "hash_incoming")
                .in_spec("media:uploaded-document")
                .out_spec("media:document-hash;textable")
                .build(),
            title="Hash Incoming",
            command="hash_incoming",
        ),
        Cap(
            urn=CapUrnBuilder()
                .tag("op", "verify_binary")
                .in_spec("media:package-data")
                .out_spec("media:verification-status;textable")
                .build(),
            title="Verify Binary",
            command="verify_binary",
        ),
        read_file_info_cap,
    ]

    return CapManifest(
        name="InteropTestPlugin",
        version="1.0.0",
        description="Interoperability testing plugin (Python)",
        caps=caps,
    )


# =============================================================================
# Op Implementations
# =============================================================================

# === STREAMING OPS (no accumulation) ===

class EchoOp(Op):
    async def perform(self, dry: DryContext, wet: WetContext) -> None:
        req: Request = wet.get_required(WET_KEY_REQUEST)
        payload = collect_payload(req.take_frames())
        payload_bytes = cbor_value_to_bytes(payload)
        req.emitter().emit_cbor(payload_bytes)

    def metadata(self) -> OpMetadata:
        return OpMetadata.builder("EchoOp").build()


class BinaryEchoOp(Op):
    async def perform(self, dry: DryContext, wet: WetContext) -> None:
        req: Request = wet.get_required(WET_KEY_REQUEST)
        payload = collect_payload(req.take_frames())
        payload_bytes = cbor_value_to_bytes(payload)
        req.emitter().emit_cbor(payload_bytes)

    def metadata(self) -> OpMetadata:
        return OpMetadata.builder("BinaryEchoOp").build()


class PeerEchoOp(Op):
    async def perform(self, dry: DryContext, wet: WetContext) -> None:
        req: Request = wet.get_required(WET_KEY_REQUEST)
        print("[peer_echo] Handler started", file=sys.stderr)
        payload = collect_payload(req.take_frames())
        payload_bytes = cbor_value_to_bytes(payload)
        print(f"[peer_echo] Collected {len(payload_bytes)} bytes, calling peer", file=sys.stderr)

        peer_frames = req.peer().invoke(
            "cap:in=media:;out=media:",
            [CapArgumentValue("media:customer-message;textable", payload_bytes)],
        )

        print("[peer_echo] Got peer response stream", file=sys.stderr)
        cbor_value = collect_peer_response(peer_frames)
        print(f"[peer_echo] Got peer response value: {cbor_value!r}", file=sys.stderr)
        req.emitter().emit_cbor(cbor_value)

    def metadata(self) -> OpMetadata:
        return OpMetadata.builder("PeerEchoOp").build()


# === ACCUMULATING OPS ===

class DoubleOp(Op):
    async def perform(self, dry: DryContext, wet: WetContext) -> None:
        req: Request = wet.get_required(WET_KEY_REQUEST)
        print("[double] Handler starting", file=sys.stderr)
        payload = collect_payload(req.take_frames())
        data = payload if isinstance(payload, dict) else json.loads(payload)
        value = data["value"]
        result = value * 2
        print(f"[double] Parsed value: {value}, doubling to: {result}", file=sys.stderr)
        req.emitter().emit_cbor(result)
        print("[double] Handler complete", file=sys.stderr)

    def metadata(self) -> OpMetadata:
        return OpMetadata.builder("DoubleOp").build()


class StreamChunksOp(Op):
    async def perform(self, dry: DryContext, wet: WetContext) -> None:
        req: Request = wet.get_required(WET_KEY_REQUEST)
        payload = collect_payload(req.take_frames())
        data = payload if isinstance(payload, dict) else json.loads(payload)
        count = data["value"]
        for i in range(count):
            req.emitter().emit_cbor(f"chunk-{i}".encode('utf-8'))
        req.emitter().emit_cbor(b"done")

    def metadata(self) -> OpMetadata:
        return OpMetadata.builder("StreamChunksOp").build()


class SlowResponseOp(Op):
    async def perform(self, dry: DryContext, wet: WetContext) -> None:
        req: Request = wet.get_required(WET_KEY_REQUEST)
        payload = collect_payload(req.take_frames())
        data = payload if isinstance(payload, dict) else json.loads(payload)
        sleep_ms = data["value"]
        time.sleep(sleep_ms / 1000.0)
        req.emitter().emit_cbor(f"slept-{sleep_ms}ms".encode('utf-8'))

    def metadata(self) -> OpMetadata:
        return OpMetadata.builder("SlowResponseOp").build()


class GenerateLargeOp(Op):
    async def perform(self, dry: DryContext, wet: WetContext) -> None:
        req: Request = wet.get_required(WET_KEY_REQUEST)
        payload = collect_payload(req.take_frames())
        data = payload if isinstance(payload, dict) else json.loads(payload)
        size = data["value"]
        pattern = b"ABCDEFGH"
        result = bytearray()
        for i in range(size):
            result.append(pattern[i % len(pattern)])
        req.emitter().emit_cbor(bytes(result))

    def metadata(self) -> OpMetadata:
        return OpMetadata.builder("GenerateLargeOp").build()


class WithStatusOp(Op):
    async def perform(self, dry: DryContext, wet: WetContext) -> None:
        req: Request = wet.get_required(WET_KEY_REQUEST)
        payload = collect_payload(req.take_frames())
        data = payload if isinstance(payload, dict) else json.loads(payload)
        steps = data["value"]
        for i in range(steps):
            req.emitter().emit_log("processing", f"step {i}")
            time.sleep(0.01)
        req.emitter().emit_cbor(b"completed")

    def metadata(self) -> OpMetadata:
        return OpMetadata.builder("WithStatusOp").build()


class ThrowErrorOp(Op):
    async def perform(self, dry: DryContext, wet: WetContext) -> None:
        req: Request = wet.get_required(WET_KEY_REQUEST)
        payload = collect_payload(req.take_frames())
        data = payload if isinstance(payload, dict) else json.loads(payload)
        message = data["value"]
        raise RuntimeError(message)

    def metadata(self) -> OpMetadata:
        return OpMetadata.builder("ThrowErrorOp").build()


class NestedCallOp(Op):
    async def perform(self, dry: DryContext, wet: WetContext) -> None:
        req: Request = wet.get_required(WET_KEY_REQUEST)
        print("[nested_call] Starting handler", file=sys.stderr)
        payload = collect_payload(req.take_frames())
        data = payload if isinstance(payload, dict) else json.loads(payload)
        value = data["value"]
        print(f"[nested_call] Parsed value: {value}", file=sys.stderr)

        input_data = json.dumps({"value": value}).encode('utf-8')
        print("[nested_call] Calling peer double", file=sys.stderr)
        peer_frames = req.peer().invoke(
            'cap:in="media:json;order-value;record;textable";op=double;out="media:integer;loyalty-points;numeric;textable"',
            [CapArgumentValue("media:json;order-value;record;textable", input_data)],
        )

        cbor_value = collect_peer_response(peer_frames)
        print(f"[nested_call] Peer response: {cbor_value!r}", file=sys.stderr)

        if isinstance(cbor_value, int):
            host_result = cbor_value
        elif isinstance(cbor_value, bytes):
            host_result = json.loads(cbor_value)
        else:
            host_result = cbor_value

        final_result = host_result * 2
        print(f"[nested_call] Final result: {final_result}", file=sys.stderr)
        req.emitter().emit_cbor(final_result)

    def metadata(self) -> OpMetadata:
        return OpMetadata.builder("NestedCallOp").build()


class HeartbeatStressOp(Op):
    async def perform(self, dry: DryContext, wet: WetContext) -> None:
        req: Request = wet.get_required(WET_KEY_REQUEST)
        payload = collect_payload(req.take_frames())
        data = payload if isinstance(payload, dict) else json.loads(payload)
        duration_ms = data["value"]
        chunks = duration_ms // 100
        for _ in range(chunks):
            time.sleep(0.1)
        time.sleep((duration_ms % 100) / 1000.0)
        req.emitter().emit_cbor(f"stressed-{duration_ms}ms".encode('utf-8'))

    def metadata(self) -> OpMetadata:
        return OpMetadata.builder("HeartbeatStressOp").build()


class ConcurrentStressOp(Op):
    async def perform(self, dry: DryContext, wet: WetContext) -> None:
        req: Request = wet.get_required(WET_KEY_REQUEST)
        payload = collect_payload(req.take_frames())
        data = payload if isinstance(payload, dict) else json.loads(payload)
        work_units = data["value"]
        total = 0
        for i in range(work_units * 1000):
            total = (total + i) & 0xFFFFFFFFFFFFFFFF  # Keep in u64 range
        req.emitter().emit_cbor(f"computed-{total}".encode('utf-8'))

    def metadata(self) -> OpMetadata:
        return OpMetadata.builder("ConcurrentStressOp").build()


class GetManifestOp(Op):
    async def perform(self, dry: DryContext, wet: WetContext) -> None:
        req: Request = wet.get_required(WET_KEY_REQUEST)
        collect_payload(req.take_frames())  # Consume frames (media:void)
        manifest = build_manifest()
        result_bytes = json.dumps(manifest.to_dict()).encode('utf-8')
        req.emitter().emit_cbor(result_bytes)

    def metadata(self) -> OpMetadata:
        return OpMetadata.builder("GetManifestOp").build()


class ProcessLargeOp(Op):
    async def perform(self, dry: DryContext, wet: WetContext) -> None:
        req: Request = wet.get_required(WET_KEY_REQUEST)
        payload = collect_payload(req.take_frames())
        payload_bytes = cbor_value_to_bytes(payload)
        checksum = hashlib.sha256(payload_bytes).hexdigest()
        result = {"size": len(payload_bytes), "checksum": checksum}
        req.emitter().emit_cbor(json.dumps(result).encode('utf-8'))

    def metadata(self) -> OpMetadata:
        return OpMetadata.builder("ProcessLargeOp").build()


class HashIncomingOp(Op):
    async def perform(self, dry: DryContext, wet: WetContext) -> None:
        req: Request = wet.get_required(WET_KEY_REQUEST)
        payload = collect_payload(req.take_frames())
        payload_bytes = cbor_value_to_bytes(payload)
        checksum = hashlib.sha256(payload_bytes).hexdigest()
        req.emitter().emit_cbor(checksum.encode('utf-8'))

    def metadata(self) -> OpMetadata:
        return OpMetadata.builder("HashIncomingOp").build()


class VerifyBinaryOp(Op):
    async def perform(self, dry: DryContext, wet: WetContext) -> None:
        req: Request = wet.get_required(WET_KEY_REQUEST)
        payload = collect_payload(req.take_frames())
        payload_bytes = cbor_value_to_bytes(payload)
        seen = set(payload_bytes)
        if len(seen) == 256:
            req.emitter().emit_cbor(b"ok")
        else:
            missing = sorted(b for b in range(256) if b not in seen)
            msg = f"missing byte values: {missing[:10]}"
            if len(missing) > 10:
                msg += f" and {len(missing) - 10} more"
            req.emitter().emit_cbor(msg.encode('utf-8'))

    def metadata(self) -> OpMetadata:
        return OpMetadata.builder("VerifyBinaryOp").build()


class ReadFileInfoOp(Op):
    async def perform(self, dry: DryContext, wet: WetContext) -> None:
        req: Request = wet.get_required(WET_KEY_REQUEST)
        payload = collect_payload(req.take_frames())
        payload_bytes = cbor_value_to_bytes(payload)
        checksum = hashlib.sha256(payload_bytes).hexdigest()
        result = {"size": len(payload_bytes), "checksum": checksum}
        req.emitter().emit_cbor(json.dumps(result).encode('utf-8'))

    def metadata(self) -> OpMetadata:
        return OpMetadata.builder("ReadFileInfoOp").build()


def main():
    """Main entry point."""
    manifest = build_manifest()
    runtime = PluginRuntime.with_manifest(manifest)

    # Register all handlers as Op types
    runtime.register_op_type('cap:in="media:";op=echo;out="media:"', EchoOp)
    runtime.register_op_type('cap:in="media:json;order-value;record;textable";op=double;out="media:integer;loyalty-points;numeric;textable"', DoubleOp)
    runtime.register_op_type('cap:in="media:json;record;textable;update-count";op=stream_chunks;out="media:order-updates;textable"', StreamChunksOp)
    runtime.register_op_type('cap:in="media:product-image";op=binary_echo;out="media:product-image"', BinaryEchoOp)
    runtime.register_op_type('cap:in="media:json;payment-delay-ms;record;textable";op=slow_response;out="media:payment-result;textable"', SlowResponseOp)
    runtime.register_op_type('cap:in="media:json;record;report-size;textable";op=generate_large;out="media:sales-report"', GenerateLargeOp)
    runtime.register_op_type('cap:in="media:fulfillment-steps;json;record;textable";op=with_status;out="media:fulfillment-status;textable"', WithStatusOp)
    runtime.register_op_type('cap:in="media:json;payment-error;record;textable";op=throw_error;out=media:void', ThrowErrorOp)
    runtime.register_op_type('cap:in="media:customer-message;textable";op=peer_echo;out="media:customer-message;textable"', PeerEchoOp)
    runtime.register_op_type('cap:in="media:json;order-value;record;textable";op=nested_call;out="media:final-price;integer;numeric;textable"', NestedCallOp)
    runtime.register_op_type('cap:in="media:json;monitoring-duration-ms;record;textable";op=heartbeat_stress;out="media:health-status;textable"', HeartbeatStressOp)
    runtime.register_op_type('cap:in="media:json;order-batch-size;record;textable";op=concurrent_stress;out="media:batch-result;textable"', ConcurrentStressOp)
    runtime.register_op_type('cap:in=media:void;op=get_manifest;out="media:json;record;service-capabilities;textable"', GetManifestOp)
    runtime.register_op_type('cap:in="media:uploaded-document";op=process_large;out="media:document-info;json;record;textable"', ProcessLargeOp)
    runtime.register_op_type('cap:in="media:uploaded-document";op=hash_incoming;out="media:document-hash;textable"', HashIncomingOp)
    runtime.register_op_type('cap:in="media:package-data";op=verify_binary;out="media:verification-status;textable"', VerifyBinaryOp)
    runtime.register_op_type('cap:in="media:file-path;invoice;textable";op=read_file_info;out="media:invoice-metadata;json;record;textable"', ReadFileInfoOp)

    runtime.run()


if __name__ == "__main__":
    main()
