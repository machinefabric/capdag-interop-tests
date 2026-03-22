"""Performance benchmark tests.

Measures latency, throughput, and large payload transfer speed across
router x host x plugin combinations using raw CBOR frames.

Architecture:
    Test (Engine) → Router (RelaySwitch) → Host (PluginHost) → Plugin
"""

import json
import os
import time
import statistics
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

SUPPORTED_ROUTER_LANGS = ["rust", "swift"]  # Both routers
SUPPORTED_HOST_LANGS = ["rust", "go", "swift"]  # All relay hosts (no python relay host)
SUPPORTED_PLUGIN_LANGS = ["rust", "go", "python", "swift"]


@pytest.mark.timeout(60)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_latency_benchmark(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Benchmark request/response latency: 100 echo iterations, report p50/p95/p99."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()
        iterations = 100
        latencies = []

        for _ in range(iterations):
            test_input = b"benchmark"
            req_id = make_req_id()

            start = time.perf_counter()
            send_request(writer, req_id, TEST_CAPS["echo"], test_input)
            output, frames = read_response(reader)
            duration = (time.perf_counter() - start) * 1000

            assert output == test_input
            latencies.append(duration)

        p50 = statistics.median(latencies)
        p95 = statistics.quantiles(latencies, n=20)[18]
        p99 = statistics.quantiles(latencies, n=100)[98]
        avg = statistics.mean(latencies)

        print(
            f"\n  [{router_lang}/{host_lang}/{plugin_lang}] Latency: "
            f"p50={p50:.2f}ms, p95={p95:.2f}ms, p99={p99:.2f}ms, avg={avg:.2f}ms"
        )

        # 3-tier adds router hop overhead vs 2-tier
        if plugin_lang == "python":
            threshold = 1600
        else:
            threshold = 400
        assert p99 < threshold, (
            f"[{router_lang}/{host_lang}/{plugin_lang}] p99 latency too high: {p99:.2f}ms (threshold: {threshold}ms)"
        )



@pytest.mark.timeout(60)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_throughput_benchmark(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Benchmark throughput: echo requests/second over 2 seconds."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()
        duration_seconds = 2
        test_input = b"throughput"
        count = 0

        start = time.perf_counter()
        while (time.perf_counter() - start) < duration_seconds:
            req_id = make_req_id()
            send_request(writer, req_id, TEST_CAPS["echo"], test_input)
            output, frames = read_response(reader)
            assert output == test_input
            count += 1

        elapsed = time.perf_counter() - start
        rps = count / elapsed

        print(
            f"\n  [{router_lang}/{host_lang}/{plugin_lang}] Throughput: "
            f"{rps:.2f} req/s ({count} requests in {elapsed:.2f}s)"
        )

        # 3-tier has significant overhead (router→socket→host→plugin = 3 process hops)
        if plugin_lang == "python":
            threshold = 5
        else:
            threshold = 10
        assert rps > threshold, (
            f"[{router_lang}/{host_lang}/{plugin_lang}] throughput too low: {rps:.2f} req/s (threshold: {threshold})"
        )



@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_large_payload_throughput(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang, throughput_results):
    """Benchmark large payload transfer: 10MB generated data, report MB/s."""
    try:
        topology = (TestTopology()
            .router(router_binaries[router_lang])
            .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
            .build())

        with topology:
            reader, writer = topology.start()
            # Read payload size from environment variable (default 10 MB)
            payload_mb = int(os.environ.get('THROUGHPUT_MB', '10'))
            payload_size = payload_mb * 1024 * 1024
            input_json = json.dumps({"value": payload_size}).encode()

            req_id = make_req_id()
            start = time.perf_counter()
            send_request(writer, req_id, TEST_CAPS["generate_large"], input_json, media_urn="media:report-size;json;textable;record")

            # Count bytes without accumulating (avoids Python memory pressure for large payloads)
            import cbor2
            total_bytes = 0
            for _ in range(50000):
                frame = reader.read()
                if frame is None:
                    break
                if frame.frame_type == FrameType.CHUNK and frame.payload:
                    decoded = cbor2.loads(frame.payload)
                    if isinstance(decoded, bytes):
                        total_bytes += len(decoded)
                    elif isinstance(decoded, str):
                        total_bytes += len(decoded.encode('utf-8'))
                    else:
                        raise ValueError(f"Unexpected CBOR type in chunk: {type(decoded)}")
                if frame.frame_type in (FrameType.END, FrameType.ERR):
                    break

            elapsed = time.perf_counter() - start
            assert total_bytes == payload_size, (
                f"[{router_lang}/{host_lang}/{plugin_lang}] expected {payload_size} bytes, got {total_bytes}"
            )

            mb_per_sec = (total_bytes / (1024 * 1024)) / elapsed
            print(
                f"\n  [{router_lang}/{host_lang}/{plugin_lang}] Large payload: "
                f"{mb_per_sec:.2f} MB/s ({payload_size / (1024 * 1024):.0f} MB in {elapsed:.2f}s)"
            )

            # Record throughput result for matrix display
            throughput_results.record(router_lang, host_lang, plugin_lang, mb_per_sec, "pass")

            assert mb_per_sec > 1, (
                f"[{router_lang}/{host_lang}/{plugin_lang}] throughput too low: {mb_per_sec:.2f} MB/s"
            )
    except Exception as e:
        # Record failure
        throughput_results.record(router_lang, host_lang, plugin_lang, None, "fail")
        raise



@pytest.mark.timeout(60)
@pytest.mark.parametrize("router_lang", SUPPORTED_ROUTER_LANGS)
@pytest.mark.parametrize("host_lang", SUPPORTED_HOST_LANGS)
@pytest.mark.parametrize("plugin_lang", SUPPORTED_PLUGIN_LANGS)
def test_concurrent_stress(router_binaries, relay_host_binaries, plugin_binaries, router_lang, host_lang, plugin_lang):
    """Test concurrent workload simulation: plugin processes 100 work units."""
    topology = (TestTopology()
        .router(router_binaries[router_lang])
        .host("default", relay_host_binaries[host_lang], [plugin_binaries[plugin_lang]])
        .build())

    with topology:
        reader, writer = topology.start()
        work_units = 100
        input_json = json.dumps({"value": work_units}).encode()
        req_id = make_req_id()
        send_request(writer, req_id, TEST_CAPS["concurrent_stress"], input_json, media_urn="media:order-batch-size;json;textable;record")
        output, frames = read_response(reader)

        if isinstance(output, bytes):
            output_str = output.decode("utf-8", errors="replace")
        else:
            output_str = str(output)

        assert output_str.startswith("computed-"), (
            f"[{router_lang}/{host_lang}/{plugin_lang}] unexpected output: {output_str!r}"
        )

