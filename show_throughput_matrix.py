#!/opt/homebrew/Caskroom/miniforge/base/envs/machinefabric/bin/python
"""Display throughput matrix from test results.

Reads artifacts/throughput_matrix.json and displays a formatted matrix
showing MB/s for each host-plugin combination.

Usage:
    python show_throughput_matrix.py
    python show_throughput_matrix.py artifacts/throughput_matrix.json
"""

import json
import sys
from pathlib import Path


def print_throughput_matrix(json_file):
    """Print formatted throughput matrix."""
    if not Path(json_file).exists():
        print(f"No throughput matrix found at: {json_file}")
        print("Run: pytest tests/test_performance.py::test_large_payload_throughput")
        return

    with open(json_file) as f:
        data = json.load(f)

    langs = ["rust", "go", "python", "swift"]
    routers = sorted(data.keys())

    # Print one matrix per router
    all_rows = []
    for router in routers:
        router_data = data[router]

        # Header
        print()
        print("="*80)
        print(f"THROUGHPUT MATRIX (MB/s) - Router: {router.upper()}")
        print("="*80)
        print()
        header = 'host ↓ \\ plugin →'
        print(f"  {header:>20}", end="")
        for p in langs:
            print(f"  {p:>8}", end="")
        print()
        print(f"  {'─' * 20}", end="")
        for _ in langs:
            print(f"  {'─' * 8}", end="")
        print()

        # Rows
        for h in langs:
            print(f"  {h:>20}", end="")
            for p in langs:
                cell = router_data.get(h, {}).get(p)
                if cell is None:
                    print(f"  {'--':>8}", end="")
                elif cell.get("status") == "pass" and cell.get("mb_per_sec") is not None:
                    mb_s = cell["mb_per_sec"]
                    print(f"  {mb_s:>8.2f}", end="")
                    all_rows.append((f"{router}-{h}-{p}", mb_s))
                else:
                    print(f"  {'X':>8}", end="")
                    all_rows.append((f"{router}-{h}-{p}", None))
            print()

    # Sorted ranking across all routers
    print()
    print("="*80)
    print("RANKING (fastest to slowest)")
    print("="*80)
    print()
    all_rows.sort(key=lambda r: (r[1] is None, -(r[1] or 0)))
    for label, val in all_rows:
        if val is not None:
            print(f"  {label:<30} {val:>8.2f} MB/s")
        else:
            print(f"  {label:<30} {'FAIL':>8}")
    print()

    # Bar chart (slowest to fastest)
    print()
    print("="*80)
    print("BAR CHART (slowest to fastest)")
    print("="*80)
    print()

    # Filter out failures and sort slowest to fastest
    successful_rows = [(label, val) for label, val in all_rows if val is not None]
    successful_rows.sort(key=lambda r: r[1])  # slowest first

    if not successful_rows:
        print("  No successful tests to chart")
        print()
        return

    # Find max value for scaling
    max_val = max(val for _, val in successful_rows)
    bar_width = 60  # max characters for bar

    for label, val in successful_rows:
        # Scale bar proportionally to max value
        bar_len = int((val / max_val) * bar_width) if max_val > 0 else 0
        bar = "█" * bar_len
        print(f"  {label:<30} {bar} {val:>6.2f} MB/s")
    print()


if __name__ == "__main__":
    json_file = sys.argv[1] if len(sys.argv) > 1 else "artifacts/throughput_matrix.json"
    print_throughput_matrix(json_file)
