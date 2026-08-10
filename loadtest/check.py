#!/usr/bin/env python3
import json
import os
import sys


def milliseconds(nanoseconds: float) -> float:
    return nanoseconds / 1_000_000


if len(sys.argv) != 2:
    raise SystemExit("usage: check.py <vegeta-report.json>")

with open(sys.argv[1], encoding="utf-8") as report_file:
    report = json.load(report_file)

target_rps = float(os.getenv("TARGET_RPS", "5000"))
minimum_success = float(os.getenv("MIN_SUCCESS_RATIO", "0.99"))
minimum_throughput = float(
    os.getenv("MIN_THROUGHPUT_RPS", str(target_rps * minimum_success))
)

rate = float(report["rate"])
throughput = float(report["throughput"])
success = float(report["success"])
latencies = report["latencies"]

print(
    "Acceptance summary: "
    f"rate={rate:.2f} rps, throughput={throughput:.2f} rps, "
    f"success={success * 100:.3f}%, "
    f"p95={milliseconds(latencies['95th']):.2f} ms, "
    f"p99={milliseconds(latencies['99th']):.2f} ms"
)

failures = []
if rate < target_rps:
    failures.append(f"offered rate {rate:.2f} is below {target_rps:.2f} rps")
if throughput < minimum_throughput:
    failures.append(
        f"successful throughput {throughput:.2f} is below "
        f"{minimum_throughput:.2f} rps"
    )
if success < minimum_success:
    failures.append(
        f"success ratio {success * 100:.3f}% is below "
        f"{minimum_success * 100:.3f}%"
    )

if failures:
    print("LOAD TEST FAILED:", file=sys.stderr)
    for failure in failures:
        print(f"- {failure}", file=sys.stderr)
    errors = report.get("errors") or []
    for error in errors[:10]:
        print(f"- Vegeta: {error}", file=sys.stderr)
    raise SystemExit(1)

print("LOAD TEST PASSED")
