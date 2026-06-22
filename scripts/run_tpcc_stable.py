#!/usr/bin/env python3
"""Run the TPCC matrix only when the machine is cool and idle enough."""

from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_BINARY = ROOT_DIR / "target" / "release" / "tpcc"
DEFAULT_TMP_DIR = ROOT_DIR / "target" / "tpcc-stable-run-data"


@dataclass(frozen=True)
class Variant:
    name: str
    backend: str
    db_name: str
    sqlite_profile: str | None = None


@dataclass(frozen=True)
class Health:
    cpu_percent: float | None
    max_temp_c: float | None
    temp_sources: int

    def format(self) -> str:
        cpu = "-" if self.cpu_percent is None else f"{self.cpu_percent:.1f}%"
        temp = "-" if self.max_temp_c is None else f"{self.max_temp_c:.1f}C"
        return f"cpu={cpu}, max_temp={temp}, temp_sources={self.temp_sources}"


VARIANTS = [
    Variant("kitesql-lmdb", "kitesql-lmdb", "kitesql-lmdb"),
    Variant("kitesql-rocksdb", "kitesql-rocksdb", "kitesql-rocksdb"),
    Variant("sqlite-balanced", "sqlite", "sqlite-balanced.sqlite", "balanced"),
    Variant("sqlite-practical", "sqlite", "sqlite-practical.sqlite", "practical"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run kitesql-lmdb, kitesql-rocksdb, sqlite-balanced, and "
            "sqlite-practical TPCC benchmarks with cooldown gates between runs."
        )
    )
    parser.add_argument("--binary", type=Path, default=DEFAULT_BINARY)
    parser.add_argument("--result-dir", type=Path)
    parser.add_argument("--tmp-dir", type=Path, default=DEFAULT_TMP_DIR)
    parser.add_argument("--stamp", default=dt.datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))
    parser.add_argument("--num-ware", type=int, default=int(os.getenv("TPCC_NUM_WARE", "1")))
    parser.add_argument("--measure-time", type=int, default=None)
    parser.add_argument("--tpcc-max-retry", type=int, default=5)
    parser.add_argument("--duplicate-retry", type=int, default=1)
    parser.add_argument("--cool-temp-c", type=float, default=65.0)
    parser.add_argument("--idle-cpu-percent", type=float, default=20.0)
    parser.add_argument("--min-cooldown-sec", type=int, default=300)
    parser.add_argument("--stable-samples", type=int, default=3)
    parser.add_argument("--sample-interval-sec", type=float, default=10.0)
    parser.add_argument("--max-wait-sec", type=int, default=7200)
    parser.add_argument("--skip-health-check", action="store_true")
    parser.add_argument(
        "--build",
        action="store_true",
        help="Run `cargo build -p tpcc --release` before the benchmark matrix.",
    )
    return parser.parse_args()


def read_proc_stat() -> tuple[int, int] | None:
    try:
        line = Path("/proc/stat").read_text(encoding="utf-8").splitlines()[0]
    except (OSError, IndexError):
        return None
    parts = line.split()
    if not parts or parts[0] != "cpu":
        return None
    values = [int(v) for v in parts[1:]]
    idle = values[3] + (values[4] if len(values) > 4 else 0)
    total = sum(values)
    return idle, total


def sample_cpu_percent(interval_sec: float) -> float | None:
    first = read_proc_stat()
    if first is None:
        return None
    time.sleep(interval_sec)
    second = read_proc_stat()
    if second is None:
        return None
    idle_delta = second[0] - first[0]
    total_delta = second[1] - first[1]
    if total_delta <= 0:
        return None
    busy_delta = max(0, total_delta - idle_delta)
    return 100.0 * busy_delta / total_delta


def is_likely_cpu_sensor(label: str, name: str) -> bool:
    text = f"{label} {name}".lower()
    cpu_tokens = (
        "cpu",
        "core",
        "package",
        "pkg",
        "k10temp",
        "zenpower",
        "coretemp",
        "x86_pkg_temp",
        "tctl",
        "tdie",
        "ccd",
    )
    non_cpu_tokens = ("nvme", "ssd", "pch", "acpitz", "wifi", "iwlwifi", "battery", "bat")
    if any(token in text for token in cpu_tokens):
        return True
    if any(token in text for token in non_cpu_tokens):
        return False
    return False


def read_temp_file(path: Path) -> float | None:
    try:
        raw = path.read_text(encoding="utf-8").strip()
        value = float(raw)
    except (OSError, ValueError):
        return None
    if value > 1000.0:
        value /= 1000.0
    if -20.0 <= value <= 130.0:
        return value
    return None


def read_temperature_samples() -> list[float]:
    samples: list[float] = []

    for hwmon in Path("/sys/class/hwmon").glob("hwmon*"):
        name = ""
        try:
            name = (hwmon / "name").read_text(encoding="utf-8").strip()
        except OSError:
            pass
        for temp_input in hwmon.glob("temp*_input"):
            label_path = temp_input.with_name(temp_input.name.replace("_input", "_label"))
            try:
                label = label_path.read_text(encoding="utf-8").strip()
            except OSError:
                label = ""
            if not is_likely_cpu_sensor(label, name):
                continue
            value = read_temp_file(temp_input)
            if value is not None:
                samples.append(value)

    for zone in Path("/sys/class/thermal").glob("thermal_zone*"):
        try:
            zone_type = (zone / "type").read_text(encoding="utf-8").strip()
        except OSError:
            zone_type = ""
        if not is_likely_cpu_sensor(zone_type, ""):
            continue
        value = read_temp_file(zone / "temp")
        if value is not None:
            samples.append(value)

    return samples


def sample_health(args: argparse.Namespace) -> Health:
    cpu_percent = sample_cpu_percent(args.sample_interval_sec)
    temps = read_temperature_samples()
    return Health(
        cpu_percent=cpu_percent,
        max_temp_c=max(temps) if temps else None,
        temp_sources=len(temps),
    )


def append_line(path: Path, line: str = "") -> None:
    with path.open("a", encoding="utf-8") as file:
        file.write(line)
        file.write("\n")


def wait_for_ready(
    args: argparse.Namespace,
    log_file: Path,
    variant_name: str,
    previous_finished_at: float | None,
) -> Health:
    if args.skip_health_check:
        health = Health(cpu_percent=None, max_temp_c=None, temp_sources=0)
        append_line(log_file, f"[runner] health checks skipped for {variant_name}")
        return health

    append_line(log_file, f"[runner] waiting for stable machine state before {variant_name}")
    append_line(
        log_file,
        "[runner] gates: "
        f"temp<={args.cool_temp_c:.1f}C, "
        f"cpu<={args.idle_cpu_percent:.1f}%, "
        f"min_cooldown={args.min_cooldown_sec}s, "
        f"stable_samples={args.stable_samples}",
    )

    start = time.monotonic()
    stable = 0
    last_health = Health(cpu_percent=None, max_temp_c=None, temp_sources=0)

    while True:
        elapsed = time.monotonic() - start
        if elapsed > args.max_wait_sec:
            raise TimeoutError(
                f"waited {args.max_wait_sec}s for {variant_name}, last state: {last_health.format()}"
            )

        cooldown_left = 0.0
        if previous_finished_at is not None:
            cooldown_left = max(
                0.0, args.min_cooldown_sec - (time.monotonic() - previous_finished_at)
            )

        health = sample_health(args)
        last_health = health
        temp_ok = health.max_temp_c is None or health.max_temp_c <= args.cool_temp_c
        cpu_ok = health.cpu_percent is None or health.cpu_percent <= args.idle_cpu_percent
        cooldown_ok = cooldown_left <= 0.0

        reasons = []
        if not cooldown_ok:
            reasons.append(f"cooldown_left={cooldown_left:.0f}s")
        if not temp_ok:
            reasons.append(f"temp={health.max_temp_c:.1f}C")
        if not cpu_ok:
            reasons.append(f"cpu={health.cpu_percent:.1f}%")

        if cooldown_ok and temp_ok and cpu_ok:
            stable += 1
            append_line(
                log_file,
                f"[runner] ready sample {stable}/{args.stable_samples}: {health.format()}",
            )
            if stable >= args.stable_samples:
                append_line(log_file, f"[runner] machine accepted for {variant_name}")
                return health
        else:
            stable = 0
            append_line(
                log_file,
                f"[runner] waiting: {health.format()}, reason={', '.join(reasons)}",
            )


def remove_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
    elif path.exists():
        path.unlink()


def quote_cmd(cmd: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in cmd)


def build_command(args: argparse.Namespace, variant: Variant, db_path: Path) -> list[str]:
    cmd = [
        str(args.binary),
        "--backend",
        variant.backend,
        "--num-ware",
        str(args.num_ware),
        "--max-retry",
        str(args.tpcc_max_retry),
        "--path",
        str(db_path),
    ]
    if args.measure_time is not None:
        cmd.extend(["--measure-time", str(args.measure_time)])
    if variant.sqlite_profile is not None:
        cmd.extend(["--sqlite-profile", variant.sqlite_profile])
    return cmd


def duplicate_key_failure(log_text: str) -> bool:
    needles = (
        "UNIQUE constraint failed",
        "duplicate key",
        "primary key",
        "Duplicate",
    )
    return any(needle in log_text for needle in needles)


def extract_tpmc(log_text: str) -> str:
    match = re.search(r"<TpmC>\s*\n\s*([0-9.]+)", log_text)
    return match.group(1) if match else "-"


def extract_p90(log_text: str, label: str) -> str:
    marker = "<90th Percentile RT (MaxRT)>"
    if marker not in log_text:
        return "-"
    block = log_text.split(marker, 1)[1]
    for line in block.splitlines():
        if label not in line:
            continue
        parts = line.split()
        return parts[2] if len(parts) >= 3 else "-"
    return "-"


def summarize_log(log_file: Path, status: str, notes: str) -> dict[str, str]:
    log_text = log_file.read_text(encoding="utf-8", errors="replace")
    if status != "ok":
        return {
            "tpmc": "-",
            "new_order": "-",
            "payment": "-",
            "order_status": "-",
            "delivery": "-",
            "stock_level": "-",
            "notes": notes,
        }
    return {
        "tpmc": extract_tpmc(log_text),
        "new_order": extract_p90(log_text, "New-Order"),
        "payment": extract_p90(log_text, "Payment"),
        "order_status": extract_p90(log_text, "Order-Status"),
        "delivery": extract_p90(log_text, "Delivery"),
        "stock_level": extract_p90(log_text, "Stock-Level"),
        "notes": notes,
    }


def run_variant(
    args: argparse.Namespace,
    variant: Variant,
    log_dir: Path,
    summary_file: Path,
    previous_finished_at: float | None,
) -> float:
    log_file = log_dir / f"{variant.name}.log"
    db_path = args.tmp_dir / variant.db_name
    log_file.write_text("", encoding="utf-8")

    start_health = wait_for_ready(args, log_file, variant.name, previous_finished_at)
    status = "ok"
    notes = f"start {start_health.format()}"
    attempts = 0
    max_attempts = args.duplicate_retry + 1
    cmd = build_command(args, variant, db_path)

    while attempts < max_attempts:
        attempts += 1
        remove_path(db_path)
        append_line(log_file)
        append_line(log_file, f"## Attempt {attempts}")
        append_line(log_file, f"$ {quote_cmd(cmd)}")
        append_line(log_file)

        with log_file.open("a", encoding="utf-8", errors="replace") as file:
            proc = subprocess.run(cmd, cwd=ROOT_DIR, stdout=file, stderr=subprocess.STDOUT)

        if proc.returncode == 0:
            break

        log_text = log_file.read_text(encoding="utf-8", errors="replace")
        if attempts < max_attempts and duplicate_key_failure(log_text):
            notes = "retry after duplicate-key failure"
            append_line(
                log_file,
                f"[runner] duplicate-key style failure detected, retrying {variant.name} from scratch",
            )
            continue

        status = "failed"
        tail = " ".join(log_text.splitlines()[-5:]).strip()
        notes = tail or f"exit code {proc.returncode}"
        break

    finished_at = time.monotonic()
    end_health = sample_health(args) if not args.skip_health_check else Health(None, None, 0)
    append_line(log_file, f"[runner] finished state: {end_health.format()}")

    result = summarize_log(log_file, status, notes)
    append_line(
        summary_file,
        "| {name} | {status} | {attempts} | {measure} | {tpmc} | {new_order} | "
        "{payment} | {order_status} | {delivery} | {stock_level} | {notes} | "
        "[{name}](./logs/{name}.log) |".format(
            name=variant.name,
            status=status,
            attempts=attempts,
            measure=f"{args.measure_time}s" if args.measure_time is not None else "tpcc default (720s)",
            tpmc=result["tpmc"],
            new_order=result["new_order"],
            payment=result["payment"],
            order_status=result["order_status"],
            delivery=result["delivery"],
            stock_level=result["stock_level"],
            notes=result["notes"].replace("|", "\\|"),
        ),
    )

    remove_path(db_path)
    print(log_file.read_text(encoding="utf-8", errors="replace"), end="")
    return finished_at


def write_summary_header(args: argparse.Namespace, summary_file: Path) -> None:
    measure = f"{args.measure_time}s" if args.measure_time is not None else "tpcc default (720s)"
    summary_file.write_text(
        "\n".join(
            [
                "# TPCC Stable Run Summary",
                "",
                f"- Timestamp: {args.stamp}",
                f"- Warehouses: {args.num_ware}",
                f"- TPCC max retry: {args.tpcc_max_retry}",
                f"- Duplicate-key retries per variant: {args.duplicate_retry}",
                f"- Measure time: {measure}",
                f"- Binary: `{args.binary}`",
                (
                    "- Machine gates: "
                    f"temp<={args.cool_temp_c:.1f}C, "
                    f"cpu<={args.idle_cpu_percent:.1f}%, "
                    f"min_cooldown={args.min_cooldown_sec}s, "
                    f"stable_samples={args.stable_samples}, "
                    f"sample_interval={args.sample_interval_sec}s"
                ),
                "",
                "| Variant | Status | Attempts | Measure Time | TpmC | New-Order p90 | Payment p90 | Order-Status p90 | Delivery p90 | Stock-Level p90 | Notes | Raw Log |",
                "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |",
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def main() -> int:
    args = parse_args()
    args.binary = args.binary.resolve()
    args.tmp_dir = args.tmp_dir.resolve()
    result_dir = (
        args.result_dir.resolve()
        if args.result_dir is not None
        else ROOT_DIR / "tpcc" / "results" / args.stamp
    )
    log_dir = result_dir / "logs"
    summary_file = result_dir / "summary.md"
    log_dir.mkdir(parents=True, exist_ok=True)
    args.tmp_dir.mkdir(parents=True, exist_ok=True)

    if args.build:
        subprocess.run(["cargo", "build", "-p", "tpcc", "--release"], cwd=ROOT_DIR, check=True)
    if not args.binary.exists() or not os.access(args.binary, os.X_OK):
        print(
            f"missing executable binary: {args.binary}\n"
            "build it first with: cargo build -p tpcc --release",
            file=sys.stderr,
        )
        return 1

    write_summary_header(args, summary_file)

    previous_finished_at: float | None = None
    try:
        for variant in VARIANTS:
            previous_finished_at = run_variant(
                args, variant, log_dir, summary_file, previous_finished_at
            )
    finally:
        try:
            args.tmp_dir.rmdir()
        except OSError:
            pass

    print(f"\nGenerated summary: {summary_file}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
