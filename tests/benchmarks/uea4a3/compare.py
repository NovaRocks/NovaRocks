#!/usr/bin/env python3
"""Derive the UEA-4A-3 comparison from the raw receipts, failing closed.

Every statistic is recomputed from each query's submit and terminal time; the
summaries the scenario wrote are not trusted. A run is invalid when its
provenance does not match (manifest, binaries, fixture, topology, results);
an invalid run evaluates no gate. B0 noise beyond its bound is reported as an
unstable environment and removes no sample.
"""

from __future__ import annotations

import collections
import hashlib
import json
import math
from pathlib import Path
import statistics
import sys

SCENARIO = "uea4/scan-producer-performance"
SCENARIO_DIR = "uea4-scan-producer-performance"
RECEIPT = "uea4a3-performance.json"
SIDES = ("b0", "candidate")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def nearest_rank(values: list[float], quantile: float) -> float | None:
    """Nearest-rank quantile: the ceil(p * n)-th smallest value."""
    if not values:
        return None
    ordered = sorted(values)
    rank = math.ceil(quantile * len(ordered))
    return ordered[min(max(rank, 1), len(ordered)) - 1]


def median(values: list[float]) -> float | None:
    present = [value for value in values if value is not None]
    return statistics.median(present) if present else None


def noise(values: list[float]) -> float | None:
    """The largest relative distance of one repetition from their median."""
    center = median(values)
    if center is None or center == 0 or any(value is None for value in values):
        return None
    return max(abs(value / center - 1) for value in values)


def window_stats(samples: list[dict]) -> dict:
    succeeded = [sample for sample in samples if sample["error"] is None]
    latencies = [(sample["finished_micros"] - sample["submitted_micros"]) / 1000
                 for sample in succeeded]
    span_micros = (max(sample["finished_micros"] for sample in samples)
                   - min(sample["submitted_micros"] for sample in samples)) if samples else 0
    return {
        "queries": len(samples),
        "successes": len(succeeded),
        "errors": len(samples) - len(succeeded),
        "span_seconds": span_micros / 1e6,
        "throughput_qps": len(succeeded) / (span_micros / 1e6) if span_micros else 0.0,
        "p50_ms": nearest_rank(latencies, 0.50),
        "p95_ms": nearest_rank(latencies, 0.95),
        "p99_ms": nearest_rank(latencies, 0.99),
    }


def workload_view(workload: dict) -> dict:
    windows = [window_stats(window["samples"]) for window in workload["windows"]]
    counts = collections.Counter(sample["result_digest"] for window in workload["windows"]
                                 for sample in window["samples"] if sample["result_digest"])
    previews: dict[str, str] = {}
    for window in workload["windows"]:
        for sample in window["samples"]:
            if sample["result_digest"] and sample.get("result_preview"):
                previews.setdefault(sample["result_digest"], sample["result_preview"])
    digests = sorted(counts)
    errors = [sample["error"] for window in workload["windows"]
              for sample in window["samples"] if sample["error"]]
    resources: dict[str, dict] = {}
    for window in workload["windows"]:
        for role in window["resources"]:
            entry = resources.setdefault(role["role"], {"peak_rss_bytes": [], "peak_threads": [],
                                                        "cpu_seconds": []})
            entry["peak_rss_bytes"].append(role["peak_rss_bytes"])
            entry["peak_threads"].append(role.get("peak_threads"))
            entry["cpu_seconds"].append(None if role["cpu_millis"] is None
                                        else role["cpu_millis"] / 1000)
    return {
        "gated": workload["gated"],
        "windows": windows,
        "throughput_qps": median([window["throughput_qps"] for window in windows]),
        "p50_ms": median([window["p50_ms"] for window in windows]),
        "p95_ms": median([window["p95_ms"] for window in windows]),
        "p99_ms": median([window["p99_ms"] for window in windows]),
        "throughput_noise": noise([window["throughput_qps"] for window in windows]),
        "p95_noise": noise([window["p95_ms"] for window in windows]),
        "measured_errors": errors,
        "warmup_errors": workload["warmup"]["errors"],
        "result_digests": digests,
        "result_counts": dict(counts),
        "result_previews": previews,
        "resources": {role: {"peak_rss_bytes": median(values["peak_rss_bytes"]),
                             "peak_threads": median(values["peak_threads"]),
                             "cpu_seconds": median(values["cpu_seconds"])}
                      for role, values in sorted(resources.items())},
        "backends": [window["backends"] for window in workload["windows"]],
    }


def backend_signals(views: list[list[dict]]) -> list[dict]:
    """Per backend, never averaged across backends: each window's dispatch
    quantile bounds and scan pending deltas, as recorded."""
    by_backend: dict[int, dict] = {}
    for window in views:
        for backend in window:
            entry = by_backend.setdefault(backend["backend"], {"dispatch": [], "scan_pending": []})
            entry["dispatch"].append(backend["dispatch"])
            entry["scan_pending"].append(backend["scan_pending"])
    return [{"backend": index, **entry} for index, entry in sorted(by_backend.items())]


def load(output: Path, manifest_path: Path) -> tuple[dict, dict, dict, list[str]]:
    invalid: list[str] = []
    hashes = json.loads((output / "input-hashes.json").read_text())
    manifest = json.loads(manifest_path.read_text())
    manifest_sha = sha256(manifest_path)
    if hashes["manifest_sha256"] != manifest_sha:
        invalid.append("the manifest changed after the run started")
    runs: dict[tuple[str, str], dict] = {}
    for group in manifest["config_groups"]:
        for side in SIDES:
            root = output / group["name"] / side / SCENARIO_DIR
            receipt = json.loads((root / RECEIPT).read_text())
            evidence = json.loads((root / "scenario-evidence.json").read_text())
            name = f"{group['name']}/{side}"
            if (evidence.get("scenario") != SCENARIO or evidence.get("outcome") != "passed"
                    or evidence.get("cluster_size") != 3
                    or evidence.get("launch_profile") != "performance"):
                invalid.append(f"{name} did not finish a native 1FE+3BE performance run")
            if (receipt["side"] != side or receipt["group"] != group["name"]
                    or receipt["manifest_sha256"] != manifest_sha):
                invalid.append(f"{name} receipt names another side, group or manifest")
            if receipt["binary_sha256"] != hashes["binary_sha256_by_side"][side]:
                invalid.append(f"{name} ran a binary other than the recorded {side} binary")
            if receipt["driver_workers"] != group["driver_workers"]:
                invalid.append(f"{name} ran with other driver workers than its group")
            runs[(group["name"], side)] = {"receipt": receipt, "evidence": evidence}
    fixtures = {run["receipt"]["fixture_sha256"] for run in runs.values()}
    if len(fixtures) != 1:
        invalid.append("the runs read different Iceberg inputs: " + ", ".join(sorted(fixtures)))
    for group in manifest["config_groups"]:
        semantics = {side: runs[(group["name"], side)]["evidence"]
                     .get("effective_launch_config_semantics_sha256") for side in SIDES}
        if len(set(semantics.values())) != 1:
            invalid.append(f"{group['name']}: the two sides launched different effective configs")
    return manifest, hashes, runs, invalid


def compare_group(manifest: dict, group: dict, runs: dict, invalid: list[str]) -> dict:
    gates = manifest["gates"]
    rows = []
    for name in group["workloads"]:
        sides = {}
        for side in SIDES:
            workload = next(item for item in runs[(group["name"], side)]["receipt"]["workloads"]
                            if item["name"] == name)
            sides[side] = workload_view(workload)
        b0, candidate = sides["b0"], sides["candidate"]
        label = f"{group['name']}/{name}"
        # B0's majority result is the reference. A B0 minority result is a
        # baseline defect, reported with its values; it removes no sample.
        reference = None
        if b0["result_counts"]:
            ranked = sorted(b0["result_counts"].items(), key=lambda item: (-item[1], item[0]))
            if len(ranked) > 1 and ranked[0][1] == ranked[1][1]:
                invalid.append(f"{label}: B0 has no majority result")
            reference = ranked[0][0]
        else:
            invalid.append(f"{label}: B0 returned no result")
        b0_divergent = {digest: {"count": count, "preview": b0["result_previews"].get(digest)}
                        for digest, count in b0["result_counts"].items() if digest != reference}
        if b0["gated"] and (b0["measured_errors"] or b0["warmup_errors"]):
            invalid.append(f"{label}: B0 had errors, so it is no baseline")
        checks = []
        candidate_divergent = {digest: {"count": count,
                                        "preview": candidate["result_previews"].get(digest)}
                               for digest, count in candidate["result_counts"].items()
                               if digest != reference}
        if candidate_divergent or not candidate["result_counts"]:
            checks.append({"check": "result", "passed": False,
                           "detail": {"reference": reference,
                                      "reference_preview": b0["result_previews"].get(reference),
                                      "candidate_divergent": candidate_divergent}})
        if b0["gated"]:
            errors = len(candidate["measured_errors"]) + candidate["warmup_errors"]
            checks.append({"check": "errors", "passed": errors == 0, "value": errors})
            if b0["throughput_qps"] and candidate["throughput_qps"] is not None:
                regression = 1 - candidate["throughput_qps"] / b0["throughput_qps"]
                checks.append({"check": "throughput_regression", "value": regression,
                               "limit": gates["throughput_regression_max"],
                               "passed": regression <= gates["throughput_regression_max"]})
            else:
                checks.append({"check": "throughput_regression", "passed": False,
                               "detail": "no throughput on one side"})
            if b0["p95_ms"] and candidate["p95_ms"] is not None:
                growth = candidate["p95_ms"] / b0["p95_ms"] - 1
                checks.append({"check": "p95_growth", "value": growth,
                               "limit": gates["p95_growth_max"],
                               "passed": growth <= gates["p95_growth_max"]})
            else:
                checks.append({"check": "p95_growth", "passed": False,
                               "detail": "no p95 on one side"})
        unstable = [kind for kind, value, limit in (
            ("throughput", b0["throughput_noise"], gates["b0_throughput_noise_max"]),
            ("p95", b0["p95_noise"], gates["b0_p95_noise_max"]))
            if value is None or value > limit]
        rows.append({
            "workload": name, "gated": b0["gated"], "b0": b0, "candidate": candidate,
            "reference_result": reference, "b0_divergent_results": b0_divergent,
            "candidate_backends": backend_signals(candidate.pop("backends")),
            "b0_backends": backend_signals(b0.pop("backends")),
            "checks": checks, "b0_unstable": unstable,
        })
    result = {"group": group["name"], "driver_workers": group["driver_workers"], "workloads": rows}
    if group["control"]:
        result["control"] = compare_control(manifest, group, runs)
    return result


def control_view(samples: dict) -> dict:
    return {
        "short_scan_p99_ms": nearest_rank(samples["short_scan_ms"], 0.99),
        "short_scan_samples": len(samples["short_scan_ms"]),
        "short_scan_errors": samples["short_scan_errors"],
        "kill_delivery_p99_ms": nearest_rank(samples["kill_delivery_ms"], 0.99),
        "kill_ack_p99_ms": nearest_rank(samples["kill_ack_ms"], 0.99),
        "kill_samples": len(samples["kill_delivery_ms"]),
        "kill_invalid": samples["kill_invalid"],
    }


def compare_control(manifest: dict, group: dict, runs: dict) -> dict:
    gates = manifest["gates"]
    control = manifest["control"]
    sides = {}
    for side in SIDES:
        report = runs[(group["name"], side)]["receipt"]["control"]
        sides[side] = {
            "kill_query_uncancelled_ms": report["kill_query_uncancelled_ms"],
            "idle": control_view(report["idle"]),
            "loaded": control_view(report["loaded"]),
            "background": window_stats(report["background_samples"]),
            "heartbeat": report["heartbeat"],
            "runtime_filter": report["runtime_filter"],
        }
    candidate = sides["candidate"]
    checks = []
    for kind, key, expected in (("short_scan", "short_scan_p99_ms", control["short_query_samples"]),
                                ("kill", "kill_delivery_p99_ms", control["kill_samples"])):
        idle, loaded = candidate["idle"][key], candidate["loaded"][key]
        complete = (candidate["loaded"][f"{kind}_samples"] == expected
                    and candidate["idle"][f"{kind}_samples"] == expected)
        checks.append({"check": f"{kind}_samples_complete", "passed": complete})
        if loaded is None or idle is None:
            checks.append({"check": f"{kind}_p99", "passed": False, "detail": "no samples"})
            continue
        checks.append({"check": f"{kind}_p99_absolute", "value": loaded,
                       "limit": gates["control_p99_max_ms"],
                       "passed": loaded <= gates["control_p99_max_ms"]})
        ratio = loaded / idle if idle else math.inf
        checks.append({"check": f"{kind}_p99_ratio_to_idle", "value": ratio,
                       "limit": gates["control_p99_max_ratio_to_idle"],
                       "passed": ratio <= gates["control_p99_max_ratio_to_idle"]})
    return {"b0": sides["b0"], "candidate": candidate, "checks": checks,
            "background_clients": control["background_clients"],
            "background_workload": control["background_workload"]}


def percent(value: float | None) -> str:
    return "—" if value is None else f"{value * 100:+.1f}%"


def number(value: float | None, digits: int = 1) -> str:
    return "—" if value is None else f"{value:.{digits}f}"


def render_markdown(result: dict) -> str:
    lines = [
        "# UEA-4A-3 集中性能对照",
        "",
        f"- 结论：**{result['verdict']}**",
        f"- manifest sha256：`{result['provenance']['manifest_sha256']}`",
        f"- B0：`{result['provenance']['b0_revision']}`；candidate：`{result['provenance']['candidate_revision']}`",
        f"- 顺序：{'，'.join('/'.join(pair) for pair in result['provenance']['order'])}",
        "",
    ]
    if result["invalid"]:
        lines += ["## 作废原因", ""] + [f"- {reason}" for reason in result["invalid"]] + [""]
    for group in result["groups"]:
        workers = group["driver_workers"] if group["driver_workers"] is not None else "默认（CPU 核数）"
        lines += [f"## 配置组 `{group['group']}`（driver worker：{workers}）", "",
                  "| 负载 | 门 | B0 吞吐 qps | cand 吞吐 qps | 吞吐变化 | B0 p95 ms | cand p95 ms | p95 变化 | cand p99 ms | 错误 | 结果一致 | B0 异常结果 | B0 噪声 |",
                  "|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
        for row in group["workloads"]:
            b0, candidate = row["b0"], row["candidate"]
            change = (None if not b0["throughput_qps"] or candidate["throughput_qps"] is None
                      else candidate["throughput_qps"] / b0["throughput_qps"] - 1)
            growth = (None if not b0["p95_ms"] or candidate["p95_ms"] is None
                      else candidate["p95_ms"] / b0["p95_ms"] - 1)
            failed = [check["check"] for check in row["checks"] if not check["passed"]]
            gate = ("不设门" if not row["gated"] else ("通过" if not failed else "未通过：" + "、".join(failed)))
            same = "是" if not any(check["check"] == "result" for check in row["checks"]) else "否"
            b0_wrong = sum(item["count"] for item in row["b0_divergent_results"].values())
            errors = len(candidate["measured_errors"]) + candidate["warmup_errors"]
            unstable = "、".join(row["b0_unstable"]) if row["b0_unstable"] else "稳定"
            lines.append(
                f"| {row['workload']} | {gate} | {number(b0['throughput_qps'], 3)} | {number(candidate['throughput_qps'], 3)} "
                f"| {percent(change)} | {number(b0['p95_ms'])} | {number(candidate['p95_ms'])} | {percent(growth)} "
                f"| {number(candidate['p99_ms'])} | {errors} | {same} | {b0_wrong} | {unstable} |")
        for row in group["workloads"]:
            for side, divergent in (("B0", row["b0_divergent_results"]),
                                    ("candidate", next((check["detail"]["candidate_divergent"]
                                                        for check in row["checks"]
                                                        if check["check"] == "result"), {}))):
                for digest, item in divergent.items():
                    lines.append(f"- {row['workload']}：{side} 返回了与参照不同的结果 `{digest}` × {item['count']}："
                                 f"`{item['preview']}`（参照 `{row['reference_result']}`："
                                 f"`{row['b0']['result_previews'].get(row['reference_result'])}`）")
        lines += ["", "资源（三轮中位数；RSS 与线程数为窗口峰值，CPU 为窗口内秒数）：", "",
                  "| 负载 | 角色 | B0 RSS MiB | cand RSS MiB | B0 线程 | cand 线程 | B0 CPU s | cand CPU s |",
                  "|---|---|---|---|---|---|---|---|"]
        for row in group["workloads"]:
            for role, b0_role in row["b0"]["resources"].items():
                candidate_role = row["candidate"]["resources"].get(role, {})
                mib = lambda value: None if value is None else value / (1024 * 1024)
                lines.append(
                    f"| {row['workload']} | {role} | {number(mib(b0_role['peak_rss_bytes']), 0)} "
                    f"| {number(mib(candidate_role.get('peak_rss_bytes')), 0)} "
                    f"| {number(b0_role.get('peak_threads'), 0)} | {number(candidate_role.get('peak_threads'), 0)} "
                    f"| {number(b0_role['cpu_seconds'])} | {number(candidate_role.get('cpu_seconds'))} |")
        lines += ["", "candidate 各 BE 的 scan 流 Pending 次数（三轮合计，budget_yield / wait）与派发时延 p99 上界（µs，按轮）：", ""]
        for row in group["workloads"]:
            cells = []
            for backend in row["candidate_backends"]:
                pending = [window or {} for window in backend["scan_pending"]]
                yields = sum(window.get("budget_yield", 0) for window in pending)
                waits = sum(window.get("wait", 0) for window in pending)
                enqueue = []
                for window in backend["dispatch"]:
                    transition = next((item for item in (window or [])
                                       if item["transition"] == "enqueue_to_worker"), None)
                    enqueue.append(number(transition["p99_micros"], 0) if transition else "—")
                cells.append(f"BE{backend['backend']}: {int(yields)}/{int(waits)}，enqueue→worker p99 {'/'.join(enqueue)}")
            lines.append(f"- {row['workload']}：" + "；".join(cells))
        control = group.get("control")
        if control:
            lines += ["", f"### 控制面（满载 = {control['background_clients']} 客户端 "
                      f"{control['background_workload']}）", "",
                      "| 侧 | KILL 查询不取消耗时 ms | 空闲短扫描 p99 | 满载短扫描 p99 | 空闲 KILL p99 | 满载 KILL p99 | 满载 KILL 往返 p99 | 无效样本 |",
                      "|---|---|---|---|---|---|---|---|"]
            for side in SIDES:
                view = control[side]
                invalid_samples = len(view["idle"]["kill_invalid"]) + len(view["loaded"]["kill_invalid"]) \
                    + len(view["idle"]["short_scan_errors"]) + len(view["loaded"]["short_scan_errors"])
                lines.append(
                    f"| {side} | {number(view['kill_query_uncancelled_ms'], 0)} "
                    f"| {number(view['idle']['short_scan_p99_ms'])} | {number(view['loaded']['short_scan_p99_ms'])} "
                    f"| {number(view['idle']['kill_delivery_p99_ms'])} | {number(view['loaded']['kill_delivery_p99_ms'])} "
                    f"| {number(view['loaded']['kill_ack_p99_ms'])} | {invalid_samples} |")
            failed = [check["check"] for check in control["checks"] if not check["passed"]]
            lines += ["", "控制面门限：" + ("通过" if not failed else "未通过：" + "、".join(failed)),
                      f"heartbeat：{control['candidate']['heartbeat']}；RF：{control['candidate']['runtime_filter']}"]
        lines.append("")
    return "\n".join(lines) + "\n"


def compare(output: Path, manifest_path: Path) -> dict:
    output = Path(output)
    manifest, hashes, runs, invalid = load(output, Path(manifest_path))
    groups = [compare_group(manifest, group, runs, invalid) for group in manifest["config_groups"]]
    checks = [check for group in groups for row in group["workloads"] for check in row["checks"]]
    checks += [check for group in groups for check in group.get("control", {}).get("checks", [])]
    failed = [check for check in checks if not check["passed"]]
    if hashes.get("smoke"):
        verdict = "冒烟（不评估门限）"
    elif invalid:
        verdict = "作废"
    else:
        verdict = "通过" if not failed else "未通过"
    result = {
        "provenance": hashes,
        "invalid": invalid,
        "groups": groups,
        "verdict": verdict,
        "passed": not hashes.get("smoke") and not invalid and not failed,
        "verdict_line": f"UEA-4A-3 comparison: {verdict}; {len(failed)} of {len(checks)} checks failed; "
                        f"{len(invalid)} validity problems",
    }
    (output / "comparison.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    (output / "comparison.md").write_text(render_markdown(result))
    return result


def main() -> int:
    if len(sys.argv) not in (2, 3):
        print("usage: compare.py <output> [<manifest>]", file=sys.stderr)
        return 2
    output = Path(sys.argv[1])
    manifest = (Path(sys.argv[2]) if len(sys.argv) == 3
                else Path(json.loads((output / "input-hashes.json").read_text())["manifest_path"]))
    result = compare(output, manifest)
    print(result["verdict_line"])
    return 0 if result["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
