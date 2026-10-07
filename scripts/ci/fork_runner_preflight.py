#!/usr/bin/env python3
"""Offline, fail-closed runner inventory for one fork PR candidate (not release approval).

Run from the candidate checkout with --base and --disabled-workflow from fresh
GET-only workflow metadata. Unknown conditions are INCLUDED, unknown runner/matrix
expressions fail. No GitHub writes or hosted jobs. See fork-runner-preflight.md.
"""
from __future__ import annotations

import argparse
import fnmatch
import itertools
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

from ruamel.yaml import YAML

UNKNOWN = object()
STANDARD = {
    "ubuntu-latest", "ubuntu-24.04", "ubuntu-22.04", "ubuntu-24.04-arm",
    "windows-latest", "windows-2025", "windows-11-arm",
    "macos-latest", "macos-15", "macos-15-intel", "macos-14",
}
TOKEN = re.compile(r"\s*('(?:[^']|'')*'|&&|\|\||==|!=|[!(),]|[\w.-]+)")


def truth(value):
    return UNKNOWN if value is UNKNOWN else bool(value)


def combine(op, left, right):
    if op in ("==", "!="):
        if left is UNKNOWN or right is UNKNOWN:
            return UNKNOWN
        return (left == right) if op == "==" else (left != right)
    a, b = truth(left), truth(right)
    if op == "&&":
        if a is False or b is False:
            return False
        return right if a is True else UNKNOWN
    if a is True:
        return left
    return right if a is False else UNKNOWN


class Expression:
    """Small expression subset; unsupported syntax never proves a job skipped."""

    def __init__(self, text, context):
        self.tokens = []
        while text.strip():
            match = TOKEN.match(text)
            if not match:
                raise ValueError(f"unsupported expression: {text}")
            self.tokens.append(match[1])
            text = text[match.end():]
        self.pos = 0
        self.context = context

    def peek(self):
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def take(self):
        token = self.peek()
        self.pos += 1
        return token

    def atom(self):
        token = self.take()
        if token == "!":
            value = truth(self.atom())
            return UNKNOWN if value is UNKNOWN else not value
        if token == "(":
            value = self.parse()
            if self.take() != ")":
                raise ValueError("unclosed expression")
            return value
        if token is None:
            raise ValueError("missing operand")
        if self.peek() == "(":
            self.take()
            if self.take() != ")":
                raise ValueError("function arguments require manual review")
            return {"always": True, "cancelled": False}.get(token, UNKNOWN)
        if token.startswith("'"):
            return token[1:-1].replace("''", "'")
        if token in ("true", "false"):
            return token == "true"
        if token.isdigit():
            return int(token)
        # Missing properties on this fully expanded matrix are empty in Actions.
        return self.context.get(token, "" if token.startswith("matrix.") else UNKNOWN)

    def parse(self, level=0):
        operators = [("||",), ("&&",), ("==", "!=")]
        if level == len(operators):
            return self.atom()
        value = self.parse(level + 1)
        while self.peek() in operators[level]:
            op = self.take()
            value = combine(op, value, self.parse(level + 1))
        return value


def evaluate(text, context):
    if not isinstance(text, str):
        return text
    text = text.strip().removeprefix("${{").removesuffix("}}").strip()
    try:
        parser = Expression(text, context)
        result = parser.parse()
        return result if parser.peek() is None else UNKNOWN
    except ValueError:
        return UNKNOWN


def resolve(value, context):
    if isinstance(value, str) and "${{" in value:
        result = evaluate(value, context)
        if result is UNKNOWN:
            raise ValueError(f"unresolved runner/matrix: {value}")
        return result
    return value


def load(text):
    # Actions uses YAML 1.2: unquoted 'on' stays a string, true/false are booleans.
    parser = YAML(typ="safe")
    parser.version = (1, 2)
    data = parser.load(text)
    data.setdefault("on", {})
    return data


def matches(paths, patterns):
    for path in paths:
        selected = False
        for pattern in patterns:
            negative = pattern.startswith("!")
            if fnmatch.fnmatchcase(path, pattern.lstrip("!")):
                selected = not negative
        if selected:
            return True
    return False


def pr_trigger(config, paths, action):
    config = config or {}
    if action not in config.get("types", ["opened", "synchronize", "reopened"]):
        return False
    if "branches" in config or "branches-ignore" in config or "paths-ignore" in config:
        raise ValueError("unhandled PR filter; manual event audit required")
    return "paths" not in config or matches(paths, config["paths"])


def matrix_rows(matrix, context, shards):
    matrix = matrix or {}
    if "exclude" in matrix:
        raise ValueError("matrix exclusion requires explicit review")
    axes = {key: value for key, value in matrix.items() if key != "include"}
    if not axes:
        return [row.copy() for row in matrix.get("include", [])] or [{}]
    for key, value in axes.items():
        if value == "${{ fromJSON(needs.e2e-upgrade-plan.outputs.shards) }}":
            axes[key] = shards
        elif not isinstance(value, list):
            raise ValueError(f"unresolved matrix axis {key}: {value}")
    rows = [dict(zip(axes, values)) for values in itertools.product(*axes.values())] if axes else []
    for include in matrix.get("include", []):
        compatible = [row for row in rows if all(key not in axes or row[key] == value for key, value in include.items())]
        if compatible:
            for row in compatible:
                row.update(include)
        else:
            rows.append(include.copy())
    return rows or [{}]


def upgrade_shards(root, workflow):
    job = workflow["jobs"]["e2e-upgrade-plan"]
    script = next(step["run"] for step in job["steps"] if step.get("id") == "plan")
    with tempfile.TemporaryDirectory(dir=root / ".preflight-tmp") as directory:
        output = Path(directory) / "output"
        subprocess.run(["bash", "-eu", "-c", script], cwd=root,
                       env={**os.environ, "GITHUB_OUTPUT": str(output)}, check=True, capture_output=True, timeout=30)
        shards = json.loads(output.read_text().split("shards=", 1)[1])
    suite = root / "tests/e2e/core/upgrade"
    selected = [path for shard in shards for path in
                (suite.glob("test_*.py") if shard == "core" else (suite / shard).rglob("test_*.py"))]
    expected = set(suite.rglob("test_*.py"))
    if not selected or len(selected) != len(set(selected)) or set(selected) != expected:
        raise ValueError("upgrade matrix omits or duplicates test files")
    return shards, len(selected)


def inventory(root, workflows, roots, context, shards):
    jobs, skipped, seen = [], [], set()

    def walk(name, supplied=None):
        if name in seen:
            return
        seen.add(name)
        workflow = workflows[name]
        local = dict(context)
        if "workflow_run" in workflow["on"]:
            local["github.event_name"] = "workflow_run"
        for key, spec in (workflow["on"].get("workflow_call") or {}).get("inputs", {}).items():
            local[f"inputs.{key}"] = spec.get("default", False if spec["type"] == "boolean" else "")
        for key, value in (supplied or {}).items():
            local[f"inputs.{key}"] = value
        if name == "docker.yml":
            # Execute the actual mode resolver with the PR's empty release input.
            script = workflow["jobs"]["mode"]["steps"][0]["run"]
            with tempfile.TemporaryDirectory(dir=root / ".preflight-tmp") as directory:
                output = Path(directory) / "output"
                subprocess.run(["bash", "-eu", "-c", script], cwd=root, check=True, timeout=30,
                               env={**os.environ, "PHASE": "", "GITHUB_OUTPUT": str(output)})
                for line in output.read_text().splitlines():
                    key, value = line.split("=", 1)
                    local[f"needs.mode.outputs.{key}"] = value
                local["needs.mode.result"] = "success"
        for key, job in workflow["jobs"].items():
            identity = f"{name}/{key}"
            if truth(evaluate(job.get("if", True), local)) is False:
                skipped.append(identity)
                continue
            if "uses" in job:
                prefix = "./.github/workflows/"
                if not job["uses"].startswith(prefix):
                    raise ValueError(f"external reusable workflow: {identity}")
                supplied = {key: evaluate(value, local) if isinstance(value, str) and "${{" in value else value
                            for key, value in job.get("with", {}).items()}
                walk(job["uses"][len(prefix):], supplied)
                continue
            for row in matrix_rows(job.get("strategy", {}).get("matrix"), local, shards):
                cell = dict(local)
                for field, value in row.items():
                    cell[f"matrix.{field}"] = resolve(value, local)
                label = resolve(job["runs-on"], cell)
                if label not in STANDARD:
                    raise ValueError(f"nonstandard runner: {identity}: {label}")
                timeout = resolve(job.get("timeout-minutes"), cell)
                if not isinstance(timeout, int) or not 0 < timeout <= 180:
                    raise ValueError(f"unbounded timeout: {identity}: {timeout}")
                jobs.append({"job": identity, "matrix": row, "runner": label, "timeout": timeout})
    for name in roots:
        walk(name)
    return jobs, skipped, seen


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--repository", default="Kane808-AI/hermes-agent")
    parser.add_argument("--disabled-workflow", action="append", default=[])
    parser.add_argument("--action", choices=["opened", "synchronize", "reopened"], default="opened")
    args = parser.parse_args()
    root = Path.cwd()
    (root / ".preflight-tmp").mkdir(exist_ok=True)
    paths = subprocess.check_output(["git", "diff", "--name-only", args.base], text=True, timeout=30).splitlines()
    untracked = subprocess.check_output(["git", "ls-files", "--others", "--exclude-standard"], text=True, timeout=30).splitlines()
    paths += [path for path in untracked if not path.startswith((".preflight-tmp/", ".local-tools/"))]
    if not paths:
        raise ValueError("no candidate diff")
    workflows = {path.name: load(path.read_text()) for path in (root / ".github/workflows").glob("*.y*ml")}
    roots, excluded = [], []
    for name, workflow in workflows.items():
        events = workflow["on"]
        if "pull_request_target" in events:
            raise ValueError(f"trusted PR-target listener requires separate audit: {name}")
        if "pull_request" not in events:
            continue
        if name in args.disabled_workflow or not pr_trigger(events["pull_request"], paths, args.action):
            excluded.append(name)
        else:
            roots.append(name)
    if "ci.yaml" not in roots:
        raise ValueError("candidate preflight must include proposed CI activation")
    # workflow_run uses DEFAULT-BRANCH source, not candidate source. Follow names
    # transitively, even when the current job's condition might later skip it.
    names = {workflows[name]["name"] for name in roots}
    listeners = []
    for _ in range(len(workflows)):
        added = False
        for name in workflows:
            trusted = load(subprocess.check_output(["git", "show", f"{args.base}:.github/workflows/{name}"], text=True, timeout=30))
            event = trusted["on"].get("workflow_run")
            if event and name not in listeners and any(fnmatch.fnmatchcase(value, pattern) for value in names for pattern in event["workflows"]):
                listeners.append(name)
                workflows[name] = trusted
                names.add(trusted["name"])
                added = True
        if not added:
            break
    shards, upgrade_files = upgrade_shards(root, workflows["tests.yml"])
    context = {"github.repository": args.repository, "github.event_name": "pull_request",
               "github.ref": "refs/pull/1/merge", "inputs.release-phase": ""}
    jobs, skipped, seen = inventory(root, workflows, roots + listeners, context, shards)
    from scripts.ci.required_results import evaluate_gate
    from scripts.run_tests_parallel import _discover_files, _slice_files
    gate = workflows["ci.yaml"]["jobs"]["all-checks-pass"]
    baseline = load(subprocess.check_output(["git", "show", f"{args.base}:.github/workflows/ci.yaml"], text=True, timeout=30))
    if gate != baseline["jobs"]["all-checks-pass"]:
        raise ValueError("aggregate policy changed; separate review required")
    for job in gate["needs"]:
        for result in ("failure", "cancelled", "", None, "unknown"):
            needs: dict[str, dict[str, object]] = {key: {"result": "success"} for key in gate["needs"]}
            needs[job]["result"] = result
            if evaluate_gate(needs)["ok"]:
                raise ValueError(f"aggregate accepted {job}={result}")
    files = _discover_files([root / "tests"])
    slices = workflows["tests.yml"]["jobs"]["test"]["strategy"]["matrix"]["slice"]
    selected = [path for index in slices for path in _slice_files(files, index, len(slices), {}, root)]
    if len(selected) != len(set(selected)) or set(selected) != set(files):
        raise ValueError("unit slices omit or duplicate files")
    print(json.dumps({"repository": args.repository, "base": args.base, "paths": sorted(paths),
                      "roots": sorted(roots), "default_branch_listeners": listeners,
                      "excluded_by_filter_or_disabled": sorted(excluded), "skipped_jobs": skipped,
                      "workflow_count": len(seen), "job_cells": len(jobs), "jobs": jobs,
                      "unit_files": len(files), "upgrade_shards": shards, "upgrade_files": upgrade_files,
                      "gate_fault_cases": len(gate["needs"]) * 5,
                      "verdict": "standard-only static candidate; NOT hosted-event authorization"}, indent=2))


if __name__ == "__main__":
    main()
