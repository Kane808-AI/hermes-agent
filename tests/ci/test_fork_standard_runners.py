"""Evaluate fork runner selection without contacting Actions or provisioning runners."""

from pathlib import Path

import hermes_yaml as yaml
import pytest

from tests.ci import _gha_expr as gha

ROOT = Path(__file__).resolve().parents[2]
FORK = "Kane808-AI/hermes-agent"
UPSTREAM = "NousResearch/hermes-agent"


def workflow(name):
    return yaml.safe_load((ROOT / ".github/workflows" / name).read_text())


def context(repository, matrix=None):
    return {"github": {"repository": repository}, "matrix": matrix or {},
            "runner": {"arch": "ARM64"}}


@pytest.mark.parametrize("filename,job", [
    ("tests.yml", "test"), ("js-tests.yml", "check"),
    ("rust-tests.yml", "bootstrap-installer"), ("nix.yml", "flake-check"),
])
def test_linux_runner_selects_standard_only_for_this_fork(filename, job):
    choice = workflow(filename)["jobs"][job]["runs-on"]
    assert gha.render(choice, context(FORK)) == "ubuntu-latest"
    assert gha.render(choice, context(UPSTREAM)) == "ubuntu-latest-32-core"


def test_os_matrix_keeps_native_windows_arm_and_macos_slices():
    job = workflow("tests-os.yml")["jobs"]["os-tests"]
    choices = []
    for row in job["strategy"]["matrix"]["include"]:
        choices.append(gha.render(job["runs-on"], context(FORK, row)))
        assert gha.render(job["runs-on"], context(UPSTREAM, row)) == row["runner"]
    assert choices.count("macos-latest") == 2
    assert "windows-latest" in choices
    assert "windows-11-arm" in choices
    assert not any("32-" in runner for runner in choices)


def test_fork_python_parallelism_fits_standard_runners():
    for filename, jobname in [("tests.yml", "test"), ("tests-os.yml", "os-tests")]:
        job = workflow(filename)["jobs"][jobname]
        step = next(s for s in job["steps"] if "HERMES_TEST_WORKERS" in s.get("env", {}))
        workers = step["env"]["HERMES_TEST_WORKERS"]
        assert 1 <= int(gha.render(workers, context(FORK, {"marker": "windows"}))) <= 4


def test_js_fork_concurrency_preserves_discovered_checks():
    job = workflow("js-tests.yml")["jobs"]["check"]
    step = next(s for s in job["steps"] if s.get("name") == "Run all workspace checks")
    command = gha.render(step["run"], context(FORK))
    assert command.strip() == "node .github/scripts/run-workspace-checks.mjs --concurrency 1"
    assert gha.render(step["run"], context(UPSTREAM)).strip() == "node .github/scripts/run-workspace-checks.mjs"
