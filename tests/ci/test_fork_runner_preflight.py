"""Behavior contracts for the conservative offline fork runner preflight."""
import pytest

from scripts.ci.fork_runner_preflight import (
    UNKNOWN,
    evaluate,
    inventory,
    load,
    matrix_rows,
    pr_trigger,
    resolve,
)


def test_workflow_yaml_preserves_event_keys_and_boolean_inputs():
    workflow = load("on:\n  workflow_call:\n    inputs:\n      enabled:\n        type: boolean\n        default: false\njobs: {}\n")
    assert workflow["on"]["workflow_call"]["inputs"]["enabled"]["default"] is False
    assert True not in workflow
    assert load("on: {pull_request: null}\njobs: {}\n")["on"] == {"pull_request": None}


@pytest.mark.parametrize("repository,label", [
    ("Kane808-AI/hermes-agent", "windows-11-arm"),
    ("another/fork", "windows-11-arm"),
    ("NousResearch/hermes-agent", "windows-latest-32-arm-core"),
])
def test_repository_routing_preserves_native_arm(repository, label):
    expression = "${{ github.repository == 'NousResearch/hermes-agent' && 'windows-latest-32-arm-core' || 'windows-11-arm' }}"
    assert resolve(expression, {"github.repository": repository}) == label


def test_unknown_is_not_permission_to_skip_or_choose_a_runner():
    assert evaluate("needs.detect.outputs.python == 'true'", {}) is UNKNOWN
    assert evaluate("false && unknown.context", {}) is False
    assert evaluate("unknown.context || true", {}) is UNKNOWN
    with pytest.raises(ValueError, match="unresolved"):
        resolve("${{ needs.plan.outputs.runner }}", {})


def test_include_only_matrix_keeps_every_cell_separate():
    rows = [{"runner": "macos-latest", "slice": "1/2"},
            {"runner": "macos-latest", "slice": "2/2"},
            {"runner": "windows-2025"}, {"runner": "windows-11-arm"}]
    assert matrix_rows({"include": rows}, {}, []) == rows
    assert matrix_rows({"slice": [1, 2]}, {}, []) == [{"slice": 1}, {"slice": 2}]


def test_nested_unknown_input_is_included_and_paid_label_rejected(tmp_path):
    workflows = {
        "root.yml": {"on": {}, "jobs": {"child": {
            "uses": "./.github/workflows/child.yml",
            "with": {"enabled": "${{ needs.detect.outputs.enabled == 'true' }}"},
        }}},
        "child.yml": {"on": {"workflow_call": {"inputs": {
            "enabled": {"type": "boolean", "default": False},
        }}}, "jobs": {"test": {"if": "inputs.enabled", "runs-on": "paid-pool", "timeout-minutes": 10}}},
    }
    with pytest.raises(ValueError, match="nonstandard runner"):
        inventory(tmp_path, workflows, ["root.yml"], {}, [])
    workflows["child.yml"]["jobs"]["test"]["runs-on"] = "windows-11-arm"
    jobs, skipped, _ = inventory(tmp_path, workflows, ["root.yml"], {}, [])
    assert [job["runner"] for job in jobs] == ["windows-11-arm"]
    assert not skipped


def test_missing_matrix_timeout_uses_bounded_default():
    assert resolve("${{ matrix.timeout || 60 }}", {}) == 60
    assert resolve("${{ matrix.timeout || 60 }}", {"matrix.timeout": 90}) == 90


def test_event_filters_do_not_admit_label_reruns_or_unrelated_bundles():
    assert not pr_trigger({"types": ["labeled"]}, [".github/workflows/tests.yml"], "opened")
    assert not pr_trigger({"paths": ["pm/**", "!pm/**/*.md"]}, ["scripts/ci/probe.py"], "opened")
    assert pr_trigger({"paths": ["pm/**", "!pm/**/*.md"]}, ["pm/core.py"], "synchronize")
