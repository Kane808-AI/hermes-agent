"""Execute the attribution workflow's actual shell step with local Git receipts."""

import os
from pathlib import Path
import subprocess

import hermes_yaml as yaml
import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("email,accepted", [
    ("codex@users.noreply.github.com", True),
    ("12345+some-user@users.noreply.github.com", True),
    ("SomeUser@users.noreply.github.com", True),
    ("nobody@example.com", False),
    ("+some-user@users.noreply.github.com", False),
    ("abc+some-user@users.noreply.github.com", False),
    ("some-user@users.noreply.github.com.attacker.test", False),
    ("some user@users.noreply.github.com", False),
    ("@users.noreply.github.com", False),
])
def test_actual_attribution_step_keeps_validation(tmp_path, email, accepted):
    workflow = yaml.safe_load((ROOT / ".github/workflows/contributor-check.yml").read_text())
    step = next(s for s in workflow["jobs"]["check-attribution"]["steps"] if s.get("id") == "check-emails")
    (tmp_path / "scripts/releases").mkdir(parents=True)
    (tmp_path / "scripts/releases/authors_legacy.py").write_text("")
    git_receipts = '''git() {
      if [[ "$1" == "merge-base" ]]; then printf 'base\\n';
      elif [[ "$*" == *--format=%ae* ]]; then printf '%s\\n' "$TEST_AUTHOR_EMAIL";
      else printf 'Test Author\\n'; fi
    }
'''
    env = {**os.environ, "TEST_AUTHOR_EMAIL": email, "GITHUB_OUTPUT": str(tmp_path / "output")}
    result = subprocess.run(
        ["bash", "-eo", "pipefail", "-c", git_receipts + step["run"]],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=15,
    )
    assert (result.returncode == 0) is accepted, result.stdout + result.stderr
    assert ("All contributor emails are mapped" in result.stdout) is accepted
