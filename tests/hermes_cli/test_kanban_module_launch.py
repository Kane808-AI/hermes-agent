"""Regression coverage for installation-pinned Kanban module workers."""

def test_module_form_keeps_workspace_off_sys_path(tmp_path):
    """A task workspace must never shadow the install tree it was spawned from.

    Workers run with cwd set to their workspace. Under a bare ``-m`` launch that
    cwd leads ``sys.path``, so a workspace that is itself a checkout of this repo
    (every git-worktree worker) supplied ``hermes_cli`` — the boot then resolved
    its install root to the workspace, found no committed dependency environment
    for that path, and exited 1 before doing any work. ``-P`` is what prevents
    it, so assert the flag AND its effect.
    """
    import os
    import subprocess
    import sys
    from pathlib import Path

    from hermes_cli import kanban_db_dispatch as kbd

    repo_root = Path(kbd.__file__).resolve().parents[1]
    assert kbd._module_hermes_argv() == [sys.executable, "-P", "-m", "hermes_cli.main"]

    # A workspace that looks like a checkout: it carries its own hermes_cli.
    workspace = tmp_path / "workspace"
    (workspace / "hermes_cli").mkdir(parents=True)
    (workspace / "hermes_cli" / "__init__.py").write_text("", encoding="utf-8")

    env = {**os.environ, "PYTHONPATH": str(repo_root)}
    env.pop("HERMES_HOME", None)
    proc = subprocess.run(
        [sys.executable, "-P", "-c", "import hermes_cli; print(hermes_cli.__file__)"],
        cwd=workspace, env=env, capture_output=True, text=True, timeout=60,
    )
    assert proc.returncode == 0, proc.stderr
    assert str(repo_root / "hermes_cli") in proc.stdout, proc.stdout


def test_module_form_pins_install_tree_on_pythonpath():
    """The module-form worker must carry the install tree on ``PYTHONPATH``.

    The pin's argv guard used to match a literal ``-m hermes_cli.main``: ``-P``
    moves that flag, so the guard silently skipped the pin and the child died
    with ``No module named 'hermes_cli'`` instead of booting.
    """
    import os
    from pathlib import Path

    from hermes_cli import kanban_db_dispatch as kbd

    repo_root = Path(kbd.__file__).resolve().parents[1]
    module_argv = kbd._module_hermes_argv()

    env: dict[str, str] = {}
    kbd._propagate_module_import_root([*module_argv, "-p", "builder", "chat", "-q", "x"], env)
    assert str(repo_root) in env.get("PYTHONPATH", "").split(os.pathsep), env

    # A resolved shim owns its own imports: nothing to pin.
    shim_env: dict[str, str] = {}
    kbd._propagate_module_import_root(["/opt/hermes/bin/hermes", "-p", "builder"], shim_env)
    assert shim_env == {}
