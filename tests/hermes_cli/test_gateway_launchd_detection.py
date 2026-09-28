"""Launchd status must resolve GUI/user ownership independently of caller context."""

import subprocess

import pytest

from hermes_cli import gateway as gw


pytestmark = pytest.mark.platforms("macos")


@pytest.mark.parametrize("domain", ["gui", "user", "legacy"])
@pytest.mark.parametrize("pid", [None, 4242])
def test_status_and_running_agree_on_domain_owned_process(
    tmp_path, monkeypatch, capsys, domain, pid
):
    plist = tmp_path / "gateway.plist"
    plist.touch()
    monkeypatch.setattr(gw.os, "getuid", lambda: 501)
    monkeypatch.setattr(gw, "supports_systemd_services", lambda: False)
    monkeypatch.setattr(gw, "get_launchd_plist_path", lambda: plist)
    monkeypatch.setattr(gw, "get_launchd_label", lambda: "ai.hermes.gateway")
    monkeypatch.setattr(gw, "launchd_plist_is_current", lambda: True)
    monkeypatch.setattr(gw, "_launchd_unsupported_marker_exists", lambda: False)
    monkeypatch.setattr("gateway.status.get_running_pid", lambda **kwargs: None)

    def launchctl(cmd, **kwargs):
        if cmd[:2] == ["launchctl", "print"]:
            loaded = cmd[2] == f"{domain}/501/ai.hermes.gateway"
            output = f"pid = {pid}\n" if pid else "state = not running\n"
            return subprocess.CompletedProcess(cmd, 0 if loaded else 113, output, "")
        assert cmd == ["launchctl", "list", "ai.hermes.gateway"]
        # Domain registration is authoritative even if list has a stale PID.
        assert domain == "legacy"
        output = f'"PID" = {pid};\n' if pid else '"Label" = "ai.hermes.gateway";\n'
        return subprocess.CompletedProcess(cmd, 0, output, "")

    monkeypatch.setattr(gw.subprocess, "run", launchctl)
    assert gw._launchctl_supervised_pid("ai.hermes.gateway") == pid
    assert gw._is_service_running() is (pid is not None)
    gw.launchd_status()
    output = capsys.readouterr().out
    if pid:
        assert f"supervised by launchd (PID {pid})" in output
    else:
        assert "registered with launchd" in output
        assert "not supervising a process" in output


@pytest.mark.parametrize("failure", ["absent", "timeout", "oserror"])
def test_failed_detection_never_reports_a_running_service(tmp_path, monkeypatch, capsys, failure):
    plist = tmp_path / "gateway.plist"
    plist.touch()
    monkeypatch.setattr(gw, "supports_systemd_services", lambda: False)
    monkeypatch.setattr(gw, "get_launchd_plist_path", lambda: plist)
    monkeypatch.setattr(gw, "launchd_plist_is_current", lambda: True)
    monkeypatch.setattr(gw, "_launchd_unsupported_marker_exists", lambda: False)
    monkeypatch.setattr("gateway.status.get_running_pid", lambda **kwargs: None)

    def launchctl(cmd, **kwargs):
        if failure == "timeout":
            raise subprocess.TimeoutExpired(cmd, 5)
        if failure == "oserror":
            raise OSError("launchctl unavailable")
        return subprocess.CompletedProcess(cmd, 113, "", "")

    monkeypatch.setattr(gw.subprocess, "run", launchctl)
    assert gw._is_service_running() is False
    gw.launchd_status()
    assert "service is not loaded" in capsys.readouterr().out
