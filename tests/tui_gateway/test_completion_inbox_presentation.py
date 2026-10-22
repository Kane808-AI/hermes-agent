"""Completion wake-up markers stay internal on both transcript read surfaces."""

from copy import deepcopy

import tui_gateway.server as server
from hermes_cli.web_routers.sessions import _project_for_display


def _completion_rows():
    return [
        {
            "role": "user",
            "content": "Internal completion wake-up",
            "display_kind": "completion_inbox",
            "display_metadata": {
                "completion_inbox": {"kind": "marker", "delivery_id": "delivery-1"},
            },
        },
        {
            "role": "assistant",
            "content": "The task is finished.",
            "display_kind": "completion_inbox",
            "display_metadata": {
                "completion_inbox": {"kind": "response", "delivery_id": "delivery-1"},
            },
        },
        {"role": "user", "content": "Keep a label without marker metadata", "display_kind": "completion_inbox"},
        {
            "role": "user",
            "content": "Keep metadata without the marker label",
            "display_metadata": {"completion_inbox": {"kind": "marker"}},
        },
    ]


def test_tui_history_omits_only_internal_completion_marker(tmp_path):
    rows = _completion_rows()
    original = deepcopy(rows)

    displayed = server._history_to_messages(rows, profile_home=tmp_path)

    assert [row["text"] for row in displayed] == [row["content"] for row in rows[1:]]
    assert displayed[0]["role"] == "assistant"
    assert displayed[0]["display_metadata"] == original[1]["display_metadata"]
    assert rows == original


def test_desktop_history_hides_only_internal_completion_marker(tmp_path):
    rows = _completion_rows()
    original = deepcopy(rows)

    displayed = _project_for_display(rows, home=tmp_path)

    assert displayed[0] == {**original[0], "display_kind": "hidden"}
    assert displayed[1:] == original[1:]
    assert rows == original
