import json

from src.qa_browser import feedback_routes


def test_build_jira_issue_payload_includes_feedback_fields(monkeypatch):
    feedback_routes.set_jira_config(
        url="https://ncats-nih.atlassian.net",
        user="odin@example.org",
        api_token="token",
        project_key="ODIN",
        issue_type="Task",
    )
    monkeypatch.setattr(feedback_routes.socket, "gethostname", lambda: "qa-host")

    payload = feedback_routes._build_jira_issue_payload({
        "id": "abc-123",
        "timestamp": "2026-05-12T10:00:00+00:00",
        "page": "/graph/123 - QA Browser",
        "name": "Keith",
        "comment": "The counts look wrong.",
    })

    assert payload["fields"]["project"]["key"] == "ODIN"
    assert payload["fields"]["issuetype"]["name"] == "Task"
    assert payload["fields"]["summary"] == "QA Browser feedback: /graph/123 - QA Browser"
    description = payload["fields"]["description"]
    assert "Host: qa-host" in description
    assert "Page: /graph/123 - QA Browser" in description
    assert "From: Keith" in description
    assert "Feedback ID: abc-123" in description
    assert "The counts look wrong." in description


def test_create_jira_issue_returns_none_without_config(monkeypatch):
    feedback_routes.set_jira_config("", "", "", "")
    called = {"value": False}

    def fake_urlopen(*args, **kwargs):
        called["value"] = True
        raise AssertionError("urlopen should not be called without Jira config")

    monkeypatch.setattr(feedback_routes.urllib_request, "urlopen", fake_urlopen)
    assert feedback_routes._create_jira_issue({"comment": "hi"}) is None
    assert called["value"] is False


def test_create_jira_issue_posts_basic_auth_json(monkeypatch):
    feedback_routes.set_jira_config(
        url="https://ncats-nih.atlassian.net",
        user="odin@example.org",
        api_token="secret-token",
        project_key="ODIN",
        issue_type="Bug",
    )
    monkeypatch.setattr(feedback_routes.socket, "gethostname", lambda: "qa-host")
    captured = {}

    class FakeResponse:
        status = 201

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return b'{"key":"ODIN-42"}'

    def fake_urlopen(req, timeout):
        captured["url"] = req.full_url
        captured["timeout"] = timeout
        captured["headers"] = dict(req.header_items())
        captured["body"] = json.loads(req.data.decode("utf-8"))
        return FakeResponse()

    monkeypatch.setattr(feedback_routes.urllib_request, "urlopen", fake_urlopen)

    issue_key = feedback_routes._create_jira_issue({
        "id": "feedback-1",
        "timestamp": "2026-05-12T10:00:00+00:00",
        "page": "/datasets - QA Browser",
        "name": "",
        "comment": "Needs sorting.",
    })

    assert issue_key == "ODIN-42"
    assert captured["url"] == "https://ncats-nih.atlassian.net/rest/api/2/issue"
    assert captured["timeout"] == 5
    assert captured["headers"]["Authorization"].startswith("Basic ")
    assert captured["headers"]["Accept"] == "application/json"
    assert captured["headers"]["Content-type"] == "application/json"
    assert captured["body"]["fields"]["project"]["key"] == "ODIN"
    assert captured["body"]["fields"]["issuetype"]["name"] == "Bug"


def test_attach_jira_key_updates_feedback_entry(tmp_path):
    feedback_file = tmp_path / "feedback.json"
    feedback_routes.set_feedback_file(str(feedback_file))
    feedback_routes._write([
        {
            "id": "feedback-1",
            "comment": "Needs sorting.",
            "resolved": False,
        }
    ])

    feedback_routes._attach_jira_key("feedback-1", "ODIN-42")

    entries = feedback_routes._read()
    assert entries[0]["jira_issue_key"] == "ODIN-42"
