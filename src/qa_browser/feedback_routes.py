"""Feedback / bug-report drawer routes.

Wired into app.py via:
    app.include_router(_feedback_module.router)
    _feedback_module.set_templates(templates)
    _feedback_module.set_feedback_file(path)
"""
import json
import os
import socket
import threading
import uuid
import base64
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib import error as urllib_error
from urllib import request as urllib_request

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse

router = APIRouter(prefix="/feedback")

_templates = None
_feedback_file: str = ""
_file_lock = threading.Lock()
_jira_url: str = os.getenv("QA_BROWSER_JIRA_URL", "").strip()
_jira_user: str = os.getenv("QA_BROWSER_JIRA_USER", "").strip()
_jira_api_token: str = os.getenv("QA_BROWSER_JIRA_API_TOKEN", "").strip()
_jira_project_key: str = os.getenv("QA_BROWSER_JIRA_PROJECT_KEY", "").strip()
_jira_issue_type: str = os.getenv("QA_BROWSER_JIRA_ISSUE_TYPE", "Task").strip() or "Task"


def set_templates(t):
    global _templates
    _templates = t


def set_feedback_file(path: str):
    global _feedback_file
    _feedback_file = path
    if path and not os.path.exists(path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump([], f)
    print(f"[feedback] file: {path!r}")


def set_jira_config(url: str, user: str, api_token: str, project_key: str, issue_type: str = "Task"):
    global _jira_url, _jira_user, _jira_api_token, _jira_project_key, _jira_issue_type
    _jira_url = (url or "").strip()
    _jira_user = (user or "").strip()
    _jira_api_token = (api_token or "").strip()
    _jira_project_key = (project_key or "").strip()
    _jira_issue_type = (issue_type or "Task").strip() or "Task"


# ── File helpers ──────────────────────────────────────────────────────────────

def _read() -> list:
    if not _feedback_file or not os.path.exists(_feedback_file):
        return []
    with _file_lock:
        with open(_feedback_file) as f:
            try:
                return json.load(f)
            except Exception:
                return []


def _write(entries: list):
    with _file_lock:
        with open(_feedback_file, "w") as f:
            json.dump(entries, f, indent=2, default=str)


def _jira_enabled() -> bool:
    return all([_jira_url, _jira_user, _jira_api_token, _jira_project_key])


def _build_jira_issue_payload(entry: dict) -> dict:
    hostname = socket.gethostname()
    page = entry.get("page") or "(unknown)"
    name = entry.get("name") or "Anonymous"
    comment = entry.get("comment") or ""
    timestamp = entry.get("timestamp") or ""
    summary = f"QA Browser feedback: {page}"
    if len(summary) > 255:
        summary = summary[:252] + "..."
    description = (
        f"QA Browser feedback\n\n"
        f"Host: {hostname}\n"
        f"Page: {page}\n"
        f"From: {name}\n"
        f"At: {timestamp}\n"
        f"Feedback ID: {entry.get('id') or ''}\n\n"
        f"Comment:\n{comment}\n"
    )
    return {
        "fields": {
            "project": {"key": _jira_project_key},
            "summary": summary,
            "description": description,
            "issuetype": {"name": _jira_issue_type},
        }
    }


def _create_jira_issue(entry: dict) -> Optional[str]:
    if not _jira_enabled():
        return None
    auth = base64.b64encode(f"{_jira_user}:{_jira_api_token}".encode("utf-8")).decode("ascii")
    payload = json.dumps(_build_jira_issue_payload(entry)).encode("utf-8")
    req = urllib_request.Request(
        f"{_jira_url.rstrip('/')}/rest/api/2/issue",
        data=payload,
        headers={
            "Authorization": f"Basic {auth}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib_request.urlopen(req, timeout=5) as response:
            if not (200 <= getattr(response, "status", 200) < 300):
                return None
            body = json.loads(response.read().decode("utf-8") or "{}")
            return body.get("key")
    except urllib_error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        print(f"[feedback] jira issue creation failed: HTTP {exc.code}: {body}")
        return None
    except (urllib_error.URLError, TimeoutError, OSError) as exc:
        print(f"[feedback] jira issue creation failed: {exc}")
        return None


def _attach_jira_key(entry_id: str, issue_key: str):
    if not issue_key:
        return
    entries = _read()
    changed = False
    for entry in entries:
        if entry.get("id") == entry_id:
            entry["jira_issue_key"] = issue_key
            changed = True
            break
    if changed:
        _write(entries)


def _create_jira_issue_and_update_entry(entry: dict):
    issue_key = _create_jira_issue(entry)
    if issue_key:
        _attach_jira_key(entry.get("id"), issue_key)


def _notify_jira_async(entry: dict):
    if not _jira_enabled():
        return
    threading.Thread(target=_create_jira_issue_and_update_entry, args=(entry,), daemon=True).start()


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("/comments", response_class=HTMLResponse)
async def get_comments(request: Request):
    entries = _read()
    visible = [e for e in entries if not e.get("resolved")]
    resolved_count = sum(1 for e in entries if e.get("resolved"))
    return _templates.TemplateResponse(request, "feedback_comments.html", {
        "request":        request,
        "entries":        visible,
        "resolved_count": resolved_count,
        "feedback_enabled": bool(_feedback_file),
    })


@router.post("/comment", response_class=HTMLResponse)
async def post_comment(
    request: Request,
    comment: str  = Form(...),
    name:    str  = Form(""),
    page:    str  = Form(""),
):
    if _feedback_file and comment.strip():
        entries = _read()
        entry = {
            "id":        str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "page":      page.strip() or "(unknown)",
            "name":      name.strip(),
            "comment":   comment.strip(),
            "resolved":  False,
        }
        entries.append(entry)
        _write(entries)
        _notify_jira_async(entry)

    entries = _read()
    visible = [e for e in entries if not e.get("resolved")]
    resolved_count = sum(1 for e in entries if e.get("resolved"))
    return _templates.TemplateResponse(request, "feedback_comments.html", {
        "request":          request,
        "entries":          visible,
        "resolved_count":   resolved_count,
        "feedback_enabled": bool(_feedback_file),
    })


@router.get("/admin", response_class=HTMLResponse)
async def admin_page(request: Request):
    entries = _read()
    return _templates.TemplateResponse(request, "feedback_admin.html", {
        "request": request,
        "entries": entries,
    })


@router.post("/admin/{entry_id}/resolve", response_class=HTMLResponse)
async def resolve_entry(request: Request, entry_id: str):
    entries = _read()
    for e in entries:
        if e["id"] == entry_id:
            e["resolved"] = not e.get("resolved", False)
            break
    _write(entries)
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url=f"{request.scope.get('root_path', '')}/feedback/admin", status_code=303)


@router.post("/admin/{entry_id}/delete", response_class=HTMLResponse)
async def delete_entry(request: Request, entry_id: str):
    entries = [e for e in _read() if e["id"] != entry_id]
    _write(entries)
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url=f"{request.scope.get('root_path', '')}/feedback/admin", status_code=303)
