"""Feedback / bug-report drawer routes.

Wired into app.py via:
    app.include_router(_feedback_module.router)
    _feedback_module.set_templates(templates)
    _feedback_module.set_feedback_file(path)
"""
import json
import os
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse

router = APIRouter(prefix="/feedback")

_templates = None
_feedback_file: str = ""
_file_lock = threading.Lock()


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


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("/comments", response_class=HTMLResponse)
async def get_comments(request: Request):
    entries = _read()
    visible = [e for e in entries if not e.get("resolved")]
    resolved_count = sum(1 for e in entries if e.get("resolved"))
    return _templates.TemplateResponse("feedback_comments.html", {
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
        entries.append({
            "id":        str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "page":      page.strip() or "(unknown)",
            "name":      name.strip(),
            "comment":   comment.strip(),
            "resolved":  False,
        })
        _write(entries)

    entries = _read()
    visible = [e for e in entries if not e.get("resolved")]
    resolved_count = sum(1 for e in entries if e.get("resolved"))
    return _templates.TemplateResponse("feedback_comments.html", {
        "request":          request,
        "entries":          visible,
        "resolved_count":   resolved_count,
        "feedback_enabled": bool(_feedback_file),
    })


@router.get("/admin", response_class=HTMLResponse)
async def admin_page(request: Request):
    entries = _read()
    return _templates.TemplateResponse("feedback_admin.html", {
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
