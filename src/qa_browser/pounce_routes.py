"""POUNCE submission validation routes.

Wired into app.py via:
    app.include_router(_pounce_module.router)
    _pounce_module.set_templates(templates)
    _pounce_module.set_pounce_config(path_to_pounce_yaml)
    _pounce_module.set_smtp_config(path_to_smtp_yaml)   # optional
"""
import os
import shutil
import smtplib
import tempfile
import time
import uuid
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import List

from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse

from src.input_adapters.pounce_sheets.mapping_coverage import (
    check_gene_coverage,
    check_metabolite_coverage,
)
from src.input_adapters.pounce_sheets.pounce_input_adapter import PounceInputAdapter
from src.input_adapters.pounce_sheets.pounce_parser import (
    _EXPERIMENT_RECOGNIZED_SHEETS,
    _PROJECT_REQUIRED_SHEETS,
    _STATS_RECOGNIZED_SHEETS,
)

router = APIRouter(prefix="/pounce")

_templates = None
_VALIDATORS_CONFIG = str(Path(__file__).resolve().parent.parent / "use_cases" / "pounce_validators.yaml")

# Resolver map populated at startup from pounce.yaml.
# Keys are entity type strings (e.g. "Metabolite", "Gene").
# Empty if pounce.yaml is missing or all resolvers fail to load.
_resolver_map: dict = {}

# ── Session store ────────────────────────────────────────────────────────────
# Keeps uploaded files alive after validation so the submitter can email them.
# session dict keys:
#   dir          — temp directory path (NOT auto-deleted)
#   created      — unix timestamp for TTL
#   project_name — display name from parsed project or uploaded filename
#   file_paths   — list of all uploaded xlsx paths
#   file_basenames — list of original basenames (for display)
#   summary      — dict: error_count, warning_count, coverage_lines

_sessions: dict = {}
_SESSION_TTL = 3600  # 1 hour

# ── SMTP configuration ───────────────────────────────────────────────────────
# Empty dict means email is disabled.
# Keys: host, port, user, password, from_address, to_address, use_tls

_smtp_config: dict = {}


def set_templates(t):
    global _templates
    _templates = t


def set_smtp_config(path: str):
    """Load SMTP credentials from a YAML file.

    Called once at app startup. If the file is missing or malformed the
    email submission feature is silently disabled.
    """
    global _smtp_config
    if not path or not os.path.exists(path):
        print(f"[pounce] SMTP config not found at {path!r} — email submission disabled")
        return
    try:
        import yaml
        with open(path) as f:
            _smtp_config = yaml.safe_load(f) or {}
        print(f"[pounce] Loaded SMTP config from {path!r}")
    except Exception as e:
        print(f"[pounce] Could not load SMTP config from {path!r}: {e}")


def set_pounce_config(pounce_yaml_path: str):
    """Load resolvers from pounce.yaml for mapping coverage checks.

    Called once at app startup. Failures are caught per-resolver so that one
    missing data file does not block the others from loading.
    """
    global _resolver_map
    if not pounce_yaml_path or not os.path.exists(pounce_yaml_path):
        print(f"[pounce] config not found at {pounce_yaml_path!r} — mapping coverage disabled")
        return
    try:
        from src.core.config import Config, create_object_from_config
        config = Config(pounce_yaml_path)
        for rc in config.config_dict.get("resolvers", []):
            label = rc.get("label", "?")
            try:
                resolver = create_object_from_config(rc)
                for t in getattr(resolver, "types", []):
                    _resolver_map[t] = resolver
                    print(f"[pounce] loaded resolver '{label}' for type '{t}'")
            except Exception as e:
                print(f"[pounce] could not load resolver '{label}': {e}")
    except Exception as e:
        print(f"[pounce] could not load config from {pounce_yaml_path!r}: {e}")


# ── Session helpers ───────────────────────────────────────────────────────────

def _cleanup_old_sessions():
    """Remove sessions older than _SESSION_TTL and delete their temp dirs."""
    now = time.time()
    expired = [sid for sid, s in list(_sessions.items()) if now - s["created"] > _SESSION_TTL]
    for sid in expired:
        shutil.rmtree(_sessions[sid]["dir"], ignore_errors=True)
        del _sessions[sid]


# ── File helpers ──────────────────────────────────────────────────────────────

def _save_upload(upload: UploadFile, directory: str, prefix: str = "") -> str:
    safe_name = Path(upload.filename).name
    dest = os.path.join(directory, f"{prefix}{safe_name}" if prefix else safe_name)
    if os.path.exists(dest):
        base, ext = os.path.splitext(safe_name)
        dest = os.path.join(directory, f"{prefix}{base}_1{ext}")
    with open(dest, "wb") as f:
        f.write(upload.file.read())
    return dest


# ── Validation helpers ────────────────────────────────────────────────────────

def _source_label(issue, project_filename: str, exp_filenames: list, stats_filenames: list) -> str:
    if issue.source_file:
        return os.path.basename(issue.source_file)
    sheet = issue.sheet
    if sheet in _PROJECT_REQUIRED_SHEETS:
        return project_filename
    if sheet in _EXPERIMENT_RECOGNIZED_SHEETS:
        return exp_filenames[0] if len(exp_filenames) == 1 else "Experiment workbook(s)"
    if sheet in _STATS_RECOGNIZED_SHEETS:
        return stats_filenames[0] if len(stats_filenames) == 1 else "Stats workbook(s)"
    return "(unknown workbook)"


def _group_issues(all_issues, project_filename, exp_filenames, stats_filenames):
    grouped: dict = {}
    for issue in all_issues:
        wb = _source_label(issue, project_filename, exp_filenames, stats_filenames)
        sheet = issue.sheet or "(no sheet)"
        grouped.setdefault(wb, {}).setdefault(sheet, []).append(issue)
    return [(wb, list(sheets.items())) for wb, sheets in grouped.items()]


def _compute_coverage(parsed_data) -> list:
    """Return AnalyteCoverage results for analyte types that have a resolver loaded."""
    coverage = []
    if parsed_data is None:
        return coverage

    if parsed_data.metabolites:
        resolver = _resolver_map.get("Metabolite")
        if resolver:
            try:
                coverage.append(check_metabolite_coverage(parsed_data.metabolites, resolver))
            except Exception as e:
                print(f"[pounce] metabolite coverage check failed: {e}")

    if parsed_data.genes:
        resolver = _resolver_map.get("Gene")
        try:
            cov = check_gene_coverage(parsed_data.genes, resolver)
            if cov:
                coverage.append(cov)
        except Exception as e:
            print(f"[pounce] gene coverage check failed: {e}")

    return coverage


def _compute_summary(all_issues: list, coverage: list) -> dict:
    """Build a summary dict for inclusion in the submission email."""
    errors   = [i for i in all_issues if i.severity == "error"]
    warnings = [i for i in all_issues if i.severity == "warning"]
    cov_lines = [
        f"  {cov.analyte_type}: {cov.mapped}/{cov.total} ({cov.mapped_pct:.1f}%)"
        for cov in coverage
    ]

    def _fmt(issue):
        parts = [p for p in [issue.sheet, issue.field,
                              f"row {issue.row}" if issue.row is not None else None]
                 if p]
        loc = " / ".join(parts)
        return f"  [{loc}] {issue.message}" if loc else f"  {issue.message}"

    return {
        "error_count":    len(errors),
        "warning_count":  len(warnings),
        "coverage_lines": cov_lines,
        "error_samples":   [_fmt(i) for i in errors[:5]],
        "warning_samples": [_fmt(i) for i in warnings[:5]],
    }


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("/validate", response_class=HTMLResponse)
async def upload_form(request: Request):
    return _templates.TemplateResponse("pounce_upload.html", {"request": request})


@router.post("/validate", response_class=HTMLResponse)
async def run_validation(
    request: Request,
    project_file:     UploadFile       = File(...),
    experiment_files: List[UploadFile] = File(...),
    stats_files:      List[UploadFile] = File(...),
):
    _cleanup_old_sessions()

    parse_error = None
    parsed_data = None
    grouped     = []
    all_issues  = []
    session_id  = None

    tmp = tempfile.mkdtemp()

    try:
        project_path = _save_upload(project_file, tmp)
        exp_paths    = [_save_upload(f, tmp, prefix=f"exp{i}_")   for i, f in enumerate(experiment_files)]
        stats_paths  = [_save_upload(f, tmp, prefix=f"stats{i}_") for i, f in enumerate(stats_files)]

        adapter = PounceInputAdapter(
            project_file=project_path,
            experiment_files=exp_paths,
            stats_results_files=stats_paths,
            validators_config=_VALIDATORS_CONFIG,
        )
        parsed_data       = adapter.get_validation_data()
        structural_issues = adapter.get_structural_issues()

        content_issues = []
        for v in adapter.get_validators():
            content_issues.extend(v.validate(parsed_data))

        all_issues = structural_issues + content_issues
        grouped    = _group_issues(
            all_issues,
            os.path.basename(project_path),
            [os.path.basename(p) for p in exp_paths],
            [os.path.basename(p) for p in stats_paths],
        )

        # Create session so files survive until the submitter clicks Send.
        session_id = str(uuid.uuid4())
        project_name = (
            parsed_data.project.project_name
            if parsed_data and parsed_data.project
            else os.path.basename(project_path)
        )
        _sessions[session_id] = {
            "dir": tmp,
            "created": time.time(),
            "project_name": project_name,
            "project_path": project_path,
            "exp_paths":    exp_paths,
            "stats_paths":  stats_paths,
            "file_paths": [project_path] + exp_paths + stats_paths,
            "file_basenames": (
                [os.path.basename(project_path)]
                + [os.path.basename(p) for p in exp_paths]
                + [os.path.basename(p) for p in stats_paths]
            ),
            "summary": None,  # filled in below after coverage is computed
        }

    except Exception as e:
        parse_error = str(e)
        shutil.rmtree(tmp, ignore_errors=True)

    errors   = [i for i in all_issues if i.severity == "error"]
    warnings = [i for i in all_issues if i.severity == "warning"]
    coverage = _compute_coverage(parsed_data)

    # Fill in session summary now that coverage is available.
    if session_id and session_id in _sessions:
        _sessions[session_id]["summary"] = _compute_summary(all_issues, coverage)

    has_metabolites         = bool(parsed_data and parsed_data.metabolites)
    has_genes               = bool(parsed_data and parsed_data.genes)
    metab_resolver_missing  = has_metabolites and "Metabolite" not in _resolver_map
    gene_resolver_missing   = has_genes       and "Gene"       not in _resolver_map

    session_filenames = (
        _sessions[session_id]["file_basenames"] if session_id and session_id in _sessions else []
    )

    return _templates.TemplateResponse("pounce_results.html", {
        "request":                request,
        "parsed_data":            parsed_data,
        "grouped":                grouped,
        "errors":                 errors,
        "warnings":               warnings,
        "parse_error":            parse_error,
        "project_filename":       project_file.filename,
        "coverage":               coverage,
        "metab_resolver_missing": metab_resolver_missing,
        "gene_resolver_missing":  gene_resolver_missing,
        "session_id":             session_id,
        "session_filenames":      session_filenames,
        "smtp_configured":        bool(_smtp_config),
    })


@router.get("/download/{session_id}/{filename}")
async def download_file(session_id: str, filename: str):
    """Serve an uploaded file back to the submitter for editing."""
    _cleanup_old_sessions()
    session = _sessions.get(session_id)
    if not session:
        return HTMLResponse("Session expired — please re-upload your files.", status_code=404)
    # Guard against path traversal: only serve filenames actually in the session.
    if filename not in session["file_basenames"]:
        return HTMLResponse("File not found.", status_code=404)
    for fp in session["file_paths"]:
        if os.path.basename(fp) == filename:
            return FileResponse(
                fp,
                filename=filename,
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
    return HTMLResponse("File not found.", status_code=404)


@router.get("/edit/{session_id}", response_class=HTMLResponse)
async def edit_form(request: Request, session_id: str):
    """Show the upload form pre-populated with the current session's filenames."""
    _cleanup_old_sessions()
    session = _sessions.get(session_id)
    if not session:
        return _templates.TemplateResponse("pounce_submit_confirm.html", {
            "request": request,
            "error": "Session expired — please re-upload your files.",
            "name": "", "email": "", "project_name": "",
        })
    return _templates.TemplateResponse("pounce_edit.html", {
        "request":          request,
        "session_id":       session_id,
        "project_basename": os.path.basename(session["project_path"]),
        "exp_basenames":    [os.path.basename(p) for p in session["exp_paths"]],
        "stats_basenames":  [os.path.basename(p) for p in session["stats_paths"]],
    })


@router.post("/edit/{session_id}", response_class=HTMLResponse)
async def revalidate(
    request:          Request,
    session_id:       str,
    project_file:     UploadFile       = File(...),
    experiment_files: List[UploadFile] = File(...),
    stats_files:      List[UploadFile] = File(...),
):
    """Re-run validation, replacing only the slots where a new file was uploaded."""
    _cleanup_old_sessions()
    session = _sessions.get(session_id)
    if not session:
        return _templates.TemplateResponse("pounce_submit_confirm.html", {
            "request": request,
            "error": "Session expired — please re-upload your files.",
            "name": "", "email": "", "project_name": "",
        })

    parse_error  = None
    parsed_data  = None
    grouped      = []
    all_issues   = []
    new_sid      = None
    project_path = ""
    tmp          = tempfile.mkdtemp()

    try:
        # Project — replace if a new file was provided, else copy existing.
        if project_file.filename:
            project_path = _save_upload(project_file, tmp)
        else:
            project_path = shutil.copy2(session["project_path"], tmp)

        # Experiments — zip new uploads with existing paths.
        exp_paths = []
        for i, (new_f, existing_p) in enumerate(zip(experiment_files, session["exp_paths"])):
            if new_f.filename:
                exp_paths.append(_save_upload(new_f, tmp, prefix=f"exp{i}_"))
            else:
                exp_paths.append(shutil.copy2(existing_p, tmp))
        # Keep any existing experiments beyond what was submitted (shouldn't happen, but safe).
        for existing_p in session["exp_paths"][len(exp_paths):]:
            exp_paths.append(shutil.copy2(existing_p, tmp))

        # Stats — same pattern.
        stats_paths = []
        for i, (new_f, existing_p) in enumerate(zip(stats_files, session["stats_paths"])):
            if new_f.filename:
                stats_paths.append(_save_upload(new_f, tmp, prefix=f"stats{i}_"))
            else:
                stats_paths.append(shutil.copy2(existing_p, tmp))
        for existing_p in session["stats_paths"][len(stats_paths):]:
            stats_paths.append(shutil.copy2(existing_p, tmp))

        adapter = PounceInputAdapter(
            project_file=project_path,
            experiment_files=exp_paths,
            stats_results_files=stats_paths,
            validators_config=_VALIDATORS_CONFIG,
        )
        parsed_data       = adapter.get_validation_data()
        structural_issues = adapter.get_structural_issues()

        content_issues = []
        for v in adapter.get_validators():
            content_issues.extend(v.validate(parsed_data))

        all_issues = structural_issues + content_issues
        grouped    = _group_issues(
            all_issues,
            os.path.basename(project_path),
            [os.path.basename(p) for p in exp_paths],
            [os.path.basename(p) for p in stats_paths],
        )

        new_sid = str(uuid.uuid4())
        project_name = (
            parsed_data.project.project_name
            if parsed_data and parsed_data.project
            else os.path.basename(project_path)
        )
        _sessions[new_sid] = {
            "dir":            tmp,
            "created":        time.time(),
            "project_name":   project_name,
            "project_path":   project_path,
            "exp_paths":      exp_paths,
            "stats_paths":    stats_paths,
            "file_paths":     [project_path] + exp_paths + stats_paths,
            "file_basenames": (
                [os.path.basename(project_path)]
                + [os.path.basename(p) for p in exp_paths]
                + [os.path.basename(p) for p in stats_paths]
            ),
            "summary": None,
        }

    except Exception as e:
        parse_error = str(e)
        shutil.rmtree(tmp, ignore_errors=True)

    errors   = [i for i in all_issues if i.severity == "error"]
    warnings = [i for i in all_issues if i.severity == "warning"]
    coverage = _compute_coverage(parsed_data)

    if new_sid and new_sid in _sessions:
        _sessions[new_sid]["summary"] = _compute_summary(all_issues, coverage)

    has_metabolites        = bool(parsed_data and parsed_data.metabolites)
    has_genes              = bool(parsed_data and parsed_data.genes)
    metab_resolver_missing = has_metabolites and "Metabolite" not in _resolver_map
    gene_resolver_missing  = has_genes       and "Gene"       not in _resolver_map

    session_filenames = (
        _sessions[new_sid]["file_basenames"] if new_sid and new_sid in _sessions else []
    )

    return _templates.TemplateResponse("pounce_results.html", {
        "request":                request,
        "parsed_data":            parsed_data,
        "grouped":                grouped,
        "errors":                 errors,
        "warnings":               warnings,
        "parse_error":            parse_error,
        "project_filename":       os.path.basename(project_path),
        "coverage":               coverage,
        "metab_resolver_missing": metab_resolver_missing,
        "gene_resolver_missing":  gene_resolver_missing,
        "session_id":             new_sid,
        "session_filenames":      session_filenames,
        "smtp_configured":        bool(_smtp_config),
    })


@router.post("/submit/{session_id}", response_class=HTMLResponse)
async def submit_pounce(
    request: Request,
    session_id: str,
    name: str = Form(...),
    email: str = Form(...),
):
    _cleanup_old_sessions()

    session = _sessions.get(session_id)
    if not session:
        return _templates.TemplateResponse("pounce_submit_confirm.html", {
            "request": request,
            "error": "Session expired — please re-upload your files and try again.",
            "name": name,
            "email": email,
            "project_name": "",
        })

    if not _smtp_config:
        return _templates.TemplateResponse("pounce_submit_confirm.html", {
            "request": request,
            "error": "Email delivery is not configured on this server.",
            "name": name,
            "email": email,
            "project_name": session["project_name"],
        })

    # Build email message.
    summary        = session.get("summary") or {}
    error_count    = summary.get("error_count", 0)
    warning_count  = summary.get("warning_count", 0)
    coverage_lines = summary.get("coverage_lines", [])
    error_samples  = summary.get("error_samples", [])
    warning_samples = summary.get("warning_samples", [])

    body_lines = [
        "POUNCE Submission",
        "",
        f"Submitter: {name} <{email}>",
        f"Project:   {session['project_name']}",
        "",
        "=== Validation Summary ===",
        f"  Errors:   {error_count}",
    ]
    if error_samples:
        body_lines.extend(error_samples)
        if error_count > 5:
            body_lines.append(f"  … {error_count - 5} more error(s)")
    body_lines.append(f"  Warnings: {warning_count}")
    if warning_samples:
        body_lines.extend(warning_samples)
        if warning_count > 5:
            body_lines.append(f"  … {warning_count - 5} more warning(s)")
    if coverage_lines:
        body_lines.append("")
        body_lines.append("=== Mapping Coverage ===")
        body_lines.extend(coverage_lines)

    msg = MIMEMultipart()
    msg["Subject"]  = f"POUNCE Submission: {session['project_name']}"
    msg["From"]     = _smtp_config["from_address"]
    msg["To"]       = _smtp_config["to_address"]
    msg["Reply-To"] = email
    msg.attach(MIMEText("\n".join(body_lines), "plain"))

    # Attach all xlsx files.
    for file_path in session["file_paths"]:
        if os.path.exists(file_path):
            with open(file_path, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition",
                "attachment",
                filename=os.path.basename(file_path),
            )
            msg.attach(part)

    # Send via SMTP.
    try:
        host     = _smtp_config.get("host", "localhost")
        port     = _smtp_config.get("port", 587)
        use_tls  = _smtp_config.get("use_tls", True)
        user     = _smtp_config.get("user")
        password = _smtp_config.get("password")
        timeout  = _smtp_config.get("timeout_seconds", 10)

        smtp = smtplib.SMTP(host, port, timeout=timeout)
        if use_tls:
            smtp.starttls()
        if user and password:
            smtp.login(user, password)
        smtp.sendmail(_smtp_config["from_address"], _smtp_config["to_address"], msg.as_string())
        smtp.quit()
    except Exception as e:
        return _templates.TemplateResponse("pounce_submit_confirm.html", {
            "request":      request,
            "error":        f"Failed to send email: {e}",
            "name":         name,
            "email":        email,
            "project_name": session["project_name"],
        })

    # Clean up session immediately after successful send.
    shutil.rmtree(session["dir"], ignore_errors=True)
    del _sessions[session_id]

    return _templates.TemplateResponse("pounce_submit_confirm.html", {
        "request":      request,
        "error":        None,
        "name":         name,
        "email":        email,
        "project_name": session["project_name"],
    })
