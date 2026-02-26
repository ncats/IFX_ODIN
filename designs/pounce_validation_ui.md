# POUNCE Validation Web UI

## Goal

Give data submitters a self-service tool to validate their POUNCE Excel workbooks
before sending them to NCATS. They upload their files, see a structured validation
report, inspect the parsed data, and email the files directly to the NCATS inbox — all
from one page.

This is the web front-end for the validation framework documented in
`pounce_validation_tool.md`. All validation logic stays in
`PounceInputAdapter` / `pounce_validators.yaml` — the UI is purely a delivery mechanism.

---

## File Layout

```
src/qa_browser/
├── app.py                         # registers router, --smtp-credentials arg
├── pounce_routes.py               # upload, results, and submit endpoints
├── templates/
│   ├── pounce_upload.html         # upload form
│   ├── pounce_results.html        # validation results + parsed data + submit form
│   └── pounce_submit_confirm.html # post-send confirmation (or error)
└── static/
    └── style.css                  # shared stylesheet

src/use_cases/secrets/
└── smtp.yaml                      # SMTP credentials (gitignored)
```

---

## User Flow

```
GET  /pounce/validate
     Upload form — drag-and-drop file pickers:
       • Project workbook (required, single file)
       • Experiment + StatsResults pairs (one or more, added dynamically)
     Single "Validate" button (shows spinner while server is working).

POST /pounce/validate
     Saves uploads to a temp directory (mkdtemp, NOT auto-deleted).
     Runs validation and mapping coverage checks.
     Stores files in a session (1 hr TTL) for later email submission.
     Returns pounce_results.html.

POST /pounce/submit/{session_id}
     Sends an email to the configured NCATS inbox with:
       • xlsx files attached
       • Submitter name/email in body
       • Validation summary (error/warning counts + first 5 of each)
       • Mapping coverage
     Cleans up the session directory on success.
     Returns pounce_submit_confirm.html.
```

---

## Upload Page (`pounce_upload.html`)

- Brief instruction text
- Drag-and-drop zones using `DataTransfer` API, falling back to click-to-browse
- Experiment/StatsResults pairs managed with a "+ Add experiment" button (JS cloning)
- "Validate" button shows `<spinner> Validating…` on submit and disables to prevent
  double-submission

---

## Results Page (`pounce_results.html`)

### Hero header

- **h1**: `project_name` (or uploaded filename if parsing failed)
- **subtitle**: `project_id · project_type · date` (whichever fields are present)
- **right-side stats**: error count (red if > 0, green if 0) and warning count

### Status banner

- Green: "Validation passed — no errors or warnings."
- Yellow: "N warning(s) — review before submitting."
- Red: "N error(s) [and M warning(s)] found."

### Stat boxes

Entity counts in a row of small boxes:
Experiments · Biospecimens · Biosamples · Run Biosamples · Genes (if present) ·
Metabolites (if present) · Stats Datasets

### Project card

A flat (non-collapsible) card directly below the stat boxes. Key-value grid showing
all `ParsedProject` fields (id, description, date, owners, collaborators, lab groups,
keywords, privacy, project type, rare disease focus, sample preparation). Fields with
no value are omitted.

### Issues

Issues grouped by **workbook → sheet** in cards. Each row: severity badge (ERROR/WARN),
field name, optional row number, message. Shown only when there are issues.

### Mapping coverage

Progress bars for gene/metabolite ID resolution rates, with expandable unmapped-ID
lists. Shown when the submission contains analytes and at least one resolver is
configured. If a resolver is missing, a note says so rather than silently omitting the
check.

### Parsed Data

Collapsible `<details>` sections for all entity types parsed from the workbooks:

| Section | Cap | Columns |
|---|---|---|
| Experiment 1..N | — (always 1 per exp) | Key-value grid (id, name, description, design, type, date, leads, platform, repo, protocols) |
| Biosamples | 25 | ID, type |
| Biospecimens | 25 | ID, type, organism, diseases |
| Exposures | 25 | Name(s), type, concentration, duration |
| Run Biosamples | 25 | Run ID, biosample ID, bio rep, tech rep, run order |
| Genes | 20 | Gene ID, symbol, biotype |
| Metabolites | 20 | Metabolite ID, name, chem class, ID level |
| Stats Results | 25 | Name, description, lead informatician, effect size, p-value col |

All sections are collapsed by default. "… N more" note shown when the table is capped.

### Submit to POUNCE form

Below the Parsed Data section. Shown only when parsing succeeded (session exists).

- If SMTP is configured: name + email fields, attached-files note, error/warning count
  note, "Submit to POUNCE" button (shows `<spinner> Sending…` on click).
- If SMTP is not configured: explanatory note, disabled button.

---

## Confirmation Page (`pounce_submit_confirm.html`)

**Success**: "Sent!" heading, project name, "Your submission has been sent to the NCATS
team. They will follow up at {email}." Link to upload another.

**Error** (session expired, SMTP failure, etc.): Error message, link to upload again.

---

## Session Store (`pounce_routes.py`)

Uploaded files are kept alive in a server-side dict after validation so the submitter
can email them without re-uploading.

```python
_sessions: dict = {}   # session_id -> session dict
_SESSION_TTL = 3600    # 1 hour

# session dict keys:
#   dir             — temp directory path (mkdtemp, manually managed)
#   created         — unix timestamp
#   project_name    — for email subject
#   file_paths      — list of all uploaded xlsx paths
#   file_basenames  — original filenames for display
#   summary         — error_count, warning_count, coverage_lines,
#                     error_samples (first 5), warning_samples (first 5)
```

`_cleanup_old_sessions()` is called on every request. Expired sessions have their
temp directories removed via `shutil.rmtree`.

On parse exception the temp dir is immediately deleted and `session_id` is `None`
(submit form is hidden).

---

## Email (`pounce_routes.py`)

Built with stdlib `smtplib` + `email.mime` — no new packages.

**Subject**: `POUNCE Submission: {project_name}`
**From/To**: from SMTP config
**Reply-To**: submitter's email
**Body**: submitter name/email, project name, error + warning counts with first 5
samples of each (formatted as `[Sheet / Field / row N] message`), mapping coverage.
**Attachments**: all uploaded xlsx files.

---

## SMTP Configuration

Loaded at startup via `--smtp-credentials` / `-e` CLI flag. If the flag is omitted or
the file is missing, email is disabled and the Submit button is shown as disabled with
an explanatory note.

**`src/use_cases/secrets/smtp.yaml`** (gitignored):

```yaml
host: mailfwd.nih.gov
port: 25
from_address: NCATSPOUNCE@nih.gov
to_address: NCATSPOUNCE@mail.nih.gov
use_tls: true          # starttls
timeout_seconds: 5
# user/password omitted — mail.smtp.auth=false
```

**`docker-compose.yml`** passes `-e src/use_cases/secrets/smtp.yaml` in the command.

---

## Server-side Logic (`pounce_routes.py`)

### Startup hooks

```python
set_templates(t)                  # called from app.py
set_pounce_config(yaml_path)      # loads resolvers for coverage checks
set_smtp_config(yaml_path)        # loads SMTP credentials; no-op if missing
```

### Resolver loading

Reads the `resolvers:` block from `pounce.yaml` via `Config` + `create_object_from_config`.
Failures are caught per-resolver. If all fail, `_resolver_map` stays empty and coverage
checks are skipped.

### Validation flow

1. `tempfile.mkdtemp()` — directory is **not** auto-deleted
2. Save uploads with sanitized filenames (`Path(f.filename).name`)
3. Construct `PounceInputAdapter`, call `get_validation_data()`,
   `get_structural_issues()`, run all validators from `get_validators()`
4. Group issues by workbook → sheet
5. Run `_compute_coverage()` for analyte types that have a resolver
6. Run `_compute_summary()` — collects counts + first-5 samples for email
7. Store session; render `pounce_results.html`
8. On exception: `shutil.rmtree(tmp)`, `session_id = None`, render error state

---

## Registration in `app.py`

```python
import src.qa_browser.pounce_routes as _pounce_module
app.include_router(_pounce_module.router)
_pounce_module.set_templates(templates)
# called in main() after args are parsed:
_pounce_module.set_pounce_config(args.pounce_config)
_pounce_module.set_smtp_config(args.smtp_credentials)
```

CLI flags added to `app.py`:

| Flag | Default | Purpose |
|---|---|---|
| `--pounce-config` / `-P` | `./src/use_cases/pounce.yaml` | Resolver source |
| `--smtp-credentials` / `-e` | `None` | SMTP config; omit to disable email |

---

## Out of Scope

- **Authentication**: internal NCATS tool on a trusted network.
- **Multi-submission history / database backing**: each session is in-memory, max 1 hr.
- **Re-validation without re-upload**: sessions store files for email only, not for
  re-running validation.
- **PDF/JSON report download**: the results page is the artifact.
- **Async processing / progress bar**: metadata-only validation is fast enough for
  synchronous handling.
- **EffectSize parsing**: not yet implemented in the input adapter.
