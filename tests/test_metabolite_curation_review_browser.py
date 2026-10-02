import asyncio
import functools
import json
import shutil
import socket
import subprocess
import threading
import time
import urllib.request
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest


ROOT = Path(__file__).parents[1]
SCRIPT = ROOT / "src/qa_browser/static/metabolite_curation_review.js"


def _chrome_path():
    candidates = [
        Path("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"),
        Path("/usr/bin/google-chrome"),
        Path("/usr/bin/chromium"),
        Path("/usr/bin/chromium-browser"),
    ]
    return next((path for path in candidates if path.exists()), None)


def _free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _row(index, curator):
    john = curator == "John Braisted"
    evidence_url = (
        "javascript:alert(1)"
        if index == 1
        else "https://github.com/ncats/RaMP-DB/pull/15"
    )
    return {
        "curation_type": "metabolite_equivalence_edges",
        "type_label": "Equivalence deny",
        "kind": "edge",
        "primary": f"CHEBI:{index} ↔ {'HMDB:' if john else 'REFMET:RM'}{index}",
        "secondary": "Equivalence edge removed",
        "note": "Reviewed",
        "sources": ["CHEBI", "HMDB" if john else "REFMET"],
        "batch_id": "john-batch" if john else "keith-batch",
        "batch_name": "John legacy" if john else "Keith review",
        "curator": curator,
        "published_at": "2026-10-01T12:00:00Z",
        "provenance_date": "2023-03-01T12:00:00Z" if john else "2026-10-01T12:00:00Z",
        "provenance_date_label": "Recorded in Git" if john else "Published",
        "origin": {
            "repository": "ncats/RaMP-backend-ncats",
            "path": "config/curation_mapping_issues_list.txt",
            "commit": "dc584fb19656642948a195d22eb7230503a2ab79",
            "authored_at": "2023-03-01T12:00:00Z",
            "raw_author": "johnbraisted",
            "attributed_curator": curator,
            "attribution_evidence_label": "GitHub PR #15 by KeithKelleher",
            "attribution_evidence_url": evidence_url,
        },
        "review_query": f"denylist_pair={index}",
    }


def _fixture_html(rows):
    return f"""<!doctype html>
<html><body>
<div id="metaboliteCurationReview" data-root-path="">
  <p id="metaboliteCurationResultCount"></p>
  <button id="metaboliteCurationRefresh" type="button">Refresh</button>
  <form id="metaboliteCurationFilters">
    <input name="q" type="search">
    <select name="curation_type" multiple></select>
    <select name="source" multiple></select>
    <select name="batch" multiple></select>
    <select name="curator" multiple></select>
    <button class="metabolite-curation-apply" type="submit">Apply</button>
    <a id="metaboliteCurationClear" href="/ramp-id-qa/curations">Clear</a>
  </form>
  <div id="metaboliteCurationTableWrap"><table><tbody id="metaboliteCurationRows"></tbody></table></div>
  <nav id="metaboliteCurationPagination"></nav>
  <p id="metaboliteCurationEmpty"></p>
</div>
<script type="application/json" id="metaboliteCurationRowsData">{json.dumps(rows)}</script>
<script src="/metabolite_curation_review.js"></script>
</body></html>"""


async def _evaluate(websocket_url, expressions):
    websockets = pytest.importorskip("websockets")
    results = []
    async with websockets.connect(websocket_url, origin="http://127.0.0.1") as websocket:
        for expression_index, expression in enumerate(expressions, start=1):
            for attempt in range(3):
                request_id = expression_index * 10 + attempt
                await websocket.send(json.dumps({
                    "id": request_id,
                    "method": "Runtime.evaluate",
                    "params": {
                        "expression": expression,
                        "returnByValue": True,
                        "awaitPromise": True,
                    },
                }))
                while True:
                    response = json.loads(await websocket.recv())
                    if response.get("id") != request_id:
                        continue
                    error = response.get("error")
                    if error and error.get("message") == "Execution context was destroyed.":
                        await asyncio.sleep(0.05)
                        break
                    if error:
                        raise AssertionError(error)
                    result = response["result"]["result"]
                    if result.get("subtype") == "error":
                        raise AssertionError(result.get("description"))
                    results.append(result.get("value"))
                    break
                if not error:
                    break
            else:
                raise AssertionError("Chrome page did not reach a stable execution context")
    return results


def test_browser_filters_contextually_without_additional_requests(tmp_path):
    chrome = _chrome_path()
    if chrome is None:
        pytest.skip("Chrome or Chromium is required for the browser regression test")

    rows = [_row(index, "John Braisted") for index in range(1, 61)]
    rows.append(_row(61, "Keith Kelleher"))
    (tmp_path / "index.html").write_text(_fixture_html(rows), encoding="utf-8")
    shutil.copyfile(SCRIPT, tmp_path / SCRIPT.name)

    http_port = _free_port()
    handler = functools.partial(SimpleHTTPRequestHandler, directory=str(tmp_path))
    server = ThreadingHTTPServer(("127.0.0.1", http_port), handler)
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    debug_port = _free_port()
    profile_dir = tmp_path / "chrome-profile"
    process = subprocess.Popen(
        [
            str(chrome), "--headless=new", "--disable-gpu", "--no-sandbox",
            "--disable-background-networking", "--no-first-run",
            "--remote-allow-origins=*", f"--remote-debugging-port={debug_port}",
            f"--user-data-dir={profile_dir}",
            f"http://127.0.0.1:{http_port}/index.html",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        pages = None
        for _attempt in range(100):
            try:
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{debug_port}/json/list", timeout=1
                ) as response:
                    pages = json.load(response)
                if pages:
                    break
            except Exception:
                time.sleep(0.05)
        if not pages:
            raise AssertionError("Chrome DevTools endpoint did not start")
        websocket_url = next(
            page["webSocketDebuggerUrl"] for page in pages if page.get("type") == "page"
        )
        expressions = [
            "new Promise(resolve => { const done = () => document.querySelectorAll('#metaboliteCurationRows tr').length === 50 ? resolve(true) : setTimeout(done, 20); done(); })",
            "performance.getEntriesByType('resource')"
            ".filter(entry => !entry.name.includes('/favicon.ico'))"
            ".map(entry => entry.name).sort()",
            "(() => { const s = document.forms.metaboliteCurationFilters.elements.curator; [...s.options].forEach(o => o.selected = o.value === 'John Braisted'); s.dispatchEvent(new Event('change', {{bubbles:true}})); return true; })()".replace("{{", "{").replace("}}", "}"),
            "({count: document.querySelector('#metaboliteCurationResultCount').textContent, type: document.forms.metaboliteCurationFilters.elements.curation_type.options[0].textContent, sources: [...document.forms.metaboliteCurationFilters.elements.source.options].map(o => o.textContent), batches: [...document.forms.metaboliteCurationFilters.elements.batch.options].map(o => o.textContent), curators: [...document.forms.metaboliteCurationFilters.elements.curator.options].map(o => o.textContent), rows: document.querySelectorAll('#metaboliteCurationRows tr').length, page: document.querySelector('#metaboliteCurationPagination').textContent, url: location.search, evidence: [...document.querySelectorAll('.metabolite-curation-origin')].map(d => d.textContent), evidenceLinks: [...document.querySelectorAll('.metabolite-curation-origin a')].map(a => a.href)})",
            "(() => { [...document.querySelectorAll('#metaboliteCurationPagination button')].find(b => b.textContent === 'Next').click(); return {rows: document.querySelectorAll('#metaboliteCurationRows tr').length, page: document.querySelector('#metaboliteCurationPagination').textContent, url: location.search}; })()",
            "(() => { document.querySelector('#metaboliteCurationClear').click(); return {count: document.querySelector('#metaboliteCurationResultCount').textContent, rows: document.querySelectorAll('#metaboliteCurationRows tr').length, url: location.search}; })()",
            "performance.getEntriesByType('resource')"
            ".filter(entry => !entry.name.includes('/favicon.ico'))"
            ".map(entry => entry.name).sort()",
        ]
        _, resources_before, _, filtered, second_page, cleared, resources_after = asyncio.run(
            _evaluate(websocket_url, expressions)
        )

        assert filtered["count"] == "60 matching of 61 active decisions."
        assert filtered["type"] == "Equivalence deny (60)"
        assert set(filtered["sources"]) == {"CHEBI (60)", "HMDB (60)"}
        assert filtered["batches"] == ["John legacy (60)"]
        assert set(filtered["curators"]) == {
            "John Braisted (60)", "Keith Kelleher (1)",
        }
        assert filtered["rows"] == 50
        assert "Page 1 of 2" in filtered["page"]
        assert "curator=John+Braisted" in filtered["url"]
        assert all("GitHub PR #15 by KeithKelleher" in text for text in filtered["evidence"])
        assert "https://github.com/ncats/RaMP-DB/pull/15" in filtered["evidenceLinks"]
        assert not any(url.startswith("javascript:") for url in filtered["evidenceLinks"])

        assert second_page["rows"] == 10
        assert "Page 2 of 2" in second_page["page"]
        assert "page=2" in second_page["url"]

        assert cleared == {
            "count": "61 matching of 61 active decisions.",
            "rows": 50,
            "url": "",
        }
        assert resources_after == resources_before
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
        server.shutdown()
        server.server_close()
