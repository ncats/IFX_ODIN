import json

from src.qa_browser import app as qa_app
from src.qa_browser.pathway_id_graph import _release_version_from_path


def _write_graph(root, version):
    graph = root / f"v{version}" / "app_graph"
    graph.mkdir(parents=True)
    # Reproduce the historical producer bug: both manifests claimed 0.1.0.
    (graph / "manifest.json").write_text(json.dumps({"version": "0.1.0"}))
    (graph / "pathway_nodes.tsv").write_text("ncats_pathway_id\n")
    (graph / "pathway_edges.tsv").write_text("source_id\ttarget_id\n")
    return graph


def test_release_version_is_derived_from_version_directory(tmp_path):
    graph = _write_graph(tmp_path, "1.2.3")
    assert _release_version_from_path(graph) == "1.2.3"


def test_version_discovery_does_not_collapse_legacy_manifests(tmp_path, monkeypatch):
    _write_graph(tmp_path, "1.0.0")
    current = _write_graph(tmp_path, "1.0.4")
    monkeypatch.setattr(qa_app, "_pathway_graph_dir", str(current))
    monkeypatch.setattr(qa_app, "PATHWAY_APP_GRAPH_BUNDLED_DIR", tmp_path / "missing")

    versions = qa_app._discover_pathway_versions()

    assert [row["version"] for row in versions] == ["1.0.0", "1.0.4"]
    assert [row["version"] for row in versions if row["current"]] == ["1.0.4"]
