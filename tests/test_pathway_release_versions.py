import json

from src.qa_browser import app as qa_app
from types import SimpleNamespace

from src.qa_browser.pathway_id_graph import (
    PathwayGraphData,
    _release_version_from_path,
    build_cross_entity_summary,
)


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


def test_cross_entity_summary_uses_association_and_edge_indexes():
    pathway = PathwayGraphData()
    pathway.nodes_by_id["IFXPathway:1"] = {"ncats_pathway_id": "IFXPathway:1"}
    pathway.edges_by_pathway["IFXPathway:1"] = [{
        "target_id": "IFXGene:1", "target_label": "GENE1"
    }]
    disease = SimpleNamespace(
        nodes=[{"ncats_disease_id": "IFXDisease:1", "consolidated_disease_name": "Disease"}],
        associations_by_ncats_id={"IFXDisease:1": [{"ncats_gene_id": "IFXGene:1"}]},
    )
    drug = SimpleNamespace(
        nodes=[{"drug_id": "IFXDrug:1", "standard_name": "Drug"}],
        edges_by_drug={"IFXDrug:1": [{
            "target_id": "IFXProtein:1", "target_label": "Gene one protein",
            "target_gene_id": "IFXGene:1", "target_symbol": "GENE1",
            "target_category": "biolink:Protein",
            "relation_kind": "drug_target",
        }]},
    )

    result = build_cross_entity_summary(
        pathway, "IFXPathway:1", disease_data=disease, drug_data=drug
    )

    assert result["disease_links"][0]["disease_id"] == "IFXDisease:1"
    assert result["drug_links"][0]["drug_id"] == "IFXDrug:1"
