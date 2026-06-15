from src.qa_browser import app as qa_app
from src.qa_browser.app import (
    _build_cure_case_url,
    _get_document_template,
)
from src.interfaces.id_resolver import IdMatch
from src.qa_browser.registry_usage import (
    extract_registry_datasets,
    extract_registry_graph,
    load_graph_registry_usage_cached,
    load_registry_graphs_cached,
    with_graph_usages,
)


class FakeResolverSnapshot:
    snapshot_id = "hcop:hcop_ortholog_genes:deps-test"
    manifest = {
        "definition": {
            "accepted_types": ["OrthologGene"],
            "class": "RejectPolicyResolver",
            "import": "/tmp/reject_policy_resolver.py",
        }
    }


class FakeCursorDb:
    def __init__(self, etl_metadata):
        self.etl_metadata = etl_metadata
        self.aql = self

    def has_collection(self, name):
        return name == "metadata_store"

    def execute(self, query):
        return [self.etl_metadata]


class FakeSysDb:
    def databases(self):
        return ["_system", "cure_pasc", "cure_rasopathies"]


class FakeResolver:
    def resolve_internal(self, nodes):
        return {
            node.id: [
                IdMatch(
                    input=node.id,
                    match=f"{node.__class__.__name__}:{node.name}",
                    equivalent_ids=[node.text],
                    context=[node.__class__.__name__],
                )
            ]
            for node in nodes
        }

    def get_prefix_counts(self):
        return [
            {"prefix": "MONDO", "count": 10},
            {"prefix": "NCBIGene", "count": "unknown"},
            {"prefix": ""},
            "bad-row",
        ]

    def get_example_ids(self, limit=5):
        return ["MONDO:0005148", "", "DOID:9352"][:limit]


class RejectPolicyResolver(FakeResolver):
    def __init__(self, resolver_snapshot, types, **kwargs):
        if "no_match_behavior" in kwargs:
            raise ValueError("no_match_behavior should not be passed")
        if "multi_match_behavior" in kwargs:
            raise ValueError("multi_match_behavior should not be passed")
        self.resolver_snapshot = resolver_snapshot
        self.types = types


def test_cure_case_report_template_accepts_refresh_database_names():
    assert _get_document_template("cure", "CaseReport") == "cure_case_report_document.html"
    assert _get_document_template("cure_pasc", "CaseReport") == "cure_case_report_document.html"
    assert _get_document_template("cure_pasc_2", "CaseReport") == "cure_case_report_document.html"
    assert _get_document_template("cure_rasopathies", "CaseReport") == "cure_case_report_document.html"
    assert _get_document_template("cure_rasopathies_2", "CaseReport") == "cure_case_report_document.html"
    assert _get_document_template("pharos", "CaseReport") == "document.html"


def test_cure_case_url_accepts_refresh_database_names():
    assert _build_cure_case_url(
        "cure_rasopathies_2",
        {"id": "report-1", "form_type": "rasopathies"},
    ) == "https://cure.ncats.io/explore/rasopathies/case-reports/case-details/report-1"
    assert _build_cure_case_url(
        "cure_pasc_2",
        {"id": "report-1", "form_type": "pasc"},
    ) == "https://cure.ncats.io/explore/long-covid/case-reports/case-details/report-1"


def test_extract_registry_datasets_from_etl_metadata():
    datasets = extract_registry_datasets({
        "registry_datasets": [{
            "source": "cure",
            "dataset": "case_reports",
            "version": "reports_20260612T182139Z",
            "snapshot_id": "cure:case_reports:reports_20260612T182139Z",
            "usages": ["adapter:CUREAdapter"],
        }],
        "resolver_metadata": {
            "by_type": {
                "Gene": {
                    "label": "cure_id_labels",
                    "kwargs": {
                        "resolver_snapshot": {
                            "source": "cure",
                            "dataset": "cure_id_labels",
                            "version": "deps-test",
                            "snapshot_id": "cure:cure_id_labels:deps-test",
                            "resolver_inputs": {
                                "data_source": {
                                    "source": "cure",
                                    "dataset": "curated_concepts",
                                    "version": "2026-05-14",
                                    "snapshot_id": "cure:curated_concepts:2026-05-14",
                                    "manifest_uri": "s3://ifx-registry/sources/cure/curated_concepts/2026-05-14/manifest.yaml",
                                }
                            },
                        },
                    },
                    "resolver_snapshot": {
                        "source": "cure",
                        "dataset": "cure_id_labels",
                        "version": "deps-test",
                        "snapshot_id": "cure:cure_id_labels:deps-test",
                        "resolver_inputs": {
                            "data_source": {
                                "source": "cure",
                                "dataset": "curated_concepts",
                                "version": "2026-05-14",
                                "snapshot_id": "cure:curated_concepts:2026-05-14",
                                "manifest_uri": "s3://ifx-registry/sources/cure/curated_concepts/2026-05-14/manifest.yaml",
                            }
                        },
                    },
                }
            }
        },
    })

    assert [dataset["snapshot_id"] for dataset in datasets] == [
        "cure:case_reports:reports_20260612T182139Z",
        "cure:curated_concepts:2026-05-14",
        "cure:cure_id_labels:deps-test",
    ]
    assert datasets[0]["usages"] == ["adapter:CUREAdapter"]
    assert datasets[1]["usages"] == ["resolver:cure_id_labels"]
    assert datasets[2]["usages"] == ["resolver:cure_id_labels"]


def test_extract_registry_datasets_marks_resolver_inputs_used_by_adapter_and_resolver():
    datasets = extract_registry_datasets({
        "registry_datasets": [{
            "source": "cure",
            "dataset": "curated_concepts",
            "version": "2026-05-14",
            "snapshot_id": "cure:curated_concepts:2026-05-14",
            "usages": ["adapter:CureIdCuratedConceptsAdapter"],
        }, {
            "source": "cure",
            "dataset": "cure_id_labels",
            "version": "deps-test",
            "snapshot_id": "cure:cure_id_labels:deps-test",
            "usages": ["resolver:cure_id_labels"],
            "resolver_inputs": {
                "data_source": {
                            "source": "cure",
                            "dataset": "curated_concepts",
                            "version": "2026-05-14",
                            "snapshot_id": "cure:curated_concepts:2026-05-14",
                            "manifest_uri": "s3://ifx-registry/sources/cure/curated_concepts/2026-05-14/manifest.yaml",
                }
            }
        }],
    })

    assert [dataset["snapshot_id"] for dataset in datasets] == [
        "cure:curated_concepts:2026-05-14",
        "cure:cure_id_labels:deps-test",
    ]
    assert datasets[0]["usages"] == [
        "adapter:CureIdCuratedConceptsAdapter",
        "resolver:cure_id_labels",
    ]


def test_load_graph_registry_usage_indexes_graphs_by_snapshot(monkeypatch):
    qa_app._registry_usage_cache.update({
        "loaded_at": 0.0,
        "usage_by_registry_id": None,
        "error": None,
    })
    monkeypatch.setattr(qa_app, "_credentials", {"user": "root", "password": "password"})

    graph_metadata = {
        "cure_pasc": {
            "registry_datasets": [{
                "source": "cure",
                "dataset": "case_reports",
                "version": "reports_20260612T182139Z",
                "snapshot_id": "cure:case_reports:reports_20260612T182139Z",
                "usages": ["adapter:CUREAdapter"],
            }]
        },
        "cure_rasopathies": {
            "registry_datasets": [{
                "source": "cure",
                "dataset": "case_reports",
                "version": "reports_20260612T182139Z",
                "snapshot_id": "cure:case_reports:reports_20260612T182139Z",
                "usages": ["adapter:RasopathiesAdapter"],
            }, {
                "source": "cure",
                "dataset": "curated_concepts",
                "version": "2026-05-14",
                "snapshot_id": "cure:curated_concepts:2026-05-14",
                "usages": ["resolver:cure_id_labels"],
            }]
        },
    }

    monkeypatch.setattr(qa_app, "get_sys_db", lambda: FakeSysDb())
    monkeypatch.setattr(qa_app, "get_db", lambda db_name: FakeCursorDb(graph_metadata[db_name]))

    usage_by_registry_id, error = load_graph_registry_usage_cached(
        credentials=qa_app._credentials,
        cache=qa_app._registry_usage_cache,
        ttl_seconds=qa_app._REGISTRY_USAGE_TTL_SECONDS,
        get_sys_db=qa_app.get_sys_db,
        get_db=qa_app.get_db,
    )

    assert error is None
    assert usage_by_registry_id == {
        "cure:case_reports:reports_20260612T182139Z": {
            "cure_pasc": ["adapter"],
            "cure_rasopathies": ["adapter"],
        },
        "cure:curated_concepts:2026-05-14": {
            "cure_rasopathies": ["resolver"],
        },
    }


def test_extract_registry_graph_builds_adapter_and_resolver_dependencies():
    graph = extract_registry_graph({
        "source_yaml": "./src/use_cases/cure/cure_rasopathies.yaml",
        "run_date": "2026-06-15T12:00:00",
        "registry_datasets": [{
            "kind": "source_snapshot",
            "source": "cure",
            "dataset": "case_reports",
            "version": "reports_20260612T182139Z",
            "snapshot_id": "cure:case_reports:reports_20260612T182139Z",
            "usages": ["adapter:CUREAdapter"],
        }, {
            "kind": "resolver_snapshot",
            "source": "cure",
            "dataset": "cure_id_labels",
            "version": "deps-test",
            "snapshot_id": "cure:cure_id_labels:deps-test",
            "usages": ["resolver:cure_id_labels"],
            "resolver_inputs": {
                "data_source": {
                    "kind": "source_snapshot",
                    "source": "cure",
                    "dataset": "curated_concepts",
                    "version": "2026-05-14",
                    "snapshot_id": "cure:curated_concepts:2026-05-14",
                }
            },
        }],
        "resolver_metadata": {
            "by_type": {
                "Gene": {
                    "label": "cure_id_labels",
                    "class": "CureIdLabelResolver",
                    "resolver_snapshot": {
                        "kind": "resolver_snapshot",
                        "source": "cure",
                        "dataset": "cure_id_labels",
                        "version": "deps-test",
                        "snapshot_id": "cure:cure_id_labels:deps-test",
                        "resolver_inputs": {
                            "data_source": {
                                "kind": "source_snapshot",
                                "source": "cure",
                                "dataset": "curated_concepts",
                                "version": "2026-05-14",
                                "snapshot_id": "cure:curated_concepts:2026-05-14",
                            }
                        },
                    },
                },
                "Drug": {
                    "label": "cure_id_labels",
                    "class": "CureIdLabelResolver",
                    "resolver_snapshot": {
                        "kind": "resolver_snapshot",
                        "source": "cure",
                        "dataset": "cure_id_labels",
                        "version": "deps-test",
                        "snapshot_id": "cure:cure_id_labels:deps-test",
                    },
                },
            }
        },
    }, "cure_rasopathies")

    assert graph["name"] == "cure_rasopathies"
    assert graph["adapters"][0]["name"] == "CUREAdapter"
    assert graph["adapters"][0]["datasets"][0]["snapshot_id"] == "cure:case_reports:reports_20260612T182139Z"
    assert graph["resolvers"][0]["name"] == "cure_id_labels"
    assert graph["resolvers"][0]["class"] == "CureIdLabelResolver"
    assert graph["resolvers"][0]["types"] == ["Drug", "Gene"]
    assert graph["resolvers"][0]["snapshot"]["snapshot_id"] == "cure:cure_id_labels:deps-test"
    assert graph["resolvers"][0]["inputs"][0]["snapshot_id"] == "cure:curated_concepts:2026-05-14"
    assert {node["kind"] for node in graph["lineage_nodes"]} == {"adapter", "data", "graph", "resolver"}
    assert {"from": "resolver:cure_id_labels", "to": "graph:cure_rasopathies", "label": "builds"} in graph["lineage_edges"]
    lineage_by_id = {node["id"]: node for node in graph["lineage_nodes"]}
    assert lineage_by_id["resolver:cure_id_labels"]["row"] < lineage_by_id["adapter:CUREAdapter"]["row"]
    assert lineage_by_id["registry:cure:curated_concepts:2026-05-14"]["row"] == lineage_by_id["resolver:cure_id_labels"]["row"]
    assert lineage_by_id["registry:cure:case_reports:reports_20260612T182139Z"]["row"] == lineage_by_id["adapter:CUREAdapter"]["row"]


def test_extract_registry_graph_handles_derived_source_dependencies():
    graph = extract_registry_graph({
        "registry_datasets": [{
            "kind": "derived_snapshot",
            "source": "surechembl",
            "dataset": "patent_family_mentions",
            "version": "2026-06-01",
            "snapshot_id": "surechembl:patent_family_mentions:2026-06-01",
            "usages": ["adapter:SureChEMBLPatentAdapter"],
            "derived_from": [{
                "kind": "source_snapshot",
                "source": "surechembl",
                "dataset": "patent_discovery",
                "version": "2026-06-01",
                "snapshot_id": "surechembl:patent_discovery:2026-06-01",
            }],
        }],
    }, "pharos")

    lineage_by_id = {node["id"]: node for node in graph["lineage_nodes"]}
    assert lineage_by_id["registry:surechembl:patent_discovery:2026-06-01"]["kind"] == "data"
    assert lineage_by_id["registry:surechembl:patent_family_mentions:2026-06-01"]["kind"] == "derived"
    assert lineage_by_id["registry:surechembl:patent_family_mentions:2026-06-01"]["level"] == 1
    assert lineage_by_id["registry:surechembl:patent_discovery:2026-06-01"]["row"] == lineage_by_id["adapter:SureChEMBLPatentAdapter"]["row"]
    assert lineage_by_id["registry:surechembl:patent_family_mentions:2026-06-01"]["row"] == lineage_by_id["adapter:SureChEMBLPatentAdapter"]["row"]
    assert {"from": "registry:surechembl:patent_discovery:2026-06-01", "to": "registry:surechembl:patent_family_mentions:2026-06-01", "label": "derived"} in graph["lineage_edges"]


def test_load_registry_graphs_lists_graph_metadata(monkeypatch):
    cache = {"loaded_at": 0.0, "graphs": None, "error": None}
    graph_metadata = {
        "cure_pasc": {
            "registry_datasets": [{
                "source": "cure",
                "dataset": "case_reports",
                "version": "reports_20260612T182139Z",
                "snapshot_id": "cure:case_reports:reports_20260612T182139Z",
                "usages": ["adapter:CUREAdapter"],
            }]
        },
        "cure_rasopathies": {
            "registry_datasets": [{
                "source": "cure",
                "dataset": "curated_concepts",
                "version": "2026-05-14",
                "snapshot_id": "cure:curated_concepts:2026-05-14",
                "usages": ["resolver:cure_id_labels"],
            }]
        },
    }

    graphs, error = load_registry_graphs_cached(
        credentials={"user": "root"},
        cache=cache,
        ttl_seconds=60,
        get_sys_db=lambda: FakeSysDb(),
        get_db=lambda db_name: FakeCursorDb(graph_metadata[db_name]),
    )

    assert error is None
    assert [graph["name"] for graph in graphs] == ["cure_pasc", "cure_rasopathies"]
    assert graphs[0]["adapters"][0]["name"] == "CUREAdapter"


def test_with_graph_usages_accepts_external_registration_id():
    entry = with_graph_usages(
        {
            "registration_id": "chembl:activity_database:chembl36",
            "source": "chembl",
            "dataset": "activity_database",
            "version": "chembl36",
        },
        {
            "chembl:activity_database:chembl36": {"pharos": ["adapter", "resolver"]},
        },
    )

    assert entry["registry_id"] == "chembl:activity_database:chembl36"
    assert entry["graph_usages"] == ["pharos"]
    assert entry["graph_usage_details"] == [{
        "graph": "pharos",
        "categories": ["adapter", "resolver"],
        "category_label": "adapter + resolver",
        "category_class": "adapter-resolver",
    }]


def test_resolver_api_constructs_named_minimal_nodes_for_input_type():
    results = qa_app._resolve_ids_for_type(FakeResolver(), "Gene", ["SYNGAP1"])

    assert results == [{
        "input": "SYNGAP1",
        "matches": [{
            "input": "SYNGAP1",
            "match": "Gene:SYNGAP1",
            "equivalent_ids": ["SYNGAP1"],
            "context": ["Gene"],
        }],
    }]


def test_resolver_api_normalizes_ids_from_string_payload():
    assert qa_app._normalize_resolver_api_ids({"ids": "NCBIGene:8831"}) == ["NCBIGene:8831"]
    assert qa_app._normalize_resolver_api_ids({"ids": [" A ", "", "B"]}) == ["A", "B"]


def test_resolver_api_serializes_prefix_counts():
    assert qa_app._resolver_prefix_counts_for_api(FakeResolver()) == [
        {"prefix": "MONDO", "count": 10},
        {"prefix": "NCBIGene", "count": None},
    ]


def test_resolver_api_serializes_example_ids():
    assert qa_app._resolver_example_ids_for_api(FakeResolver()) == [
        "MONDO:0005148",
        "DOID:9352",
    ]


def test_resolver_api_instantiates_without_etl_policy_kwargs(monkeypatch):
    qa_app._resolver_instance_cache.clear()
    monkeypatch.setattr(
        qa_app,
        "_materialize_resolver_snapshot_for_api",
        lambda source, resolver, version: FakeResolverSnapshot(),
    )
    monkeypatch.setattr(
        qa_app,
        "_load_class_from_definition",
        lambda definition: RejectPolicyResolver,
    )

    _snapshot, resolver, accepted_types, type_sensitive = qa_app._get_resolver_instance_for_api(
        "hcop",
        "hcop_ortholog_genes",
        "deps-test",
    )

    assert isinstance(resolver, RejectPolicyResolver)
    assert accepted_types == ["OrthologGene"]
    assert type_sensitive is False
