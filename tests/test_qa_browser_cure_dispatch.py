from src.qa_browser import app as qa_app
from src.qa_browser.app import (
    _build_cure_case_url,
    _get_document_template,
)
from src.qa_browser.registry_usage import extract_registry_datasets, load_graph_registry_usage_cached, with_graph_usages


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
        ttl_seconds=qa_app._REGISTRY_CATALOG_TTL_SECONDS,
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
