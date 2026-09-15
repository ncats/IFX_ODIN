from src.qa_browser import app as qa_app
from src.qa_browser.app import _build_cure_case_url, _get_document_template
from src.qa_browser.build_provenance import extract_build_inputs


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


def test_extract_build_inputs_combines_adapter_and_direct_resolver_usage():
    inputs = extract_build_inputs({
        "registry_datasets": [{
            "kind": "source_snapshot",
            "source": "cure",
            "dataset": "curated_concepts",
            "version": "2026-05-14",
            "snapshot_id": "cure:curated_concepts:2026-05-14",
            "usages": ["adapter:CureIdCuratedConceptsAdapter"],
        }],
        "resolver_metadata": {
            "by_type": {
                "Gene": {
                    "label": "cure_id_labels",
                    "kwargs": {
                        "data_source": {
                            "kind": "source_snapshot",
                            "source": "cure",
                            "dataset": "curated_concepts",
                            "version": "2026-05-14",
                            "snapshot_id": "cure:curated_concepts:2026-05-14",
                        },
                    },
                },
            },
        },
    })

    assert inputs == [{
        "source": "cure",
        "dataset": "curated_concepts",
        "version": "2026-05-14",
        "version_date": None,
        "download_date": None,
        "kind": "source_snapshot",
        "snapshot_id": "cure:curated_concepts:2026-05-14",
        "manifest_uri": None,
        "usages": ["adapter:CureIdCuratedConceptsAdapter", "resolver:cure_id_labels"],
    }]


def test_extract_build_inputs_keeps_same_snapshot_id_in_separate_kinds():
    snapshot_id = "example:records:1"

    inputs = extract_build_inputs({
        "registry_datasets": [
            {"kind": "source_snapshot", "snapshot_id": snapshot_id},
            {"kind": "derived_snapshot", "snapshot_id": snapshot_id},
        ],
    })

    assert [(entry["kind"], entry["snapshot_id"]) for entry in inputs] == [
        ("derived_snapshot", snapshot_id),
        ("source_snapshot", snapshot_id),
    ]


def test_qa_browser_has_no_registry_routes():
    paths = {route.path for route in qa_app.app.routes}
    assert not any(path == "/registry" or path.startswith("/registry/") for path in paths)
