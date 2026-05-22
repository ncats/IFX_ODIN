from src.qa_browser.app import _build_cure_case_url, _get_document_template


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

