def build_cure_case_report_url(report_id: str | None, form_type: str | None) -> str | None:
    if not report_id:
        return None
    route_slug = {
        "pasc": "long-covid",
        "rasopathies": "rasopathies",
    }.get((form_type or "").strip().lower())
    if not route_slug:
        return None
    return f"https://cure.ncats.io/explore/{route_slug}/case-reports/case-details/{report_id}"
