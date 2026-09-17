from types import SimpleNamespace

from jinja2 import Environment, FileSystemLoader


def _templates() -> Environment:
    environment = Environment(loader=FileSystemLoader("src/qa_browser/templates"))
    environment.globals["root_path"] = "/odin-qa"
    return environment


def _request(path: str) -> SimpleNamespace:
    return SimpleNamespace(scope={"path": path})


def test_harmonization_home_lists_only_harmonizer_destinations():
    rendered = _templates().get_template("home.html").render(
        request=_request("/"),
        style_version="test",
    )

    assert rendered.count('class="db-card workflow-card') == 6
    assert "Metabolite Harmonization Studio" in rendered
    assert "Disease Harmonizer Explorer" in rendered
    assert "Target Harmonizer Explorer" in rendered
    assert "Variant Harmonizer Explorer" in rendered
    assert "Drug Harmonizer Explorer" in rendered
    assert "CURE-ID Entity Resolver" in rendered
    assert 'href="/odin-qa/cure-entity-resolver"' in rendered
    assert "Pounce Submission" not in rendered
    assert "Database Browser" not in rendered
    assert 'href="/" aria-label="Return to the Informatics Development Sandbox"' in rendered


def test_global_breadcrumb_separates_frontpage_tools_from_harmonization():
    template = _templates().get_template("base.html")

    pounce = template.render(request=_request("/pounce/validate"), style_version="test")
    harmonizer = template.render(request=_request("/target-id-qa"), style_version="test")

    assert '<a href="/">Home</a>' in pounce
    assert '<a href="/odin-qa/">Harmonization</a>' not in pounce
    assert '<a href="/odin-qa/">Harmonization</a>' in harmonizer
