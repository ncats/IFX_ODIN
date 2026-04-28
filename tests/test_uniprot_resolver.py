from src.id_resolvers.uniprot_resolver import UniProtResolver
from src.models.protein import Protein


def test_uniprot_resolver_initializes_framework_fields():
    resolver = UniProtResolver(
        uniprot_json_path="./input_files/auto/uniprot/uniprot-human-reviewed.json.gz",
        types=["Protein"],
        no_match_behavior="Skip",
    )

    assert resolver.types == ["Protein"]
    assert resolver.no_match_behavior.value == "Skip"
    assert "UniProtKB:P12345" not in resolver.resolve_cache


def test_uniprot_resolver_returns_empty_list_for_unmatched_id():
    resolver = UniProtResolver(
        uniprot_json_path="./input_files/auto/uniprot/uniprot-human-reviewed.json.gz",
        types=["Protein"],
        no_match_behavior="Skip",
    )

    results = resolver.resolve_internal([Protein(id="UniProtKB:NOT_A_REAL_UNIPROT")])

    assert results["UniProtKB:NOT_A_REAL_UNIPROT"] == []


def test_uniprot_resolver_normalizes_isoform_to_base_accession():
    resolver = UniProtResolver(
        uniprot_json_path="./input_files/auto/uniprot/uniprot-human-reviewed.json.gz",
        types=["Protein"],
        no_match_behavior="Skip",
    )

    results = resolver.resolve_internal([Protein(id="UniProtKB:Q15149-9")])

    assert len(results["UniProtKB:Q15149-9"]) == 1
    assert results["UniProtKB:Q15149-9"][0].match == "UniProtKB:Q15149"


def test_uniprot_resolver_resolves_symbol_alias():
    resolver = UniProtResolver(
        uniprot_json_path="./input_files/auto/uniprot/uniprot-human-reviewed.json.gz",
        types=["Protein"],
        no_match_behavior="Skip",
    )

    results = resolver.resolve_internal([Protein(id="TP53")])

    assert len(results["TP53"]) == 1
    assert results["TP53"][0].match == "UniProtKB:P04637"
