from src.id_resolvers.disease_resolver import DiseaseIdResolver
from src.interfaces.id_resolver import NoMatchBehavior
from src.models.disease import Disease


def _write_disease_ids(path):
    path.write_text(
        "\t".join([
            "ncats_disease_id",
            "standard_id",
            "standard_name",
            "nn_curie",
            "mondo_xref",
            "DOID_xref",
            "GARD_xref",
            "OMIM_xref",
            "medgen_xref",
            "is_rare",
        ])
        + "\n"
        + "\t".join([
            "1",
            "MONDO:0011308",
            "GRACILE syndrome",
            "MONDO:0011308",
            "MONDO:0011308",
            "DOID:0050435",
            "GARD:1",
            "OMIM:603358",
            "MEDGEN:123",
            "true",
        ])
        + "\n"
        + "\t".join([
            "2",
            "MEDGEN:999",
            "No MONDO disease",
            "MEDGEN:999",
            "",
            "",
            "",
            "",
            "MEDGEN:999",
            "false",
        ])
        + "\n"
        + "\t".join([
            "3",
            "MONDO:0000002",
            "Ambiguous xref disease",
            "MONDO:0000002",
            "MONDO:0000002",
            "",
            "",
            "OMIM:603358",
            "",
            "false",
        ])
        + "\n",
        encoding="utf-8",
    )


def test_disease_resolver_maps_gard_padded_id_to_standard_mondo(tmp_path):
    disease_ids = tmp_path / "disease_ids.tsv"
    cache = tmp_path / "disease_resolver.sqlite"
    _write_disease_ids(disease_ids)

    resolver = DiseaseIdResolver(
        file_path=str(disease_ids),
        cache_path=str(cache),
        types=["Disease"],
        no_match_behavior=NoMatchBehavior.Allow,
        multi_match_behavior="First",
    )

    entity_map = resolver.resolve_nodes([Disease(id="GARD:0000001")])
    resolved = resolver.parse_flat_node_list_from_map(entity_map)

    assert len(resolved) == 1
    assert resolved[0].id == "MONDO:0011308"
    assert {xref.id_str() for xref in resolved[0].xref} >= {
        "GARD:1",
        "MONDO:0011308",
        "OMIM:603358",
    }


def test_disease_resolver_uses_non_mondo_standard_id_when_no_mondo_exists(tmp_path):
    disease_ids = tmp_path / "disease_ids.tsv"
    cache = tmp_path / "disease_resolver.sqlite"
    _write_disease_ids(disease_ids)

    resolver = DiseaseIdResolver(
        file_path=str(disease_ids),
        cache_path=str(cache),
        types=["Disease"],
        no_match_behavior=NoMatchBehavior.Allow,
        multi_match_behavior="First",
    )

    entity_map = resolver.resolve_nodes([Disease(id="MEDGEN:999")])
    resolved = resolver.parse_flat_node_list_from_map(entity_map)

    assert len(resolved) == 1
    assert resolved[0].id == "MEDGEN:999"
    assert not getattr(resolved[0], "resolver_miss", False)


def test_disease_resolver_allows_unmatched_disease_ids(tmp_path):
    disease_ids = tmp_path / "disease_ids.tsv"
    cache = tmp_path / "disease_resolver.sqlite"
    _write_disease_ids(disease_ids)

    resolver = DiseaseIdResolver(
        file_path=str(disease_ids),
        cache_path=str(cache),
        types=["Disease"],
        no_match_behavior=NoMatchBehavior.Allow,
        multi_match_behavior="First",
    )

    entity_map = resolver.resolve_nodes([Disease(id="GARD:9999999")])
    resolved = resolver.parse_flat_node_list_from_map(entity_map)

    assert len(resolved) == 1
    assert resolved[0].id == "GARD:9999999"
    assert resolved[0].resolver_miss is True


def test_disease_resolver_prefers_exact_standard_id_over_xref_match(tmp_path):
    disease_ids = tmp_path / "disease_ids.tsv"
    cache = tmp_path / "disease_resolver.sqlite"
    _write_disease_ids(disease_ids)

    resolver = DiseaseIdResolver(
        file_path=str(disease_ids),
        cache_path=str(cache),
        types=["Disease"],
        no_match_behavior=NoMatchBehavior.Allow,
        multi_match_behavior="First",
    )

    entity_map = resolver.resolve_nodes([Disease(id="MONDO:0011308")])
    resolved = resolver.parse_flat_node_list_from_map(entity_map)

    assert len(resolved) == 1
    assert resolved[0].id == "MONDO:0011308"


def test_disease_resolver_can_fan_out_ambiguous_xref_matches(tmp_path):
    disease_ids = tmp_path / "disease_ids.tsv"
    cache = tmp_path / "disease_resolver.sqlite"
    _write_disease_ids(disease_ids)

    resolver = DiseaseIdResolver(
        file_path=str(disease_ids),
        cache_path=str(cache),
        types=["Disease"],
        no_match_behavior=NoMatchBehavior.Allow,
        multi_match_behavior="All",
    )

    entity_map = resolver.resolve_nodes([Disease(id="OMIM:603358")])
    resolved = resolver.parse_flat_node_list_from_map(entity_map)

    assert {node.id for node in resolved} == {"MONDO:0011308", "MONDO:0000002"}
