import gzip

import pytest

from src.id_resolvers.hcop_ortholog_gene_resolver import HCOPOrthologGeneResolver
from src.id_resolvers.node_normalizer import TranslatorNodeNormResolver
from src.interfaces.id_resolver import IdMatch
from src.models.node import Node
from src.registry.fetchers import MaterializedDataset


def _write_hcop_file(path, rows):
    header = "\t".join([
        "ortholog_species",
        "human_entrez_gene",
        "human_ensembl_gene",
        "hgnc_id",
        "human_name",
        "human_symbol",
        "human_chr",
        "human_assert_ids",
        "ortholog_species_entrez_gene",
        "ortholog_species_ensembl_gene",
        "ortholog_species_db_id",
        "ortholog_species_name",
        "ortholog_species_symbol",
        "ortholog_species_chr",
        "ortholog_species_assert_ids",
        "support",
    ])
    with gzip.open(path, "wt") as handle:
        handle.write(header + "\n")
        for row in rows:
            handle.write("\t".join(row) + "\n")


def _dataset_for_file(path):
    return MaterializedDataset(
        source="hcop",
        dataset="human_all_sixteen_column",
        version="test",
        version_date=None,
        download_date=None,
        snapshot_id="hcop:human_all_sixteen_column:test",
        manifest_uri="s3://ifx-registry/sources/hcop/human_all_sixteen_column/test/manifest.yaml",
        manifest={"files": [{"path": path.name}]},
        local_dir=path.parent,
    )


def _resolver_snapshot(path, *, accepted_species=None, drop_blank_ortholog_identity=True):
    return MaterializedDataset(
        source="hcop",
        dataset="hcop_ortholog_genes",
        version="test",
        version_date=None,
        download_date=None,
        snapshot_id="hcop:hcop_ortholog_genes:test",
        manifest_uri="s3://ifx-registry/resolvers/hcop/hcop_ortholog_genes/test/manifest.yaml",
        manifest={
            "kind": "resolver_snapshot",
            "definition": {
                "options": {
                    "accepted_species": accepted_species,
                    "drop_blank_ortholog_identity": drop_blank_ortholog_identity,
                },
            },
            "resolved_inputs": {
                "data_source": "hcop:human_all_sixteen_column:test",
            },
        },
        local_dir=path.parent,
        resolver_inputs={"data_source": _dataset_for_file(path)},
    )


def test_hcop_ortholog_gene_resolver_is_skip_only(tmp_path, monkeypatch):
    hcop_path = tmp_path / "hcop.tsv.gz"
    _write_hcop_file(hcop_path, [])
    monkeypatch.setattr(
        HCOPOrthologGeneResolver,
        "_post_to_node_normalizer",
        lambda self, curies: {},
    )

    resolver = HCOPOrthologGeneResolver(
        resolver_snapshot=_resolver_snapshot(hcop_path),
        types=["OrthologGene"],
    )

    assert resolver.no_match_behavior.value == "Skip"

    with pytest.raises(ValueError):
        HCOPOrthologGeneResolver(
            resolver_snapshot=_resolver_snapshot(hcop_path),
            types=["OrthologGene"],
            no_match_behavior="Allow",
        )


def test_hcop_ortholog_gene_resolver_preloads_allowed_ids_from_filtered_rows(tmp_path, monkeypatch):
    hcop_path = tmp_path / "hcop.tsv.gz"
    _write_hcop_file(hcop_path, [
        ["10090", "-", "-", "-", "-", "-", "-", "-", "11", "ENSMUSG00000000011", "MGI:11", "Mouse gene 11", "Gene11", "-", "-", "OMA, Ensembl"],
        ["10090", "-", "-", "-", "-", "-", "-", "-", "22", "ENSMUSG00000000022", "MGI:22", "Mouse gene 22", "Gene22", "-", "-", "Ensembl"],
        ["10116", "-", "-", "-", "-", "-", "-", "-", "33", "ENSRNOG00000000033", "RGD:33", "Rat gene 33", "Gene33", "-", "-", "OMA, EggNOG"],
        ["10090", "-", "-", "-", "-", "-", "-", "-", "44", "ENSMUSG00000000044", "MGI:44", "-", "-", "-", "-", "OMA, EggNOG"],
    ])

    def fake_post(self, curies):
        mapping = {
            "MGI:11": {"id": {"identifier": "NCBIGene:11"}, "equivalent_identifiers": [{"identifier": "MGI:11"}, {"identifier": "NCBIGene:11"}]},
            "NCBIGene:11": {"id": {"identifier": "NCBIGene:11"}, "equivalent_identifiers": [{"identifier": "MGI:11"}, {"identifier": "NCBIGene:11"}]},
            "ENSEMBL:ENSMUSG00000000011": {"id": {"identifier": "NCBIGene:11"}, "equivalent_identifiers": [{"identifier": "ENSEMBL:ENSMUSG00000000011"}, {"identifier": "NCBIGene:11"}]},
            "MGI:22": {"id": {"identifier": "NCBIGene:22"}, "equivalent_identifiers": [{"identifier": "MGI:22"}, {"identifier": "NCBIGene:22"}]},
            "NCBIGene:22": {"id": {"identifier": "NCBIGene:22"}, "equivalent_identifiers": [{"identifier": "MGI:22"}, {"identifier": "NCBIGene:22"}]},
            "ENSEMBL:ENSMUSG00000000022": {"id": {"identifier": "NCBIGene:22"}, "equivalent_identifiers": [{"identifier": "ENSEMBL:ENSMUSG00000000022"}, {"identifier": "NCBIGene:22"}]},
        }
        return {curie: mapping.get(curie) for curie in curies}

    monkeypatch.setattr(HCOPOrthologGeneResolver, "_post_to_node_normalizer", fake_post)

    resolver = HCOPOrthologGeneResolver(
        resolver_snapshot=_resolver_snapshot(
            hcop_path,
            accepted_species=["10090"],
            drop_blank_ortholog_identity=True,
        ),
        types=["OrthologGene"],
    )

    assert resolver.allowed_canonical_ids == {"NCBIGene:11", "NCBIGene:22"}


def test_hcop_ortholog_gene_resolver_keeps_all_resolved_canonical_ids_from_hcop_row(tmp_path, monkeypatch):
    hcop_path = tmp_path / "hcop.tsv.gz"
    _write_hcop_file(hcop_path, [
        ["10090", "-", "-", "-", "-", "-", "-", "-", "55", "ENSMUSG00000000055", "MGI:55", "Mouse gene 55", "Gene55", "-", "-", "OMA"],
    ])

    def fake_post(self, curies):
        mapping = {
            "MGI:55": {"id": {"identifier": "NCBIGene:55"}, "equivalent_identifiers": [{"identifier": "MGI:55"}, {"identifier": "NCBIGene:55"}]},
            "NCBIGene:55": {"id": {"identifier": "NCBIGene:55"}, "equivalent_identifiers": [{"identifier": "NCBIGene:55"}]},
            "ENSEMBL:ENSMUSG00000000055": {"id": {"identifier": "ENSEMBL:ENSMUSG00000000055"}, "equivalent_identifiers": [{"identifier": "ENSEMBL:ENSMUSG00000000055"}]},
        }
        return {curie: mapping.get(curie) for curie in curies}

    monkeypatch.setattr(HCOPOrthologGeneResolver, "_post_to_node_normalizer", fake_post)

    resolver = HCOPOrthologGeneResolver(
        resolver_snapshot=_resolver_snapshot(hcop_path, accepted_species=["10090"]),
        types=["OrthologGene"],
    )

    assert resolver.allowed_canonical_ids == {"NCBIGene:55", "ENSEMBL:ENSMUSG00000000055"}


def test_hcop_ortholog_gene_resolver_filters_node_norm_matches_to_hcop_scope(tmp_path, monkeypatch):
    hcop_path = tmp_path / "hcop.tsv.gz"
    _write_hcop_file(hcop_path, [
        ["10090", "-", "-", "-", "-", "-", "-", "-", "11", "ENSMUSG00000000011", "MGI:11", "Mouse gene 11", "Gene11", "-", "-", "OMA"],
    ])

    monkeypatch.setattr(
        HCOPOrthologGeneResolver,
        "_post_to_node_normalizer",
        lambda self, curies: {
            curie: {"id": {"identifier": "NCBIGene:11"}, "equivalent_identifiers": [{"identifier": curie}, {"identifier": "NCBIGene:11"}]}
            for curie in curies
        },
    )

    def fake_parent_resolve(self, input_nodes):
        return {
            "MGI:11": [IdMatch(input="MGI:11", match="NCBIGene:11", equivalent_ids=["MGI:11", "NCBIGene:11"])],
            "MGI:22": [IdMatch(input="MGI:22", match="NCBIGene:22", equivalent_ids=["MGI:22", "NCBIGene:22"])],
        }

    monkeypatch.setattr(TranslatorNodeNormResolver, "resolve_internal", fake_parent_resolve)

    resolver = HCOPOrthologGeneResolver(
        resolver_snapshot=_resolver_snapshot(hcop_path, accepted_species=["10090"]),
        types=["OrthologGene"],
    )

    results = resolver.resolve_internal([Node(id="MGI:11"), Node(id="MGI:22")])

    assert results["MGI:11"][0].match == "NCBIGene:11"
    assert results["MGI:22"] == []
