from pathlib import Path

from src.constants import Prefix
from src.id_resolvers.ensembl_gene_resolver import EnsemblGeneResolver
from src.models.node import EquivalentId
from tests.registry_fakes import registry_dataset


def _id(value: str, prefix: Prefix) -> str:
    return EquivalentId(id=value, type=prefix).id_str()


def test_ensembl_resolver_uses_pinned_registry_dataset(
    tmp_path: Path,
    monkeypatch,
) -> None:
    source_file = tmp_path / "gene_transcript_identifiers.csv"
    source_file.write_text(
        "Gene stable ID,HGNC ID,NCBI gene (formerly Entrezgene) ID,Gene name\n"
        "ENSG00000141510,HGNC:11998,7157,TP53\n"
        "ENSG00000141510,HGNC:11998,7157,TP53\n",
        encoding="utf-8",
    )
    data_source = registry_dataset(
        tmp_path,
        source_file.name,
        source="ensembl",
        dataset="human_biomart",
        version="116-export1",
    )
    cache_file = tmp_path / "ensembl-resolver.sqlite"
    monkeypatch.setattr(
        EnsemblGeneResolver,
        "cache_location",
        lambda self: str(cache_file),
    )

    resolver = EnsemblGeneResolver(data_source=data_source, types=["Gene"])

    expected = {
        (_id("ENSG00000141510", Prefix.ENSEMBL),) * 2 + ("exact",),
        (
            _id("ENSG00000141510", Prefix.ENSEMBL),
            _id("11998", Prefix.HGNC),
            Prefix.HGNC.value,
        ),
        (
            _id("ENSG00000141510", Prefix.ENSEMBL),
            _id("7157", Prefix.NCBIGene),
            Prefix.NCBIGene.value,
        ),
        (
            _id("ENSG00000141510", Prefix.ENSEMBL),
            _id("TP53", Prefix.Symbol),
            Prefix.Symbol.value,
        ),
    }
    actual = {(pair.id, pair.match, pair.type) for pair in resolver.matching_ids()}
    assert actual == expected
    assert resolver.get_version_info() == (
        "ensembl:human_biomart:116-export1:ensembl-gene-resolver-1"
    )
    assert cache_file.is_file()


def test_ensembl_resolver_requires_the_registry_file(tmp_path: Path) -> None:
    data_source = registry_dataset(
        tmp_path,
        "some_other_file.csv",
        source="ensembl",
        dataset="human_biomart",
        version="116-export1",
    )

    try:
        EnsemblGeneResolver(data_source=data_source, types=["Gene"])
    except FileNotFoundError as error:
        assert "gene_transcript_identifiers.csv" in str(error)
    else:
        raise AssertionError("resolver accepted a dataset without its required file")
