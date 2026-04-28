from pathlib import Path

from src.input_adapters.tiga.tiga import TIGAAdapter
from src.models.disease import Disease
from src.models.protein import Protein
from src.models.tiga import GwasTrait, GwasTraitDiseaseEdge, ProteinGwasTraitEdge


def _write_fixture(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def test_tiga_adapter_emits_traits_and_edges(tmp_path):
    stats_path = tmp_path / "tiga_stats.tsv"
    provenance_path = tmp_path / "tiga_provenance.tsv"
    version_path = tmp_path / "tiga_version.tsv"

    _write_fixture(
        stats_path,
        (
            "ensemblId\tefoId\ttrait\tn_study\tn_snp\tn_snpw\tgeneNtrait\tgeneNstudy\ttraitNgene\ttraitNstudy\t"
            "pvalue_mlog_median\tpvalue_mlog_max\tor_median\tn_beta\tstudy_N_mean\trcras\tgeneSymbol\tTDL\tgeneFamily\tgeneIdgList\tgeneName\tmeanRank\tmeanRankScore\n"
            "ENSG0001\tEFO_0001\tTrait One\t2\t3\t1.5\t4\t5\t6\t7\t8.1\t9.2\tNA\t1\t12345.6\t0.25\tGENE1\tTbio\tKinase\tTRUE\tGene One\t10.5\t99.1\n"
        ),
    )
    _write_fixture(
        provenance_path,
        (
            "ensemblId\tTRAIT_URI\tSTUDY_ACCESSION\tPUBMEDID\tefoId\n"
            "ENSG0001\thttp://example.org/EFO_0001\tGCST1\t111\tEFO_0001\n"
            "ENSG0001\thttp://example.org/EFO_0001\tGCST2\t222\tEFO_0001\n"
        ),
    )
    _write_fixture(
        version_path,
        "version\tversion_date\tdownload_date\n20260120\t2026-02-28\t2026-04-28\n",
    )

    adapter = TIGAAdapter(
        stats_file_path=str(stats_path),
        provenance_file_path=str(provenance_path),
        version_file_path=str(version_path),
    )

    batches = list(adapter.get_all())

    assert len(batches) == 3

    trait = batches[0][0]
    trait_disease_edge = batches[1][0]
    edge = batches[2][0]

    assert isinstance(trait, GwasTrait)
    assert trait.id == "EFO_0001"
    assert trait.name == "Trait One"
    assert trait.trait_uri == "http://example.org/EFO_0001"

    assert isinstance(trait_disease_edge, GwasTraitDiseaseEdge)
    assert trait_disease_edge.start_node.id == trait.id
    assert isinstance(trait_disease_edge.end_node, Disease)
    assert trait_disease_edge.end_node.id == "EFO:0001"
    assert trait_disease_edge.end_node.name == "Trait One"

    assert isinstance(edge.start_node, Protein)
    assert edge.start_node.id == "ENSEMBL:ENSG0001"
    assert isinstance(edge, ProteinGwasTraitEdge)
    assert edge.end_node.id == trait.id
    assert len(edge.details) == 1
    assert edge.disease_ids == []
    detail = edge.details[0]
    assert detail.source == "TIGA"
    assert detail.ensg == "ENSG0001"
    assert detail.pvalue_mlog_max == 9.2
    assert detail.or_median is None
    assert detail.gene_idg_list is True
    assert len(detail.provenance_details) == 2
    assert detail.provenance_details[0].study_acc == "GCST1"
    assert detail.provenance_details[1].pubmedid == 222


def test_tiga_adapter_honors_max_rows(tmp_path):
    stats_path = tmp_path / "tiga_stats.tsv"
    provenance_path = tmp_path / "tiga_provenance.tsv"

    _write_fixture(
        stats_path,
        (
            "ensemblId\tefoId\ttrait\tn_study\tn_snp\tn_snpw\tgeneNtrait\tgeneNstudy\ttraitNgene\ttraitNstudy\t"
            "pvalue_mlog_median\tpvalue_mlog_max\tor_median\tn_beta\tstudy_N_mean\trcras\tgeneSymbol\tTDL\tgeneFamily\tgeneIdgList\tgeneName\tmeanRank\tmeanRankScore\n"
            "ENSG0001\tEFO_0001\tTrait One\t1\t1\t1\t1\t1\t1\t1\t1\t1\tNA\t1\t1\t0\tGENE1\tTbio\tNA\tFALSE\tGene One\t1\t1\n"
            "ENSG0002\tOBA_0002\tTrait Two\t1\t1\t1\t1\t1\t1\t1\t1\t1\tNA\t1\t1\t0\tGENE2\tTdark\tNA\tFALSE\tGene Two\t1\t1\n"
        ),
    )
    _write_fixture(
        provenance_path,
        (
            "ensemblId\tTRAIT_URI\tSTUDY_ACCESSION\tPUBMEDID\tefoId\n"
            "ENSG0001\thttp://example.org/EFO_0001\tGCST1\t111\tEFO_0001\n"
            "ENSG0002\thttp://example.org/OBA_0002\tGCST2\t222\tOBA_0002\n"
        ),
    )

    adapter = TIGAAdapter(
        stats_file_path=str(stats_path),
        provenance_file_path=str(provenance_path),
        max_rows=1,
    )

    batches = list(adapter.get_all())
    traits = [obj for obj in batches[0] if isinstance(obj, GwasTrait)]
    trait_disease_edges = [obj for obj in batches[1] if isinstance(obj, GwasTraitDiseaseEdge)]
    edges = [obj for obj in batches[2] if isinstance(obj, ProteinGwasTraitEdge)]

    assert len(traits) == 1
    assert len(trait_disease_edges) == 1
    assert len(edges) == 1
    assert traits[0].id == "EFO_0001"
