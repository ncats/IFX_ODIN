from pathlib import Path

from src.input_adapters.jensenlab.diseases import JensenLabDiseasesAdapter
from src.models.disease import Disease, ProteinDiseaseEdge
from src.models.protein import Protein


def _write_fixture(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def test_jensenlab_diseases_adapter_emits_diseases_and_edges(tmp_path):
    knowledge_path = tmp_path / "knowledge.tsv"
    experiments_path = tmp_path / "experiments.tsv"
    textmining_path = tmp_path / "textmining.tsv"
    version_path = tmp_path / "version.tsv"

    _write_fixture(
        knowledge_path,
        "ENSP0001\tGENE1\tDOID:1\tDisease One\tUniProtKB-KW\tCURATED\t4\n",
    )
    _write_fixture(
        experiments_path,
        "ENSP0001\tGENE1\tDOID:1\tDisease One\tTIGA\tMeanRankScore = 36\t1.004\n",
    )
    _write_fixture(
        textmining_path,
        "18S_rRNA\t18S_rRNA\tICD10:A01\tDisease Two\t7.244\t3.622\thttps://example.org/detail\n",
    )
    _write_fixture(
        version_path,
        "version\tversion_date\n\t2026-03-17\n",
    )

    adapter = JensenLabDiseasesAdapter(
        knowledge_file_path=str(knowledge_path),
        experiments_file_path=str(experiments_path),
        textmining_file_path=str(textmining_path),
        version_file_path=str(version_path),
    )

    batches = list(adapter.get_all())

    assert len(batches) == 2

    diseases = batches[0]
    edges = batches[1]

    assert {type(obj) for obj in diseases} == {Disease}
    assert {d.id for d in diseases} == {"DOID:1", "ICD10:A01"}

    assert len(edges) == 3
    assert all(isinstance(edge, ProteinDiseaseEdge) for edge in edges)
    assert all(isinstance(edge.start_node, Protein) for edge in edges)

    knowledge_edge = edges[0]
    assert knowledge_edge.start_node.id == "ENSEMBL:ENSP0001"
    assert knowledge_edge.end_node.id == "DOID:1"
    assert knowledge_edge.details[0].source == "JensenLab Knowledge UniProtKB-KW"
    assert knowledge_edge.details[0].evidence_terms == ["CURATED"]
    assert knowledge_edge.details[0].confidence == 4.0

    experiments_edge = edges[1]
    assert experiments_edge.details[0].source == "JensenLab Experiment TIGA"
    assert experiments_edge.details[0].evidence_terms == ["MeanRankScore = 36"]
    assert experiments_edge.details[0].confidence == 1.004

    textmining_edge = edges[2]
    assert textmining_edge.start_node.id == "ENSEMBL:18S_rRNA"
    assert textmining_edge.end_node.id == "ICD10:A01"
    assert textmining_edge.details[0].source == "JensenLab Text Mining"
    assert textmining_edge.details[0].zscore == 7.244
    assert textmining_edge.details[0].confidence == 3.622
    assert textmining_edge.details[0].url == "https://example.org/detail"


def test_jensenlab_diseases_adapter_honors_max_rows_per_file(tmp_path):
    knowledge_path = tmp_path / "knowledge.tsv"
    experiments_path = tmp_path / "experiments.tsv"
    textmining_path = tmp_path / "textmining.tsv"

    _write_fixture(
        knowledge_path,
        (
            "ENSP0001\tGENE1\tDOID:1\tDisease One\tUniProtKB-KW\tCURATED\t4\n"
            "ENSP0002\tGENE2\tDOID:2\tDisease Two\tMedlinePlus\tCURATED\t3\n"
        ),
    )
    _write_fixture(
        experiments_path,
        (
            "ENSP0003\tGENE3\tDOID:3\tDisease Three\tTIGA\tMeanRankScore = 36\t1.004\n"
            "ENSP0004\tGENE4\tDOID:4\tDisease Four\tTIGA\tMeanRankScore = 12\t0.500\n"
        ),
    )
    _write_fixture(
        textmining_path,
        (
            "ENSP0005\tGENE5\tDOID:5\tDisease Five\t7.244\t3.622\thttps://example.org/1\n"
            "ENSP0006\tGENE6\tDOID:6\tDisease Six\t6.000\t2.100\thttps://example.org/2\n"
        ),
    )

    adapter = JensenLabDiseasesAdapter(
        knowledge_file_path=str(knowledge_path),
        experiments_file_path=str(experiments_path),
        textmining_file_path=str(textmining_path),
        max_rows=1,
    )

    batches = list(adapter.get_all())
    diseases = batches[0]
    edges = batches[1]

    assert len(diseases) == 3
    assert len(edges) == 3
    assert {edge.end_node.id for edge in edges} == {"DOID:1", "DOID:3", "DOID:5"}


def test_jensenlab_diseases_adapter_can_filter_textmining_by_zscore(tmp_path):
    knowledge_path = tmp_path / "knowledge.tsv"
    experiments_path = tmp_path / "experiments.tsv"
    textmining_path = tmp_path / "textmining.tsv"

    _write_fixture(
        knowledge_path,
        "ENSP0001\tGENE1\tDOID:1\tDisease One\tUniProtKB-KW\tCURATED\t4\n",
    )
    _write_fixture(
        experiments_path,
        "ENSP0002\tGENE2\tDOID:2\tDisease Two\tTIGA\tMeanRankScore = 36\t1.004\n",
    )
    _write_fixture(
        textmining_path,
        (
            "ENSP0003\tGENE3\tDOID:3\tDisease Three\t5.999\t3.0\thttps://example.org/low\n"
            "ENSP0004\tGENE4\tDOID:4\tDisease Four\t6.000\t3.1\thttps://example.org/high\n"
        ),
    )

    adapter = JensenLabDiseasesAdapter(
        knowledge_file_path=str(knowledge_path),
        experiments_file_path=str(experiments_path),
        textmining_file_path=str(textmining_path),
        textmining_min_zscore=6.0,
    )

    batches = list(adapter.get_all())
    diseases = batches[0]
    edges = batches[1]

    assert {d.id for d in diseases} == {"DOID:1", "DOID:2", "DOID:4"}
    assert {edge.end_node.id for edge in edges} == {"DOID:1", "DOID:2", "DOID:4"}
