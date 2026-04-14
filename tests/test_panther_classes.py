from pathlib import Path

from src.input_adapters.panther.panther_classes import PantherClassesAdapter
from src.models.panther_class import (
    PantherClass,
    PantherClassParentEdge,
    PantherFamily,
    PantherFamilyParentEdge,
    ProteinPantherClassEdge,
    ProteinPantherFamilyEdge,
)


def _write_fixture(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def test_panther_adapter_emits_family_and_class_graph(tmp_path):
    class_path = tmp_path / "Protein_Class_19.0"
    relationship_path = tmp_path / "Protein_class_relationship"
    sequence_path = tmp_path / "PTHR19.0_human"
    version_path = tmp_path / "panther_classes_version.tsv"

    _write_fixture(
        class_path,
        (
            "! version: 17.0\n"
            "! date: 1/11/2022\n"
            "#removed PC00255 because only one family in class\n"
            "#PC00255\t1.08.01.08.00\tTATA-binding transcription factor\tCommented out class\n"
            "PC00000\t1.00.00.00.00\tprotein class\t\n"
            "PC00197\t1.01.00.00.00\ttransmembrane signal receptor\tA receptor class\n"
            "PC00021\t1.01.01.00.00\tG-protein coupled receptor\tGPCR class\n"
        ),
    )
    _write_fixture(
        relationship_path,
        (
            "! version: 17.0\n"
            "PC00197\treceptor\tPC00000\tprotein class\t05\n"
            "PC00021\tGPCR\tPC00197\treceptor\t01\n"
        ),
    )
    _write_fixture(
        sequence_path,
        (
            "HUMAN|HGNC=1|UniProtKB=P11111\tP11111\tGENE1\tPTHR10000:SF1\tFAMILY ONE\tProtein One\t\t\t\t"
            "transmembrane signal receptor#PC00197;G-protein coupled receptor#PC00021\t\n"
            "HUMAN|HGNC=2|UniProtKB=P22222\tP22222\tGENE2\tPTHR10000:SF2\tFAMILY ONE\tProtein Two\t\t\t\t"
            "\t\n"
        ),
    )
    _write_fixture(
        version_path,
        "version\tversion_date\tdownload_date\n19.0\t2026-04-14\t2026-04-14\n",
    )

    adapter = PantherClassesAdapter(
        class_file_path=str(class_path),
        relationship_file_path=str(relationship_path),
        sequence_classification_file_path=str(sequence_path),
        version_file_path=str(version_path),
    )

    batches = list(adapter.get_all())

    family_nodes = batches[0]
    class_nodes = batches[1]
    family_parent_edges = batches[2]
    class_parent_edges = batches[3]
    family_edges = batches[4]
    class_edges = batches[5]

    assert {type(obj) for obj in family_nodes} == {PantherFamily}
    assert {node.id for node in family_nodes} == {
        "PANTHER.FAMILY:PTHR10000",
        "PANTHER.FAMILY:PTHR10000:SF1",
        "PANTHER.FAMILY:PTHR10000:SF2",
    }
    family_node_map = {node.id: node for node in family_nodes}
    assert family_node_map["PANTHER.FAMILY:PTHR10000"].level == "family"
    assert family_node_map["PANTHER.FAMILY:PTHR10000"].name == "FAMILY ONE"
    assert family_node_map["PANTHER.FAMILY:PTHR10000:SF1"].level == "subfamily"
    assert family_node_map["PANTHER.FAMILY:PTHR10000:SF1"].name is None

    assert {type(obj) for obj in class_nodes} == {PantherClass}
    assert {node.id for node in class_nodes} == {
        "PANTHER.CLASS:PC00000",
        "PANTHER.CLASS:PC00197",
        "PANTHER.CLASS:PC00021",
    }
    assert "PANTHER.CLASS:#PC00255" not in {node.id for node in class_nodes}

    assert all(isinstance(edge, PantherFamilyParentEdge) for edge in family_parent_edges)
    assert {(edge.start_node.id, edge.end_node.id) for edge in family_parent_edges} == {
        ("PANTHER.FAMILY:PTHR10000:SF1", "PANTHER.FAMILY:PTHR10000"),
        ("PANTHER.FAMILY:PTHR10000:SF2", "PANTHER.FAMILY:PTHR10000"),
    }

    assert all(isinstance(edge, PantherClassParentEdge) for edge in class_parent_edges)
    assert {(edge.start_node.id, edge.end_node.id) for edge in class_parent_edges} == {
        ("PANTHER.CLASS:PC00197", "PANTHER.CLASS:PC00000"),
        ("PANTHER.CLASS:PC00021", "PANTHER.CLASS:PC00197"),
    }

    assert all(isinstance(edge, ProteinPantherFamilyEdge) for edge in family_edges)
    assert {(edge.start_node.id, edge.end_node.id) for edge in family_edges} == {
        ("UniProtKB:P11111", "PANTHER.FAMILY:PTHR10000:SF1"),
        ("UniProtKB:P22222", "PANTHER.FAMILY:PTHR10000:SF2"),
    }

    assert all(isinstance(edge, ProteinPantherClassEdge) for edge in class_edges)
    assert {(edge.start_node.id, edge.end_node.id) for edge in class_edges} == {
        ("UniProtKB:P11111", "PANTHER.CLASS:PC00197"),
        ("UniProtKB:P11111", "PANTHER.CLASS:PC00021"),
    }

    version = adapter.get_version()
    assert version.version == "19.0"
    assert version.version_date.isoformat() == "2026-04-14"
    assert version.download_date.isoformat() == "2026-04-14"
