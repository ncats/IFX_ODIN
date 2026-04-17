import gzip

from src.input_adapters.string.string_ppi import StringPPIAdapter


def test_string_ppi_adapter_applies_default_cutoff_and_skips_self_pairs(tmp_path):
    data_path = tmp_path / "9606.protein.links.v12.0.txt.gz"
    version_path = tmp_path / "string_version.tsv"

    with gzip.open(data_path, "wt", encoding="utf-8") as handle:
        handle.write(
            "\n".join(
                [
                    "protein1 protein2 combined_score",
                    "9606.ENSP0001 9606.ENSP0002 399",
                    "9606.ENSP0001 9606.ENSP0001 900",
                    "9606.ENSP0002 9606.ENSP0003 400",
                    "9606.ENSP0003 9606.ENSP0004 700",
                ]
            )
        )

    version_path.write_text("version\tversion_date\n12.0\t2025-01-15\n", encoding="utf-8")

    adapter = StringPPIAdapter(
        file_path=str(data_path),
        version_file_path=str(version_path),
    )

    batches = list(adapter.get_all())
    edges = [edge for batch in batches for edge in batch]

    assert len(edges) == 2
    assert [(edge.start_node.id, edge.end_node.id, edge.score) for edge in edges] == [
        ("ENSEMBL:ENSP0002", "ENSEMBL:ENSP0003", [400]),
        ("ENSEMBL:ENSP0003", "ENSEMBL:ENSP0004", [700]),
    ]
    assert all(edge.sources == ["STRING"] for edge in edges)

    version = adapter.get_version()
    assert version.version == "12.0"
    assert version.version_date.isoformat() == "2025-01-15"


def test_string_ppi_adapter_supports_explicit_cutoff_override(tmp_path):
    data_path = tmp_path / "9606.protein.links.v12.0.txt"

    data_path.write_text(
        "\n".join(
            [
                "protein1 protein2 combined_score",
                "9606.ENSP0001 9606.ENSP0002 250",
                "9606.ENSP0002 9606.ENSP0003 300",
            ]
        ),
        encoding="utf-8",
    )

    adapter = StringPPIAdapter(
        file_path=str(data_path),
        score_cutoff=300,
    )

    batches = list(adapter.get_all())
    edges = [edge for batch in batches for edge in batch]

    assert len(edges) == 1
    assert edges[0].start_node.id == "ENSEMBL:ENSP0002"
    assert edges[0].end_node.id == "ENSEMBL:ENSP0003"
    assert edges[0].score == [300]


def test_string_ppi_adapter_honors_max_rows_on_kept_edges(tmp_path):
    data_path = tmp_path / "9606.protein.links.v12.0.txt"

    data_path.write_text(
        "\n".join(
            [
                "protein1 protein2 combined_score",
                "9606.ENSP0001 9606.ENSP0002 250",
                "9606.ENSP0002 9606.ENSP0002 900",
                "9606.ENSP0003 9606.ENSP0004 400",
                "9606.ENSP0004 9606.ENSP0005 500",
                "9606.ENSP0005 9606.ENSP0006 600",
            ]
        ),
        encoding="utf-8",
    )

    adapter = StringPPIAdapter(
        file_path=str(data_path),
        max_rows=2,
    )

    batches = list(adapter.get_all())
    edges = [edge for batch in batches for edge in batch]

    assert len(edges) == 2
    assert [(edge.start_node.id, edge.end_node.id, edge.score) for edge in edges] == [
        ("ENSEMBL:ENSP0003", "ENSEMBL:ENSP0004", [400]),
        ("ENSEMBL:ENSP0004", "ENSEMBL:ENSP0005", [500]),
    ]


def test_string_ppi_adapter_canonicalizes_pair_direction(tmp_path):
    data_path = tmp_path / "9606.protein.links.v12.0.txt"

    data_path.write_text(
        "\n".join(
            [
                "protein1 protein2 combined_score",
                "9606.ENSP9999 9606.ENSP0001 500",
            ]
        ),
        encoding="utf-8",
    )

    adapter = StringPPIAdapter(file_path=str(data_path))

    batches = list(adapter.get_all())
    edges = [edge for batch in batches for edge in batch]

    assert len(edges) == 1
    assert edges[0].start_node.id == "ENSEMBL:ENSP0001"
    assert edges[0].end_node.id == "ENSEMBL:ENSP9999"
    assert edges[0].score == [500]
