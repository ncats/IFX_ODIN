from datetime import date

from src.input_adapters.bioplex.bioplex_ppi import BioPlexPPIAdapter
from tests.registry_fakes import registry_dataset


def test_bioplex_adapter_preserves_probability_fields_and_canonicalizes_direction(tmp_path):
    data_path = tmp_path / "BioPlex_293T_Network_10K_Dec_2019.tsv"
    data_path.write_text(
        "\n".join(
            [
                '"GeneA"\t"GeneB"\t"UniprotA"\t"UniprotB"\t"SymbolA"\t"SymbolB"\t"pW"\t"pNI"\t"pInt"',
                '"2"\t"1"\t"Q8N7W2-2"\t"P00813"\t"BEND7"\t"ADA"\t"0.01"\t"0.02"\t"0.97"',
            ]
        ),
        encoding="utf-8",
    )
    adapter = BioPlexPPIAdapter(
        data_source=registry_dataset(
            tmp_path,
            data_path.name,
            source="bioplex",
            dataset="ppi",
            version="3.0",
            version_date=date(2024, 1, 19),
        ),
    )

    batches = list(adapter.get_all())
    edges = [edge for batch in batches for edge in batch]

    assert len(edges) == 1
    assert edges[0].start_node.id == "UniProtKB:P00813"
    assert edges[0].end_node.id == "UniProtKB:Q8N7W2-2"
    assert edges[0].p_wrong == [0.01]
    assert edges[0].p_ni == [0.02]
    assert edges[0].p_int == [0.97]
    assert edges[0].sources == []

    version = adapter.get_version()
    assert version.version == "3.0"
    assert version.version_date.isoformat() == "2024-01-19"


def test_bioplex_adapter_falls_back_to_ncbi_gene_for_unknown_uniprot(tmp_path):
    data_path = tmp_path / "BioPlex_HCT116_Network_5.5K_Dec_2019.tsv"
    data_path.write_text(
        "\n".join(
            [
                '"GeneA"\t"GeneB"\t"UniprotA"\t"UniprotB"\t"SymbolA"\t"SymbolB"\t"pW"\t"pNI"\t"pInt"',
                '"3012"\t"4673"\t"UNKNOWN"\t"P55209"\t"HIST1H2AE"\t"NAP1L1"\t"0.001"\t"0.002"\t"0.997"',
            ]
        ),
        encoding="utf-8",
    )
    adapter = BioPlexPPIAdapter(
        data_source=registry_dataset(tmp_path, data_path.name, version="3.0"),
    )

    batches = list(adapter.get_all())
    edges = [edge for batch in batches for edge in batch]

    assert len(edges) == 1
    assert {edges[0].start_node.id, edges[0].end_node.id} == {"NCBIGene:3012", "UniProtKB:P55209"}


def test_bioplex_adapter_honors_max_rows_on_kept_edges(tmp_path):
    data_path = tmp_path / "BioPlex_293T_Network_10K_Dec_2019.tsv"

    data_path.write_text(
        "\n".join(
            [
                '"GeneA"\t"GeneB"\t"UniprotA"\t"UniprotB"\t"SymbolA"\t"SymbolB"\t"pW"\t"pNI"\t"pInt"',
                '"1"\t"1"\t"P11142"\t"P11142"\t"HSPA8"\t"HSPA8"\t"0.1"\t"0.2"\t"0.7"',
                '"1"\t"2"\t"P11142"\t"P00813"\t"HSPA8"\t"ADA"\t"0.01"\t"0.02"\t"0.97"',
                '"3"\t"4"\t"Q9Y3U8"\t"P36578"\t"RPL36"\t"RPL4"\t"0.03"\t"0.04"\t"0.93"',
                '"5"\t"6"\t"P26373"\t"Q09028-3"\t"RPL13"\t"RBBP4"\t"0.05"\t"0.06"\t"0.89"',
            ]
        ),
        encoding="utf-8",
    )

    adapter = BioPlexPPIAdapter(
        data_source=registry_dataset(tmp_path, data_path.name, version="3.0"),
        max_rows=2,
    )

    batches = list(adapter.get_all())
    edges = [edge for batch in batches for edge in batch]

    assert len(edges) == 2
    assert [(edge.start_node.id, edge.end_node.id) for edge in edges] == [
        ("UniProtKB:P00813", "UniProtKB:P11142"),
        ("UniProtKB:P36578", "UniProtKB:Q9Y3U8"),
    ]
