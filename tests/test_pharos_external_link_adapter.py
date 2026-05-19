import csv

from src.input_adapters.pharos_linkouts.linkouts import PharosExternalLinkAdapter
from src.models.external_link import ExternalLinkProvider, ProteinExternalLinkEdge


def _write_tsv(path, fieldnames, rows):
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(rows)


def _write_csv(path, fieldnames, rows):
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _flatten(adapter):
    return [entry for batch in adapter.get_all() for entry in batch]


def test_pharos_external_link_adapter_emits_providers_and_static_edges(tmp_path):
    protein_path = tmp_path / "proteins.tsv"
    _write_tsv(
        protein_path,
        ["ncats_protein_id", "uniprot_id", "consolidated_symbol", "is_canonical"],
        [
            {
                "ncats_protein_id": "IFXProtein:1",
                "uniprot_id": "P12345",
                "consolidated_symbol": "DRD2",
                "is_canonical": "True",
            },
            {
                "ncats_protein_id": "IFXProtein:2",
                "uniprot_id": "Q99999",
                "consolidated_symbol": "A|B",
                "is_canonical": "True",
            },
            {
                "ncats_protein_id": "IFXProtein:3",
                "uniprot_id": "Q11111",
                "consolidated_symbol": "SKIP",
                "is_canonical": "False",
            },
        ],
    )

    adapter = PharosExternalLinkAdapter(protein_file_path=str(protein_path))
    entries = _flatten(adapter)

    providers = [entry for entry in entries if isinstance(entry, ExternalLinkProvider)]
    edges = [entry for entry in entries if isinstance(entry, ProteinExternalLinkEdge)]

    assert {provider.source for provider in providers} == {
        "PubChem",
        "ARCHS4",
        "GlyGen",
        "Dark Kinome",
        "RESOLUTE",
        "TIGA",
        "LinkedOmicsKB",
    }
    assert {provider.name for provider in providers} >= {"PubChem", "Dark Kinase Knowledgebase"}
    assert [(edge.source, edge.start_node.id, edge.source_id) for edge in edges] == [
        ("PubChem", "IFXProtein:1", "P12345"),
        ("ARCHS4", "IFXProtein:1", "DRD2"),
        ("PubChem", "IFXProtein:2", "Q99999"),
    ]


def test_pharos_external_link_adapter_preserves_source_ids_for_file_lists(tmp_path):
    protein_path = tmp_path / "proteins.tsv"
    glygen_path = tmp_path / "glygen.csv"
    dark_path = tmp_path / "dark.tsv"
    resolute_path = tmp_path / "resolute.tsv"
    linkedomics_path = tmp_path / "linkedomics.tsv"
    tiga_path = tmp_path / "tiga.tsv"

    _write_tsv(protein_path, ["ncats_protein_id", "uniprot_id", "consolidated_symbol", "is_canonical"], [])
    _write_csv(glygen_path, ["uniprot_canonical_ac"], [{"uniprot_canonical_ac": "P31749-1"}])
    _write_tsv(dark_path, ["symbol", "url"], [{"symbol": "BRSK1", "url": "https://darkkinome.org/kinase/BRSK1"}])
    _write_tsv(
        resolute_path,
        ["symbol", "nextprot_ids", "ensembl_protein_ids", "url"],
        [{"symbol": "SLC10A1", "url": "https://re-solute.eu/knowledgebase/gene/SLC10A1"}],
    )
    _write_tsv(linkedomics_path, ["symbol", "url"], [{"symbol": "A1BG", "url": "https://kb.linkedomics.org/gene/A1BG"}])
    _write_tsv(tiga_path, ["ensemblId"], [{"ensemblId": "ENSG00000149295"}, {"ensemblId": "ENSG00000149295"}])

    adapter = PharosExternalLinkAdapter(
        protein_file_path=str(protein_path),
        glygen_file_path=str(glygen_path),
        dark_kinome_file_path=str(dark_path),
        resolute_file_path=str(resolute_path),
        linkedomics_file_path=str(linkedomics_path),
        tiga_stats_file_path=str(tiga_path),
    )
    edges = [entry for entry in _flatten(adapter) if isinstance(entry, ProteinExternalLinkEdge)]

    assert [(edge.source, edge.start_node.id, edge.source_id, edge.source_id_type) for edge in edges] == [
        ("GlyGen", "UniProtKB:P31749", "P31749-1", "uniprot_isoform"),
        ("Dark Kinome", "Symbol:BRSK1", "BRSK1", "symbol"),
        ("RESOLUTE", "Symbol:SLC10A1", "SLC10A1", "symbol"),
        ("TIGA", "ENSEMBL:ENSG00000149295", "ENSG00000149295", "ensembl_gene"),
        ("LinkedOmicsKB", "Symbol:A1BG", "A1BG", "symbol"),
    ]
    assert all(len(edge.details) == 1 for edge in edges)
