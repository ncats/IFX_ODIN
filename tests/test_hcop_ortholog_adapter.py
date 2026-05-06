import gzip

from src.input_adapters.hcop.orthologs import HCOPOrthologAdapter


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


def test_hcop_adapter_emits_mouse_ortholog_nodes_and_edges(tmp_path):
    hcop_path = tmp_path / "hcop.tsv.gz"
    _write_hcop_file(hcop_path, [
        ["10090", "1017", "-", "-", "-", "CDK2", "-", "-", "12566", "ENSMUSG00000025351", "MGI:104772", "cyclin dependent kinase 2", "Cdk2", "-", "-", "OMA, Ensembl"],
        ["10090", "1017", "-", "-", "-", "CDK2", "-", "-", "12566", "ENSMUSG00000025351", "MGI:104772", "cyclin dependent kinase 2", "Cdk2", "-", "-", "OMA, Ensembl"],
        ["10116", "1017", "-", "-", "-", "CDK2", "-", "-", "362817", "ENSRNOG00000000001", "RGD:1", "rat kinase", "Cdk2", "-", "-", "OMA"],
        ["10090", "1499", "-", "-", "-", "CTNNB1", "-", "-", "12345", "ENSMUSG00000000001", "MGI:12345", "-", "-", "-", "-", "OMA"],
    ])

    adapter = HCOPOrthologAdapter(
        file_path=str(hcop_path),
        accepted_species=["10090"],
        drop_blank_ortholog_identity=True,
    )

    batches = list(adapter.get_all())
    entries = [entry for batch in batches for entry in batch]

    ortholog_nodes = [entry for entry in entries if entry.__class__.__name__ == "OrthologGene"]
    ortholog_edges = [entry for entry in entries if entry.__class__.__name__ == "GeneOrthologGeneEdge"]

    assert len(ortholog_nodes) == 1
    assert ortholog_nodes[0].id == "MGI:104772"
    assert ortholog_nodes[0].entrez_gene_id == "12566"
    assert ortholog_nodes[0].ensembl_gene_id == "ENSMUSG00000025351"

    assert len(ortholog_edges) == 1
    assert ortholog_edges[0].start_node.id == "NCBIGene:1017"
    assert ortholog_edges[0].end_node.id == "MGI:104772"
    assert ortholog_edges[0].support_sources == ["Ensembl", "OMA"]
    assert ortholog_edges[0].source_db_ids == ["MGI:104772"]
    assert ortholog_edges[0].ortholog_symbols == ["Cdk2"]
    assert ortholog_edges[0].ortholog_names == ["cyclin dependent kinase 2"]


def test_hcop_adapter_falls_back_to_non_mgi_ortholog_ids(tmp_path):
    hcop_path = tmp_path / "hcop.tsv.gz"
    _write_hcop_file(hcop_path, [
        ["10090", "-", "ENSG00000141510", "-", "-", "-", "-", "-", "22059", "ENSMUSG00000059552", "-", "tumor protein p53", "Trp53", "-", "-", "EggNOG"],
    ])

    adapter = HCOPOrthologAdapter(
        file_path=str(hcop_path),
        accepted_species=["10090"],
        drop_blank_ortholog_identity=True,
    )

    entries = [entry for batch in adapter.get_all() for entry in batch]
    ortholog_node = next(entry for entry in entries if entry.__class__.__name__ == "OrthologGene")
    ortholog_edge = next(entry for entry in entries if entry.__class__.__name__ == "GeneOrthologGeneEdge")

    assert ortholog_node.id == "NCBIGene:22059"
    assert ortholog_edge.start_node.id == "ENSEMBL:ENSG00000141510"


def test_hcop_adapter_prefers_ncbi_gene_over_cgnc_for_chicken(tmp_path):
    hcop_path = tmp_path / "hcop.tsv.gz"
    _write_hcop_file(hcop_path, [
        ["9031", "1813", "-", "-", "-", "DRD2", "-", "-", "396257", "ENSGALG00000001099", "CGNC:17400", "dopamine receptor D2", "DRD2", "-", "-", "Ensembl, NCBI"],
    ])

    adapter = HCOPOrthologAdapter(
        file_path=str(hcop_path),
        accepted_species=["9031"],
        drop_blank_ortholog_identity=True,
    )

    entries = [entry for batch in adapter.get_all() for entry in batch]
    ortholog_node = next(entry for entry in entries if entry.__class__.__name__ == "OrthologGene")
    ortholog_edge = next(entry for entry in entries if entry.__class__.__name__ == "GeneOrthologGeneEdge")

    assert ortholog_node.id == "NCBIGene:396257"
    assert ortholog_node.source_primary_id == "NCBIGene:396257"
    assert ortholog_node.source_db_id == "CGNC:17400"
    assert ortholog_node.ensembl_gene_id == "ENSGALG00000001099"
    assert ortholog_edge.end_node.id == "NCBIGene:396257"
