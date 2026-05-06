from src.input_adapters.mgi.hmd_human_phenotype import HMDHumanPhenotypeAdapter


def _write_hmd_file(path, rows):
    with open(path, "w", encoding="utf-8", newline="") as handle:
        for row in rows:
            handle.write("\t".join(row) + "\n")


def test_hmd_adapter_emits_mouse_phenotype_nodes_and_gene_edges(tmp_path):
    hmd_path = tmp_path / "HMD_HumanPhenotype.rpt"
    _write_hmd_file(hmd_path, [
        ["A1CF", "29974", "A1cf", "MGI:1917115", "MP:0005376, MP:0005386", ""],
        ["A4GALT", "53947", "A4galt", "MGI:3512453", "MP:0010768", ""],
    ])

    adapter = HMDHumanPhenotypeAdapter(file_path=str(hmd_path))

    entries = [entry for batch in adapter.get_all() for entry in batch]
    phenotype_nodes = [entry for entry in entries if entry.__class__.__name__ == "MousePhenotype"]
    phenotype_edges = [entry for entry in entries if entry.__class__.__name__ == "GeneMousePhenotypeEdge"]

    assert {node.id for node in phenotype_nodes} == {"MP:0005376", "MP:0005386", "MP:0010768"}
    assert all(node.name is None for node in phenotype_nodes)

    assert len(phenotype_edges) == 3
    assert phenotype_edges[0].start_node.id == "NCBIGene:29974"
    assert phenotype_edges[0].start_node.symbol == "A1CF"
    assert phenotype_edges[0].details[0].source == "MGI"
    assert phenotype_edges[0].details[0].source_id == "MGI:1917115"


def test_hmd_adapter_falls_back_to_symbol_when_geneid_missing(tmp_path):
    hmd_path = tmp_path / "HMD_HumanPhenotype.rpt"
    _write_hmd_file(hmd_path, [
        ["A1CF", "", "A1cf", "MGI:1917115", "MP:0005376", ""],
    ])

    adapter = HMDHumanPhenotypeAdapter(file_path=str(hmd_path))
    entries = [entry for batch in adapter.get_all() for entry in batch]
    phenotype_edge = next(entry for entry in entries if entry.__class__.__name__ == "GeneMousePhenotypeEdge")

    assert phenotype_edge.start_node.id == "Symbol:A1CF"


def test_hmd_adapter_skips_rows_without_gene_or_phenotype(tmp_path):
    hmd_path = tmp_path / "HMD_HumanPhenotype.rpt"
    _write_hmd_file(hmd_path, [
        ["A1CF", "", "A1cf", "MGI:1917115", "", ""],
        ["", "", "A1cf", "MGI:1917115", "MP:0005376", ""],
    ])

    adapter = HMDHumanPhenotypeAdapter(file_path=str(hmd_path))
    entries = [entry for batch in adapter.get_all() for entry in batch]

    assert entries == []
