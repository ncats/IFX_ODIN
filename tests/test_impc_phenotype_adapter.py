import csv
import gzip

from src.input_adapters.impc.phenotypes import IMPCPhenotypeAdapter


def _write_impc_file(path, rows):
    fieldnames = [
        "marker_accession_id",
        "marker_symbol",
        "phenotyping_center",
        "colony_id",
        "sex",
        "zygosity",
        "allele_accession_id",
        "allele_symbol",
        "allele_name",
        "strain_accession_id",
        "strain_name",
        "project_name",
        "pipeline_name",
        "pipeline_stable_id",
        "procedure_stable_id",
        "procedure_name",
        "parameter_stable_id",
        "parameter_name",
        "top_level_mp_term_id",
        "top_level_mp_term_name",
        "mp_term_id",
        "mp_term_name",
        "p_value",
        "percentage_change",
        "effect_size",
        "statistical_method",
        "resource_name",
    ]
    with gzip.open(path, "wt", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_impc_adapter_emits_mouse_phenotype_nodes_and_edges(tmp_path):
    impc_path = tmp_path / "impc.csv.gz"
    _write_impc_file(impc_path, [
        {
            "marker_accession_id": "MGI:94924",
            "marker_symbol": "Drd2",
            "phenotyping_center": "WTSI",
            "colony_id": "COLONY1",
            "sex": "male",
            "zygosity": "heterozygote",
            "allele_accession_id": "MGI:123456",
            "allele_symbol": "Drd2<tm1a>",
            "allele_name": "targeted mutation 1a",
            "strain_accession_id": "MGI:2159965",
            "strain_name": "C57BL/6N",
            "project_name": "IMPC",
            "pipeline_name": "Adult",
            "pipeline_stable_id": "PIPE1",
            "procedure_stable_id": "PROC1",
            "procedure_name": "Clinical Chemistry",
            "parameter_stable_id": "PARAM1",
            "parameter_name": "Total bilirubin",
            "top_level_mp_term_id": "MP:0005376",
            "top_level_mp_term_name": "homeostasis/metabolism phenotype",
            "mp_term_id": "MP:0005635",
            "mp_term_name": "decreased circulating bilirubin level",
            "p_value": "3.97e-09",
            "percentage_change": "-143.755506173609",
            "effect_size": "1.54010313470039",
            "statistical_method": "GLS",
            "resource_name": "IMPC",
        },
        {
            "marker_accession_id": "MGI:94924",
            "marker_symbol": "Drd2",
            "phenotyping_center": "WTSI",
            "colony_id": "COLONY2",
            "sex": "female",
            "zygosity": "homozygote",
            "allele_accession_id": "MGI:123457",
            "allele_symbol": "Drd2<tm1b>",
            "allele_name": "targeted mutation 1b",
            "strain_accession_id": "MGI:2159965",
            "strain_name": "C57BL/6N",
            "project_name": "IMPC",
            "pipeline_name": "Adult",
            "pipeline_stable_id": "PIPE1",
            "procedure_stable_id": "PROC1",
            "procedure_name": "Clinical Chemistry",
            "parameter_stable_id": "PARAM1",
            "parameter_name": "Total bilirubin",
            "top_level_mp_term_id": "MP:0005376",
            "top_level_mp_term_name": "homeostasis/metabolism phenotype",
            "mp_term_id": "MP:0005635",
            "mp_term_name": "decreased circulating bilirubin level",
            "p_value": "1.0e-06",
            "percentage_change": "-120",
            "effect_size": "1.2",
            "statistical_method": "GLS",
            "resource_name": "IMPC",
        },
    ])

    adapter = IMPCPhenotypeAdapter(file_path=str(impc_path))

    entries = [entry for batch in adapter.get_all() for entry in batch]
    phenotype_nodes = [entry for entry in entries if entry.__class__.__name__ == "MousePhenotype"]
    phenotype_edges = [entry for entry in entries if entry.__class__.__name__ == "OrthologGeneMousePhenotypeEdge"]

    assert len(phenotype_nodes) == 1
    assert phenotype_nodes[0].id == "MP:0005635"
    assert phenotype_nodes[0].name == "decreased circulating bilirubin level"

    assert len(phenotype_edges) == 2
    assert phenotype_edges[0].start_node.id == "MGI:94924"
    assert phenotype_edges[0].end_node.id == "MP:0005635"
    assert phenotype_edges[0].details[0].source == "IMPC"
    assert phenotype_edges[0].details[0].source_id == "IMPC"
    assert phenotype_edges[0].details[0].top_level_term_id == "MP:0005376"
    assert phenotype_edges[0].details[0].procedure_name == "Clinical Chemistry"
    assert phenotype_edges[0].details[0].parameter_name == "Total bilirubin"
    assert phenotype_edges[0].details[0].p_value == 3.97e-09


def test_impc_adapter_skips_rows_without_marker_or_mp_term_id(tmp_path):
    impc_path = tmp_path / "impc.csv.gz"
    _write_impc_file(impc_path, [
        {"marker_accession_id": "", "mp_term_id": "MP:1", "mp_term_name": "x"},
        {"marker_accession_id": "MGI:1", "mp_term_id": "", "mp_term_name": "x"},
    ])

    adapter = IMPCPhenotypeAdapter(file_path=str(impc_path))

    entries = [entry for batch in adapter.get_all() for entry in batch]
    assert entries == []
