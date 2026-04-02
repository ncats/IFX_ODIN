import yaml

from src.core.config import ETL_Config


def test_working_pharos_compare_yaml_uses_tcrd_target_resolver():
    with open("./src/use_cases/working_pharos_compare.yaml", "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    resolvers = {resolver["label"]: resolver for resolver in config["resolvers"]}
    input_adapters = config["input_adapters"]

    assert resolvers["tcrd_targets"]["class"] == "TCRDTargetResolver"
    assert resolvers["tcrd_targets"]["kwargs"]["types"] == ["Protein", "Gene", "Transcript"]
    assert resolvers["tcrd_targets"]["kwargs"]["canonical_type"] == "Protein"
    assert resolvers["tcrd_targets"]["kwargs"]["collapse_reviewed_targets"] is True
    assert resolvers["tcrd_targets"]["kwargs"]["gene_file_path"] == "/Users/kelleherkj/Downloads/260401Targets/gene_ids.tsv"
    assert resolvers["tcrd_targets"]["kwargs"]["transcript_file_path"] == "/Users/kelleherkj/Downloads/260401Targets/transcript_ids.tsv"
    assert resolvers["tcrd_targets"]["kwargs"]["protein_file_paths"] == ["/Users/kelleherkj/Downloads/260401Targets/protein_ids.tsv"]
    assert resolvers["tissue_ids"]["class"] == "TissueResolver"

    assert input_adapters[0]["class"] == "ProteinNodeAdapter"
    assert input_adapters[0]["kwargs"]["file_path"] == "/Users/kelleherkj/Downloads/260401Targets/protein_ids.tsv"
    assert input_adapters[0]["kwargs"]["collapse_reviewed_targets"] is True


def test_working_target_graph_compare_yaml_uses_gene_and_protein_resolvers():
    with open("./src/use_cases/working_target_graph_compare.yaml", "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    resolvers = {resolver["label"]: resolver for resolver in config["resolvers"]}
    input_adapters = config["input_adapters"]

    assert resolvers["tg_genes"]["class"] == "TargetGraphGeneResolver"
    assert resolvers["tg_genes"]["kwargs"]["types"] == ["Gene"]
    assert resolvers["tg_genes"]["kwargs"]["file_path"] == "/Users/kelleherkj/Downloads/260401Targets/gene_ids.tsv"

    assert resolvers["tg_proteins"]["class"] == "TargetGraphProteinResolver"
    assert resolvers["tg_proteins"]["kwargs"]["types"] == ["Protein"]
    assert resolvers["tg_proteins"]["kwargs"]["file_paths"] == ["/Users/kelleherkj/Downloads/260401Targets/protein_ids.tsv"]

    assert resolvers["tg_transcripts"]["class"] == "TargetGraphTranscriptResolver"
    assert resolvers["tg_transcripts"]["kwargs"]["types"] == ["Transcript"]
    assert resolvers["tg_transcripts"]["kwargs"]["file_path"] == "/Users/kelleherkj/Downloads/260401Targets/transcript_ids.tsv"

    assert resolvers["tissue_ids"]["class"] == "TissueResolver"

    adapter_classes = [adapter["class"] for adapter in input_adapters[:7]]
    assert adapter_classes == [
        "ProteinNodeAdapter",
        "IsoformProteinEdgeAdapter",
        "GeneNodeAdapter",
        "TranscriptNodeAdapter",
        "GeneTranscriptEdgeAdapter",
        "TranscriptProteinEdgeAdapter",
        "GeneProteinEdgeAdapter",
    ]
