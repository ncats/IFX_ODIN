import yaml

from src.core.config import ETL_Config


def test_working_pharos_compare_yaml_uses_tcrd_target_resolver():
    config = ETL_Config("./src/use_cases/working_pharos_compare.yaml")

    assert "tcrd_targets" in config.resolvers
    resolver = config.resolvers["tcrd_targets"]
    assert resolver.__class__.__name__ == "TCRDTargetResolver"
    assert "Gene" in resolver.types
    assert resolver.canonical_class.__name__ == "Protein"
    assert "tissue_ids" in config.resolvers
    assert config.resolvers["tissue_ids"].__class__.__name__ == "TissueResolver"


def test_working_target_graph_compare_yaml_uses_gene_and_protein_resolvers():
    with open("./src/use_cases/working_target_graph_compare.yaml", "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    resolvers = {resolver["label"]: resolver for resolver in config["resolvers"]}

    assert resolvers["tg_genes"]["class"] == "TargetGraphGeneResolver"
    assert resolvers["tg_genes"]["kwargs"]["types"] == ["Gene"]

    assert resolvers["tg_proteins"]["class"] == "TargetGraphProteinResolver"
    assert resolvers["tg_proteins"]["kwargs"]["types"] == ["Protein"]

    assert resolvers["tissue_ids"]["class"] == "TissueResolver"
