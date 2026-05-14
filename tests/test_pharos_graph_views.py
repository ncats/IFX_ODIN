import yaml


def test_target_graph_current_tdls_exports_read_nested_tdl_metadata():
    config_paths = [
        "./src/use_cases/pharos/target_graph_aql_post.yaml",
        "./src/use_cases/pharos/impatient_target_graph_aql_post.yaml",
    ]

    for config_path in config_paths:
        with open(config_path, "r", encoding="utf-8") as handle:
            config = yaml.safe_load(handle)

        graph_views = {view["id"]: view for view in config["graph_views"]}
        query = graph_views["current_tdls"]["query"]

        assert "pro.tdl_meta.tdl_ligand_count" in query
        assert "pro.tdl_meta.tdl_drug_count" in query
        assert "pro.tdl_meta.tdl_go_term_count" in query
        assert "pro.tdl_meta.tdl_generif_count" in query
        assert "pro.tdl_meta.tdl_pm_score" in query
        assert "pro.tdl_meta.tdl_antibody_count" in query
        assert "pro.tdl_ligand_count" not in query
