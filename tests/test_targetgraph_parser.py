from src.input_adapters.target_graph.protein_nodes_and_edges import ProteinNodeAdapter



file_path = "/Users/kelleherkj/IdeaProjects/NCATS_ODIN/input_files/target_graph/protein_ids.csv"
additional_id_file_path = "/Users/kelleherkj/IdeaProjects/NCATS_ODIN/input_files/target_graph/uniprotkb_mapping.csv"

tgpp = ProteinNodeAdapter(file_path=file_path, additional_id_file_path=additional_id_file_path)

tgpp.get_all_combined()

# tests/test_targetgraph_parser.py
def test_example():
    assert 1 == 2