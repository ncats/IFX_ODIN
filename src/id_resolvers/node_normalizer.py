from typing import List, Dict
import requests
from src.interfaces.id_resolver import IdResolver, IdResolverResult, IdMatch
from src.models.node import Node


class TranslatorNodeNormResolver(IdResolver):
    base_url = "https://nodenormalization-sri.renci.org/1.4"
    name = f"Translator Node Normalizer: {base_url}"
    conflate_genes_and_proteins: bool = False
    use_equivalent_ids = False

    def node_norm_url(self):
        return f"{self.base_url}/get_normalized_nodes"

    def resolve_internal(self, input_nodes: List[Node]) -> Dict[str, IdResolverResult]:

        input_nodes = list(set(input_nodes))
        post_body = {
            "curies": input_nodes,
            "conflate": self.conflate_genes_and_proteins,
            "description": False,
            "drug_chemical_conflate": False
        }

        response = requests.post(self.node_norm_url(), json=post_body)

        if response.status_code != 200:
            print(f"Failed to resolve IDs: {response.status_code} {response.text}")
            return {}

        response_data = response.json()

        result_list = {}
        for input_id, results in response_data.items():
            res_obj = IdResolverResult()
            if results is not None:
                res_obj.best_matches = [IdMatch(input=input_id, match=results['id']['identifier'])]
                res_obj.other_matches = [
                    IdMatch(input = input_id, match=equiv_id['identifier']) for equiv_id in results['equivalent_identifiers']
                ]
            result_list[input_id] = res_obj
        return result_list
