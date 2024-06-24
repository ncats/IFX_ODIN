from typing import List, Dict
import requests
from src.interfaces.id_normalizer import IdNormalizer, IdNormalizerResult, NormalizationMatch
from src.models.node import Node


class TranslatorNodeNormalizer(IdNormalizer):
    base_url = "https://nodenormalization-sri.renci.org/1.4"
    name = f"Translator Node Normalizer: {base_url}"
    conflate_genes_and_proteins: bool = False
    use_equivalent_ids = False

    def node_norm_url(self):
        return f"{self.base_url}/get_normalized_nodes"

    def normalize_internal(self, input_nodes: List[Node]) -> Dict[str, IdNormalizerResult]:

        input_nodes = list(set(input_nodes))
        post_body = {
            "curies": input_nodes,
            "conflate": self.conflate_genes_and_proteins,
            "description": False,
            "drug_chemical_conflate": False
        }

        response = requests.post(self.node_norm_url(), json=post_body)

        if response.status_code != 200:
            print(f"Failed to normalize IDs: {response.status_code} {response.text}")
            return {}

        response_data = response.json()

        result_list = {}
        for input_id, results in response_data.items():
            res_obj = IdNormalizerResult()
            if results is not None:
                res_obj.best_matches = [NormalizationMatch(input=input_id, match=results['id']['identifier'])]
                res_obj.other_matches = [
                    NormalizationMatch(input = input_id, match=equiv_id['identifier']) for equiv_id in results['equivalent_identifiers']
                ]
            result_list[input_id] = res_obj
        return result_list
