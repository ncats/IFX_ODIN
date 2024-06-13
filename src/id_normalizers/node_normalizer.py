from typing import Union, List, Dict
import requests
from src.interfaces.id_normalizer import IdNormalizer, IdNormalizerResult, NormalizationMatch


class TranslatorNodeNormalizer(IdNormalizer):
    base_url = "https://nodenormalization-sri.renci.org/1.4"
    conflate_genes_and_proteins: bool = False

    def node_norm_url(self):
        return f"{self.base_url}/get_normalized_nodes"

    def normalize(self, input_ids: Union[str, List[str]]) -> Dict[str, IdNormalizerResult]:

        if not isinstance(input_ids, list):
            input_ids = [input_ids]
        input_ids = list(set(input_ids))
        post_body = {
            "curies": input_ids,
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
                res_obj.best_matches = [NormalizationMatch(match=results['id']['identifier'])]
                res_obj.other_matches = [
                    NormalizationMatch(match=equiv_id['identifier']) for equiv_id in results['equivalent_identifiers']
                ]
            result_list[input_id] = res_obj
        return result_list
