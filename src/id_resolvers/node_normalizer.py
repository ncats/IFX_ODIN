import time
from typing import List, Dict, Iterable

import requests
from src.interfaces.id_resolver import IdResolver, IdMatch
from src.models.node import Node
from src.shared.util import yield_per


class TranslatorNodeNormResolver(IdResolver):
    base_url = "https://nodenormalization-sri.renci.org/1.5"
    name = f"Translator Node Normalizer: {base_url}"
    conflate_genes_and_proteins: bool = False
    retryable_status_codes = {408, 429, 500, 502, 503, 504}

    def __init__(self,
                 types: List[str],
                 batch_size: int = 50000,
                 request_timeout: int = 120,
                 max_retries: int = 10,
                 retry_backoff_seconds: int = 60,
                 **kwargs):
        super().__init__(types=types, **kwargs)
        self.batch_size = batch_size
        self.request_timeout = request_timeout
        self.max_retries = max(1, max_retries)
        self.retry_backoff_seconds = retry_backoff_seconds

    def node_norm_url(self):
        return f"{self.base_url}/get_normalized_nodes"

    def resolve_internal(self, input_nodes: List[Node]) -> Dict[str, List[IdMatch]]:

        input_ids = list(set([node.id for node in input_nodes]))

        result_list = {}

        for batch in yield_per(input_ids, self.batch_size):
            response_data = self._post_to_node_normalizer(batch)

            for input_id, results in response_data.items():
                res_obj: List[IdMatch] = []
                if results is not None:
                    res_obj = [
                        IdMatch(
                            input=input_id,
                            match=results['id']['identifier'],
                            equivalent_ids=[equiv_id['identifier'] for equiv_id in results['equivalent_identifiers']]
                        )]
                result_list[input_id] = res_obj
        return result_list

    def _post_to_node_normalizer(self, curies: Iterable[str]):
        batch = list(curies)
        post_body = {
            "curies": batch,
            "conflate": self.conflate_genes_and_proteins,
            "description": False,
            "drug_chemical_conflate": False
        }

        response = self._post_with_retries(batch, post_body)
        print(f"Resolved {len(batch)} from Translator Node Normalizer")
        return response.json()

    def _post_with_retries(self, batch: List[str], post_body: Dict):
        last_exception = None

        for attempt in range(1, self.max_retries + 1):
            try:
                response = requests.post(
                    self.node_norm_url(),
                    json=post_body,
                    timeout=self.request_timeout,
                )
            except requests.exceptions.RequestException as exc:
                last_exception = exc
                if attempt == self.max_retries:
                    break
                self._sleep_before_retry(attempt, batch, f"{type(exc).__name__}: {exc}")
                continue

            if response.status_code == 200:
                return response

            if response.status_code not in self.retryable_status_codes or attempt == self.max_retries:
                self._raise_node_normalizer_error(batch, response=response)

            self._sleep_before_retry(
                attempt,
                batch,
                f"{response.status_code} {response.text[:500]}",
            )

        self._raise_node_normalizer_error(batch, exception=last_exception)

    def _sleep_before_retry(self, attempt: int, batch: List[str], reason: str):
        sleep_seconds = self.retry_backoff_seconds * attempt
        print(
            f"Node Normalizer request failed for {len(batch)} IDs "
            f"(attempt {attempt}/{self.max_retries}): {reason}. "
            f"Retrying in {sleep_seconds} seconds."
        )
        time.sleep(sleep_seconds)

    @staticmethod
    def _sample_batch(batch: List[str]) -> str:
        return ", ".join(batch[:10])

    def _raise_node_normalizer_error(self, batch: List[str], response=None, exception: Exception = None):
        sample = self._sample_batch(batch)
        if response is not None:
            print(f"Failed to resolve IDs: {response.status_code} {response.text}")
            raise RuntimeError(
                f"Translator Node Normalizer failed with HTTP {response.status_code} "
                f"for {len(batch)} IDs after {self.max_retries} attempts. "
                f"Sample input IDs: {sample}"
            )
        raise RuntimeError(
            f"Translator Node Normalizer request failed for {len(batch)} IDs "
            f"after {self.max_retries} attempts. Sample input IDs: {sample}"
        ) from exception
