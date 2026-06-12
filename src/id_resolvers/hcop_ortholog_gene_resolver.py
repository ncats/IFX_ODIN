from typing import Dict, List, Set

from src.input_adapters.shared.hcop import HCOPRecordHelper
from src.id_resolvers.node_normalizer import TranslatorNodeNormResolver
from src.interfaces.id_resolver import IdMatch, NoMatchBehavior
from src.models.node import Node
from src.shared.util import yield_per


class HCOPOrthologGeneResolver(TranslatorNodeNormResolver):
    name = "HCOP OrthologGene Resolver"

    def __init__(self,
                 types: List[str],
                 file_path: str = None,
                 data_source=None,
                 accepted_species: List[str] = None,
                 drop_blank_ortholog_identity: bool = True,
                 batch_size: int = 50000,
                 **kwargs):
        if "no_match_behavior" in kwargs:
            raise ValueError("HCOPOrthologGeneResolver is skip-only and does not accept no_match_behavior")

        super().__init__(
            types=types,
            no_match_behavior=NoMatchBehavior.Skip,
            **kwargs,
        )

        self.batch_size = batch_size
        if data_source is not None:
            file_path = str(data_source.file("human_all_hcop_sixteen_column.txt.gz"))
        if file_path is None:
            raise ValueError("HCOPOrthologGeneResolver requires file_path or data_source")
        self.hcop_helper = HCOPRecordHelper(
            file_path=file_path,
            accepted_species=accepted_species,
            drop_blank_ortholog_identity=drop_blank_ortholog_identity,
        )
        self.allowed_canonical_ids = self._build_allowed_canonical_ids()

    def resolve_internal(self, input_nodes: List[Node]) -> Dict[str, List[IdMatch]]:
        nn_results = super().resolve_internal(input_nodes)
        filtered_results = {}
        for input_id, matches in nn_results.items():
            filtered_results[input_id] = [
                match for match in matches
                if match.match in self.allowed_canonical_ids
            ]
        return filtered_results

    def _build_allowed_canonical_ids(self) -> Set[str]:
        allowed_canonical_ids = set()
        input_ids = sorted(self._collect_allowed_hcop_ids())

        for batch in yield_per(input_ids, self.batch_size):
            response_data = self._post_to_node_normalizer(batch)
            for results in response_data.values():
                if results is not None:
                    allowed_canonical_ids.add(results["id"]["identifier"])

        print(
            f"Preloaded {len(allowed_canonical_ids)} allowed HCOP ortholog canonical IDs "
            f"from {self.hcop_helper.file_path}"
        )
        return allowed_canonical_ids

    def _collect_allowed_hcop_ids(self) -> Set[str]:
        allowed_ids = set()
        for row in self.hcop_helper.iter_accepted_rows():
            allowed_ids.update(self.hcop_helper.ortholog_curies_from_row(row))
        return allowed_ids
