from typing import Generator, List, Union

from src.input_adapters.chebi.chebi_obo_adapter import ChebiFullOboAdapter
from src.models.chebi import ChemicalEntity
from src.models.metabolite_harmonization import (
    ChebiChemicalEntityMetaboliteIdentifierEdge,
    MetaboliteIdentifier,
)
from src.models.node import Node, Relationship


class ChebiMetaboliteIdentifierBridgeAdapter(ChebiFullOboAdapter):
    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        node_types, role_classes_by_id = self._collect_node_metadata()
        batch: List[ChebiChemicalEntityMetaboliteIdentifierEdge] = []
        term_count = 0

        for term_data in self._iter_term_blocks():
            term_count += 1
            if self._should_skip_term(term_data):
                if self.max_terms is not None and term_count >= self.max_terms:
                    break
                continue

            source_id = self._required_first(term_data, "id")
            if source_id in role_classes_by_id or node_types.get(source_id) is not ChemicalEntity:
                if self.max_terms is not None and term_count >= self.max_terms:
                    break
                continue

            batch.append(
                ChebiChemicalEntityMetaboliteIdentifierEdge(
                    start_node=ChemicalEntity(id=source_id),
                    end_node=MetaboliteIdentifier(id=source_id),
                    source_label=self._first(term_data.get("name")),
                )
            )
            if len(batch) >= self.batch_size:
                yield batch
                batch = []

            if self.max_terms is not None and term_count >= self.max_terms:
                break

        if batch:
            yield batch
