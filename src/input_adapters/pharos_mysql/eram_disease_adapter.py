from collections import OrderedDict
from typing import Generator, List, Union

from src.constants import Prefix
from src.input_adapters.pharos_mysql.base import Pharos319Adapter
from src.models.disease import Disease, DiseaseAssociationDetail, ProteinDiseaseEdge
from src.models.node import EquivalentId, Node, Relationship
from src.models.protein import Protein
from src.shared.sqlalchemy_tables.pharos_tables_old import Disease as mysql_Disease, Protein as mysql_Protein


class ERAMDiseaseAdapter(Pharos319Adapter):
    batch_size = 10_000

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        diseases_by_id: OrderedDict[str, Disease] = OrderedDict()
        edge_by_key: OrderedDict[tuple[str, str], ProteinDiseaseEdge] = OrderedDict()

        rows = (
            self.get_session().query(
                mysql_Disease.did,
                mysql_Disease.ncats_name,
                mysql_Disease.source,
                mysql_Protein.uniprot,
            )
            .join(mysql_Protein, mysql_Protein.id == mysql_Disease.protein_id)
            .filter(mysql_Disease.dtype == "eRAM")
            .order_by(mysql_Disease.id)
        )

        for did, ncats_name, legacy_sources, uniprot in rows:
            disease_id = (did or "").strip()
            disease_name = (ncats_name or "").strip()
            uniprot_id = (uniprot or "").strip()
            if not disease_id or not disease_name or not uniprot_id:
                continue

            diseases_by_id.setdefault(
                disease_id,
                Disease(
                    id=disease_id,
                    name=disease_name,
                ),
            )

            protein_id = EquivalentId(id=uniprot_id, type=Prefix.UniProtKB).id_str()
            edge_key = (protein_id, disease_id)
            source_terms = self._split_legacy_sources(legacy_sources)

            if edge_key not in edge_by_key:
                edge_by_key[edge_key] = ProteinDiseaseEdge(
                    start_node=Protein(id=protein_id),
                    end_node=diseases_by_id[disease_id],
                    details=[
                        DiseaseAssociationDetail(
                            source="eRAM",
                            source_id=disease_id,
                            original_sources=source_terms,
                        )
                    ],
                )
            else:
                detail = edge_by_key[edge_key].details[0]
                detail.original_sources = self._merge_unique(detail.original_sources, source_terms)

        yield list(diseases_by_id.values())

        edge_values = list(edge_by_key.values())
        for i in range(0, len(edge_values), self.batch_size):
            yield edge_values[i:i + self.batch_size]

    @staticmethod
    def _split_legacy_sources(raw_value: str | None) -> List[str]:
        if not raw_value:
            return []
        return [
            token.strip()
            for token in raw_value.split("|")
            if token and token.strip()
        ]

    @staticmethod
    def _merge_unique(existing_values: List[str], new_values: List[str]) -> List[str]:
        merged = list(existing_values or [])
        seen = set(merged)
        for value in new_values:
            if value in seen:
                continue
            seen.add(value)
            merged.append(value)
        return merged
