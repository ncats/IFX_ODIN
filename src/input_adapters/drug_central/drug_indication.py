from collections import OrderedDict
import hashlib
from typing import Generator, List, Union

from sqlalchemy import text

from src.constants import DataSourceName, Prefix
from src.input_adapters.drug_central.drug_node import DrugCentralAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.disease import Disease, DiseaseAssociationDetail, ProteinDiseaseEdge
from src.models.node import EquivalentId, Node, Relationship
from src.models.protein import Protein


class DrugCentralIndicationAdapter(InputAdapter, DrugCentralAdapter):
    batch_size = 10000

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.DrugCentral

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        diseases_by_id: OrderedDict[str, Disease] = OrderedDict()
        edge_by_key: OrderedDict[tuple[str, str], ProteinDiseaseEdge] = OrderedDict()
        seen_detail_keys: set[tuple[str, str, str, str | None, str | None]] = set()

        with self.get_session() as session:
            rows = session.execute(text("""
                select distinct
                    o.struct_id,
                    s.name as drug_name,
                    o.concept_name,
                    o.umls_cui,
                    o.snomed_conceptid,
                    o.doid,
                    a.accession
                from omop_relationship_doid_view o
                join structures s
                    on s.id = o.struct_id
                join act_table_full a
                    on a.struct_id = o.struct_id
                where o.relationship_name = 'indication'
                  and a.organism = 'Homo sapiens'
                  and a.accession is not null
                  and trim(a.accession) <> ''
                order by o.struct_id, o.concept_name, o.umls_cui, a.accession
            """)).mappings()

            for row in rows:
                disease_name = (row["concept_name"] or "").strip()
                if not disease_name:
                    continue
                umls_cui = (row["umls_cui"] or "").strip() or None
                disease_id = self._disease_id(disease_name, umls_cui)

                diseases_by_id.setdefault(
                    disease_id,
                    Disease(
                        id=disease_id,
                        name=disease_name,
                    ),
                )

                snomed_id = (
                    EquivalentId(id=str(row["snomed_conceptid"]).strip(), type=Prefix.SNOMEDCT).id_str()
                    if row["snomed_conceptid"] is not None and str(row["snomed_conceptid"]).strip()
                    else None
                )
                doid = (row["doid"] or "").strip() or None
                drug_name = (row["drug_name"] or "").strip() or None

                for accession in self._split_accessions(row["accession"]):
                    protein_id = EquivalentId(id=accession, type=Prefix.UniProtKB).id_str()
                    edge_key = (protein_id, disease_id)
                    detail_key = (protein_id, disease_id, drug_name or "", snomed_id, doid)
                    if detail_key in seen_detail_keys:
                        continue
                    seen_detail_keys.add(detail_key)

                    detail = DiseaseAssociationDetail(
                        source="DrugCentral Indication",
                        source_id=EquivalentId(id=umls_cui, type=Prefix.UMLS).id_str() if umls_cui else None,
                        drug_name=drug_name,
                        snomed_id=snomed_id,
                        doid=doid,
                    )

                    if edge_key not in edge_by_key:
                        edge_by_key[edge_key] = ProteinDiseaseEdge(
                            start_node=Protein(id=protein_id),
                            end_node=diseases_by_id[disease_id],
                            details=[detail],
                        )
                    else:
                        edge_by_key[edge_key].details.append(detail)

        yield list(diseases_by_id.values())
        edge_values = list(edge_by_key.values())
        for i in range(0, len(edge_values), self.batch_size):
            yield edge_values[i:i + self.batch_size]

    @staticmethod
    def _split_accessions(raw_accessions: str) -> List[str]:
        return [
            token.strip()
            for token in raw_accessions.split("|")
            if token and token.strip()
        ]

    @staticmethod
    def _disease_id(disease_name: str, umls_cui: str | None) -> str:
        if umls_cui:
            return EquivalentId(id=umls_cui, type=Prefix.UMLS).id_str()
        digest = hashlib.sha1(disease_name.strip().lower().encode("utf-8")).hexdigest()[:16]
        return f"DrugCentral:INDICATION:{digest}"
