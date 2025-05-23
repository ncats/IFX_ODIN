from typing import List, Union, Generator

from src.constants import DataSourceName
from src.input_adapters.neo4j_adapter import Neo4jAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import Node, Relationship
from src.models.protein import Protein


class ExpandIDGFamilies(InputAdapter, Neo4jAdapter):


    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.PostProcessing

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo()

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        unmatched_fams = self.runQuery(unmatched_fam_query)
        new_fam_map = {}

        for row in unmatched_fams:
            pro_id, pro_fam, canpro_id, canpro_fam = row
            if pro_fam is None:
                set_fam(pro_id, canpro_fam, new_fam_map)
            else:
                set_fam(canpro_id, pro_fam, new_fam_map)

        yield [Protein(id=id, idg_family=fam) for id, fam in new_fam_map.items()]


def set_fam(protein_id, fam_to_set, fam_map):
    if protein_id not in fam_map:
        fam_map[protein_id] = fam_to_set
        return
    existing_fam_to_set = fam_map[protein_id]
    if existing_fam_to_set != fam_to_set:
        raise Exception(f"ID {protein_id} has conflicting families {existing_fam_to_set} and {fam_to_set}")


unmatched_fam_query = """MATCH (pro:`biolink:Protein`)-[r:Has_Canonical_Isoform]->(canpro:`biolink:Protein`)
    WHERE (pro.idg_family is null AND canpro.idg_family is not null) 
    OR (canpro.idg_family is null AND pro.idg_family is not null)
    RETURN pro.id, pro.idg_family, canpro.id, canpro.idg_family"""