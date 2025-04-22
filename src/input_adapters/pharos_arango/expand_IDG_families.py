from typing import List, Union, Generator

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import Node, Relationship
from src.models.protein import Protein
from src.shared.arango_adapter import ArangoAdapter


class ExpandIDGFamilies(InputAdapter, ArangoAdapter):


    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.PostProcessing

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo()

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        unmatched_fams = self.runQuery(unmatched_fam_query)
        new_fam_map = {}

        for row in unmatched_fams:
            pro_id = row['pro_id']
            pro_fam = row['pro_idg_family']
            canpro_id = row['canpro_id']
            canpro_fam = row['canpro_idg_family']
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


unmatched_fam_query = """FOR pro IN `biolink:Protein`
    FOR rel IN Has_Canonical_Isoform
        FILTER rel._from == pro._id
        FOR canpro IN `biolink:Protein`
            FILTER rel._to == canpro._id
            FILTER (IS_NULL(pro.idg_family) AND !IS_NULL(canpro.idg_family)) 
                OR (IS_NULL(canpro.idg_family) AND !IS_NULL(pro.idg_family))
            RETURN {
                pro_id: pro.id,
                pro_idg_family: pro.idg_family,
                canpro_id: canpro.id,
                canpro_idg_family: canpro.idg_family
            }
"""