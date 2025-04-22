from typing import List, Generator

from src.constants import DataSourceName
from src.input_adapters.neo4j_adapter import Neo4jAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.ligand import ProteinLigandRelationship, Ligand
from src.models.protein import Protein


class SetLigandActivityFlagAdapter(InputAdapter, Neo4jAdapter):

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.PostProcessing

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo()

    def get_all(self) -> Generator[List[ProteinLigandRelationship], None, None]:
        passing_activities = self.runQuery(passing_activities_query)

        yield [ProteinLigandRelationship(
            start_node=Protein(id=pro_id),
            end_node=Ligand(id=lig_id),
            meets_idg_cutoff=True
        ) for pro_id, lig_id in passing_activities]


passing_activities_query = """MATCH (n:`biolink:Protein`)-[r:`biolink:interacts_with`]->(l:`biolink:ChemicalEntity`)
    WHERE
    (n.idg_family IN ['GPCR', 'Nuclear Receptor'] AND ANY(value IN r.act_value WHERE value >= 7))
 OR (n.idg_family = 'Kinase' AND ANY(value IN r.act_value WHERE value >= 7.52288))
 OR (n.idg_family = 'Ion Channel' AND ANY(value IN r.act_value WHERE value >= 5))
 OR ((n.idg_family is null OR NOT n.idg_family IN ['Ion Channel', 'Kinase', 'GPCR', 'Nuclear Receptor']) AND ANY(value IN r.act_value WHERE value >= 6))
    RETURN distinct n.id, l.id"""
