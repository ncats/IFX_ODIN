from typing import List, Generator

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.ligand import ProteinLigandRelationship, Ligand
from src.models.protein import Protein
from src.shared.arango_adapter import ArangoAdapter


class SetLigandActivityFlagAdapter(InputAdapter, ArangoAdapter):

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.PostProcessing

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo()

    def get_all(self) -> Generator[List[ProteinLigandRelationship], None, None]:
        passing_activities = self.runQuery(passing_activities_query)

        yield [ProteinLigandRelationship(
            start_node=Protein(id=row['protein_id']),
            end_node=Ligand(id=row['chemical_entity_id']),
            meets_idg_cutoff=True
        ) for row in passing_activities]


passing_activities_query = """FOR pro IN `biolink:Protein`
  FOR chem, rel IN OUTBOUND pro `biolink:interacts_with`
    LET act_values = rel.details[* FILTER CURRENT.act_value >= (
      pro.idg_family == "Kinase" ? 7.52288 :
      pro.idg_family == "Ion Channel" ? 5 :
      pro.idg_family IN ["GPCR", "Nuclear Receptor"] ? 7 : 6
    )]
    FILTER LENGTH(act_values) > 0
    RETURN DISTINCT {
      protein_id: pro.id,
      chemical_entity_id: chem.id
    }
"""
