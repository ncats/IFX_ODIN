from typing import List, Union

from src.constants import Prefix
from src.input_adapters.cellosaurus.cellosaurus import CellosaurusBaseAdapter
from src.models.disease import Disease
from src.models.node import Node, Relationship, EquivalentId
from src.models.pounce.data import Biospecimen, BiospecimenDiseaseRelationship


def parse_id(database: str, accession: str):
    if database == 'NCIt':
        return EquivalentId(id = accession, type = Prefix.NCIT).id_str()
    if database == 'ORDO':
        id_part = accession.split('Orphanet_')[1]
        return EquivalentId(id = id_part, type = Prefix.orphanet).id_str()
    raise Exception('Unknown database for disease association: ', database)

class DiseaseAdapter(CellosaurusBaseAdapter):

    def get_all(self) -> List[Union[Node, Relationship]]:
        root_node = self.get_root_node()
        disease_map = {}
        for disease_node in root_node.findall('./cell-line-list/cell-line/disease-list/xref'):
            database = disease_node.get('database')
            accession = disease_node.get('accession')
            disease_id = parse_id(database, accession)
            if disease_id in disease_map:
                continue

            disease_obj = Disease(
                id = disease_id,
                name = disease_node.find('./label').text
            )

            disease_map[disease_id] = disease_obj
        return list(disease_map.values())

class DiseaseCellLineRelationshipAdapter(CellosaurusBaseAdapter):
    def get_all(self) -> List[Union[Node, Relationship]]:
        root_node = self.get_root_node()
        cell_line_disease_edges = []
        disease_map = {}

        for cell_line_node in root_node.findall('./cell-line-list/cell-line'):
            accession = cell_line_node.find("./accession-list/accession[@type='primary']").text
            cell_line_id = EquivalentId(id = accession, type = Prefix.Cellosaurus).id_str()
            cell_line_obj = Biospecimen(id = cell_line_id)
            for disease_node in cell_line_node.findall('./disease-list/xref'):
                database = disease_node.get('database')
                accession = disease_node.get('accession')
                disease_id = parse_id(database, accession)
                if disease_id not in disease_map:
                    disease_map[disease_id] = Disease(
                        id=disease_id
                    )
                disease_obj = disease_map[disease_id]
                cell_line_disease_edges.append(
                    BiospecimenDiseaseRelationship(
                        start_node=cell_line_obj,
                        end_node=disease_obj
                    )
                )
        return cell_line_disease_edges
