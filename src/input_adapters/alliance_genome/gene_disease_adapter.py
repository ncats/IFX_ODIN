import gzip
import json
from datetime import datetime
from typing import List

from src.interfaces.input_adapter import InputAdapter
from src.models.disease import Disease, GeneDiseaseRelationship
from src.models.gene import Gene
from src.models.node import Node


class GeneDiseaseAssociationAdapter(InputAdapter):
    file_path: str
    database_version: str = ""

    def __init__(self, file_path: str):
        self.file_path = file_path

    def get_all(self) -> List[Node]:
        disease_map = {}
        relationships = []
        with gzip.open(self.file_path, 'rt', encoding='utf-8') as data_file:
            data = json.load(data_file)
            self.database_version = data['metadata']['databaseVersion']
            for item in data['data']:
                disease_id = item['DOID']
                name = item['DOtermName']
                if disease_id not in disease_map:
                    disease_obj = Disease(id=disease_id)
                    disease_obj.full_name = name
                    disease_map[disease_id] = disease_obj

                if item['DBobjectType'] != 'gene':
                    raise (Exception(item))

                gene_id = item['DBObjectID']
                type = item['AssociationType']
                evidence_code = item['EvidenceCode']
                evidence_term = item['EvidenceCodeName']
                reference = item['Reference']
                date = datetime.strptime(item['Date'], '%Y%m%d')
                source = item['Source']
                if type == 'is_not_implicated_in':
                    continue
                relationships.append(
                    GeneDiseaseRelationship(
                        start_node=Gene(id=gene_id),
                        end_node=Disease(id=disease_id),
                        types=[type],
                        evidence_codes=[evidence_code],
                        evidence_terms=[evidence_term],
                        references=[reference],
                        dates=[date],
                        sources=[source]
                    )
                )
        return [*disease_map.values(), *relationships]
