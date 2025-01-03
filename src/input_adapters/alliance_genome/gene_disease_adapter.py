import gzip
import json
from datetime import datetime
from typing import List

from src.interfaces.input_adapter import NodeInputAdapter, RelationshipInputAdapter
from src.models.disease import Disease, GeneDiseaseRelationship
from src.models.gene import Gene
from src.models.node import Node


class GeneDiseaseAssociationAdapter(NodeInputAdapter, RelationshipInputAdapter):
    file_path: str
    database_version: str = ""
    name = "Alliance Genome Gene-Disease Relationship Adapter"

    def __init__(self, file_path: str):
        self.file_path = file_path

    def get_audit_trail_entries(self, obj) -> List[str]:
        if isinstance(obj, Node):
            return [f"Node created based on Alliance Genome version {self.database_version}"]
        return [f"Relationship data source: Alliance Genome version {self.database_version}"]

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
