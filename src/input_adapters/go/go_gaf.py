import gzip
import os
from datetime import datetime, date
from typing import List, Generator

from src.constants import Prefix, DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.go_term import GoType, ProteinGoTermRelationship, GoTerm, GoEvidence
from src.models.node import EquivalentId
from src.models.protein import Protein


class ProteinGoTermEdgeAdapter(InputAdapter):

    def get_datasource_name(self) -> DataSourceName:
        if self.source == "UniProt":
            return DataSourceName.UniProt
        if self.source == "GO":
            return DataSourceName.GO
        return DataSourceName.Unknown

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            download_date=self.download_date
        )

    gaf_file_name: str
    source: str
    download_date: date

    def __init__(self, gaf_file_name: str, source: str):
        self.gaf_file_name = gaf_file_name
        self.source = source
        self.download_date = datetime.fromtimestamp(os.path.getmtime(gaf_file_name)).date()

    def get_all(self) -> Generator[List[ProteinGoTermRelationship], None, None]:
        pro_go_edges: List[ProteinGoTermRelationship] = []

        with gzip.open(self.gaf_file_name, 'rt') as file:
            count = 0
            for line in file:
                count += 1
                if line.startswith('!'):
                    continue
                parsed_line = parse_gaf_line(line)

                if parsed_line['evidence_code'] in GoEvidence.no_data_codes():
                    continue

                pro_id = EquivalentId(id=parsed_line['db_object_id'], type=Prefix.UniProtKB)
                pro_obj = Protein(id=pro_id.id_str())

                go_id = EquivalentId(id=parsed_line['go_id'].split(':', 1)[1], type=Prefix.GO)
                go_obj = GoTerm(id=go_id.id_str())

                go_evidence = GoEvidence.parse_by_abbreviation(
                    abbreviation=parsed_line['evidence_code'],
                    assigned_by=parsed_line['assigned_by'])

                pro_go_edges.append(ProteinGoTermRelationship(
                    start_node=pro_obj,
                    end_node=go_obj,
                    evidence=[go_evidence]
                ))
                if count >= self.batch_size:
                    yield pro_go_edges
                    count = 0
                    pro_go_edges: List[ProteinGoTermRelationship] = []

        yield pro_go_edges


def parse_gaf_line(line):
    columns = line.strip().split('\t')
    return {
        "db": columns[0],
        "db_object_id": columns[1],
        "db_object_symbol": columns[2],
        "qualifier": columns[3],
        "go_id": columns[4],
        "db_reference": columns[5],  # GO_REF:
        "evidence_code": columns[6],
        "with_or_from": columns[7],
        "type": GoType.parse(columns[8]),  # C, P, F
        "term": columns[9],
        "db_object_synonym": columns[10],
        "db_object_type": columns[11],
        "taxon": columns[12],  # protein taxon:9606
        "date": columns[13],
        "assigned_by": columns[14],
        "annotation_extension": columns[15] if len(columns) > 15 else None,
        "gene_product_form_id": columns[16] if len(columns) > 16 else None
    }
