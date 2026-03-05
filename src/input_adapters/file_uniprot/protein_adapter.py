import csv
from datetime import datetime
from typing import Generator, List, Union

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.keyword import ProteinKeywordEdge
from src.models.node import Node, Relationship
from src.models.pathway import ProteinPathwayRelationship
from src.models.protein import Protein
from src.shared.uniprot_file_reader import UniProtFileReader
from src.shared.uniprot_parser import UniProtParser


class ProteinAdapter(InputAdapter, UniProtFileReader):
    version_info: DatasourceVersionInfo

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.UniProt

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def __init__(self, file_path: str, version_file_path: str):
        UniProtFileReader.__init__(self, file_path=file_path)
        with open(version_file_path, 'r', encoding='utf-8') as vf:
            reader = csv.DictReader(vf, delimiter='\t')
            first_row = next(reader)
            version = first_row.get('version')
            version_date = first_row.get('version_date')
        self.version_info = DatasourceVersionInfo(
            version=version,
            version_date=datetime.strptime(version_date, '%d-%B-%Y').date(),
            download_date=self.download_date
        )

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        self.read_uniprot_file()
        proteins = []
        keywords_by_id = {}
        keyword_edges = []
        pathways_by_id = {}
        pathway_edges = []
        for row in self.raw_entries:
            protein = Protein(id=UniProtParser.get_primary_accession(row),
                              uniprot_id=UniProtParser.get_uniprot_id(row),
                              uniprot_entryType=UniProtParser.get_uniprot_entry_type(row),
                              uniprot_reviewed=UniProtParser.get_uniprot_reviewed(row),
                              uniprot_annotationScore=UniProtParser.get_uniprot_annotation_score(row),
                              description=UniProtParser.get_description(row),
                              uniprot_function=UniProtParser.get_description(row),
                              similarity=UniProtParser.get_similarity(row),
                              sequence=UniProtParser.get_sequence(row),
                              secondary_uniprot_ids=UniProtParser.get_secondary_accessions(row),
                              gene_name=UniProtParser.get_gene_name(row),
                              symbol=';'.join(UniProtParser.get_symbols(row)) if UniProtParser.get_symbols(row) else None,
                              name=UniProtParser.get_full_name(row)
                              )
            proteins.append(protein)

            protein_keywords = UniProtParser.get_keywords(row)
            if protein_keywords is not None:
                for keyword_id, keyword in protein_keywords.items():
                    if keyword_id not in keywords_by_id:
                        keywords_by_id[keyword_id] = keyword
                    keyword_edges.append(ProteinKeywordEdge(start_node=protein, end_node=keyword))

            protein_pathways = UniProtParser.get_pathways(row)
            if protein_pathways is not None:
                for pathway_id, pathway in protein_pathways.items():
                    if pathway_id not in pathways_by_id:
                        pathways_by_id[pathway_id] = pathway
                    pathway_edges.append(ProteinPathwayRelationship(start_node=protein, end_node=pathway, source='UniProt'))
        yield proteins
        yield list(keywords_by_id.values())
        yield keyword_edges

        yield list(pathways_by_id.values())
        yield pathway_edges
