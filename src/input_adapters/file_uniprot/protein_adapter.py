from typing import Generator, List

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.protein import Protein
from src.shared.uniprot_file_reader import UniProtFileReader
from src.shared.uniprot_parser import UniProtParser


class ProteinAdapter(InputAdapter, UniProtFileReader):

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.UniProt

    def get_version(self) -> DatasourceVersionInfo:
        return self.get_version_info()

    def __init__(self, file_path: str):
        UniProtFileReader.__init__(self, file_path=file_path)

    def get_all(self) -> Generator[List[Protein], None, None]:
        self.read_uniprot_file()
        proteins = []
        go_associations = []
        for row in self.raw_entries:
            isoforms = UniProtParser.get_isoforms(row)
            protein = Protein(id=UniProtParser.get_primary_accession(row),
                              uniprot_id=UniProtParser.get_uniprot_id(row),
                              description=UniProtParser.get_description(row),
                              sequence=UniProtParser.get_sequence(row),
                              secondary_uniprot_ids=UniProtParser.get_secondary_accessions(row),
                              gene_name=UniProtParser.get_gene_name(row),
                              symbol=';'.join(UniProtParser.get_symbols(row)) if UniProtParser.get_symbols(row) else None,
                              name=UniProtParser.get_full_name(row),
                              isoforms = isoforms
                              )
            proteins.append(protein)
            go_associations.extend(UniProtParser.get_go_term_associations(row, protein))
        yield proteins
        yield go_associations
