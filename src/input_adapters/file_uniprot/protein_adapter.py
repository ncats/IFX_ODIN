from src.interfaces.input_adapter import InputAdapter
from src.models.protein import Protein
from src.shared.uniprot_file_reader import UniProtFileReader
from src.shared.uniprot_parser import UniProtParser


class ProteinAdapter(InputAdapter, UniProtFileReader):

    def __init__(self, file_path: str):
        UniProtFileReader.__init__(self, file_path=file_path)

    def get_all(self):
        self.read_uniprot_file()
        proteins = []
        for row in self.raw_entries:
            proteins.append(
                Protein(id=UniProtParser.get_primary_accession(row),
                        description=UniProtParser.get_description(row),
                        sequence=UniProtParser.get_sequence(row))
            )
        return proteins
