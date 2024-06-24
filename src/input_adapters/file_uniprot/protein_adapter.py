from typing import List

from src.interfaces.input_adapter import NodeInputAdapter
from src.models.protein import Protein
from src.output_adapters.generic_labels import NodeLabel
from src.shared.uniprot_file_reader import UniProtFileReader
from src.shared.uniprot_parser import UniProtParser


class ProteinAdapter(NodeInputAdapter, UniProtFileReader):
    def get_audit_trail_entries(self, obj) -> List[str]:
        return [f"description and sequence updated by {self.name}"]

    name = "UniProt Protein Adapter"

    def __init__(self, file_path: str):
        UniProtFileReader.__init__(self, file_path=file_path)

    def get_all(self):
        self.read_uniprot_file()
        proteins = []
        for row in self.raw_entries:
            proteins.append(
                Protein(id=UniProtParser.get_primary_accession(row),
                        description=UniProtParser.get_description(row),
                        sequence=UniProtParser.get_sequence(row),
                        labels=[NodeLabel.Protein])
            )
        return proteins
