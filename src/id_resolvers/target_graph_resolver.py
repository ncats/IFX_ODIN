from typing import List, Dict

from src.interfaces.id_resolver import IdResolver, IdResolverResult, IdMatch
from src.models.node import Node
from src.shared.targetgraph_parser import TargetGraphGeneParser, TargetGraphTranscriptParser, TargetGraphProteinParser, \
    TargetGraphParser

scores = {
    "exact": 0,
    "primary accession": 1,
    "secondary accession": 2,
    "NCBI Gene ID": 2.5,
    "uniprot kb": 3,
    "symbol": 4,
    "full name": 5,
    "Ensembl": 6,
    "STRING": 7,
    "RefSeq": 8,
    "short name": 9,
    "synonym": 10
}

class TargetGraphResolver(IdResolver):
    name = "TargetGraph Resolver"
    reverse_lookup: dict
    alias_lookup: dict

    parsers: List[TargetGraphParser]

    @staticmethod
    def sort_matches(match_list: List[IdMatch]):
        match_list.sort(key=lambda x: scores.get(x.context[0], float('inf')))
        return match_list

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.reverse_lookup: Dict[str, List[IdMatch]] = {}
        self.alias_lookup: Dict[str, set] = {}
        for pp in self.parsers:
            self.parse_file(pp)

    def parse_file(self, parser):
        for line in parser.all_rows():
            id = parser.get_id(line)
            equiv_ids = parser.get_equivalent_ids(line)
            self.reverse_lookup[id] = [
                IdMatch(input=id, match=id, context=['exact'])
            ]
            self.alias_lookup[id] = set()
            for equiv_id in equiv_ids:
                id_str = equiv_id.id_str()
                self.alias_lookup.get(id).add(id_str)
                if id_str not in self.reverse_lookup:
                    self.reverse_lookup[id_str] = [
                        IdMatch(input=id_str, match=id, context=[equiv_id.type.value])
                    ]
                else:
                    list = self.reverse_lookup[id_str]
                    same_id_match = [matching_entry for matching_entry in list if
                                     matching_entry.match == id]
                    if len(same_id_match) > 0:
                        same_id_match[0].context.append(equiv_id.type.value)
                    else:
                        list.append(
                            IdMatch(input=id_str, match=id, context=[equiv_id.type.value])
                        )

    def get_matches_for_id(self, id: str):
        matches = self.reverse_lookup.get(id)
        if matches is not None:
            for match in matches:
                match.equivalent_ids = self.alias_lookup.get(match.match)
        return matches

    def resolve_internal(self, input_nodes: List[Node]) -> Dict[str, IdResolverResult]:
        result_list = {}
        for node in input_nodes:
            result_list[node.id] = IdResolverResult(matches=self.get_matches_for_id(node.id))
        return result_list

class TargetGraphProteinResolver(TargetGraphResolver):
    name = "TargetGraph Protein Resolver"
    parsers: List[TargetGraphProteinParser]

    def __init__(self, file_paths: List[str], additional_ids: str, **kwargs):
        self.parsers = [
            TargetGraphProteinParser(file_path=path, additional_id_file_path=additional_ids)
            for path in file_paths]
        TargetGraphResolver.__init__(self, **kwargs)
        super().__init__()


class TargetGraphGeneResolver(TargetGraphResolver):
    name = "TargetGraph Protein Resolver"
    parsers: List[TargetGraphGeneParser]

    def __init__(self, file_path: str):
        self.parsers = [TargetGraphGeneParser(file_path=file_path)]
        super().__init__()

class TargetGraphTranscriptResolver(TargetGraphResolver):
    name = "TargetGraph Transcript Resolver"
    parsers: List[TargetGraphTranscriptParser]
    def __init__(self, file_path: str):
        self.parsers = [TargetGraphTranscriptParser(file_path=file_path)]
        super().__init__()


