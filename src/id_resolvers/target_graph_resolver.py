from typing import List, Any, Generator

from src.constants import Prefix
from src.id_resolvers.sqlite_cache_resolver import SqliteCacheResolver, MatchingPair
from src.interfaces.id_resolver import IdMatch
from src.models.node import EquivalentId
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



class TargetGraphResolver(SqliteCacheResolver):
    name = "TargetGraph Resolver"

    parsers: List[TargetGraphParser]

    @staticmethod
    def sort_matches(match_list: List[IdMatch]):
        match_list.sort(key=lambda x: scores.get(x.context[0], float('inf')))
        return match_list


    def get_version_info(self) -> str:
        version_info = []
        for parser in self.parsers:
            version_info.append(parser.get_version_info())
        return '\t'.join(version_info)


    def matching_ids(self) -> Generator[MatchingPair, Any, None]:
        for parser in self.parsers:
            yield from self.get_one_match(parser)

    def get_one_match(self, parser):
        for line in parser.all_rows():
            id = parser.get_id(line)
            equiv_ids = parser.get_equivalent_ids(line)
            yield MatchingPair(id=id, match=id, type='exact')
            for equiv_ids in equiv_ids:
                yield MatchingPair(id=id, match=equiv_ids.id_str(), type=equiv_ids.type.value)


class TargetGraphProteinResolver(TargetGraphResolver):
    name = "TargetGraph Protein Resolver"
    parsers: List[TargetGraphProteinParser]

    def __init__(self, file_paths: List[str], additional_ids: str = None, **kwargs):
        self.parsers = [
            TargetGraphProteinParser(file_path=path, additional_id_file_path=additional_ids)
            for path in file_paths]
        TargetGraphResolver.__init__(self, **kwargs)


class TargetGraphGeneResolver(TargetGraphResolver):
    name = "TargetGraph Protein Resolver"
    parsers: List[TargetGraphGeneParser]

    def __init__(self, file_path: str, **kwargs):
        self.parsers = [TargetGraphGeneParser(file_path=file_path)]
        TargetGraphResolver.__init__(self, **kwargs)


class TargetGraphTranscriptResolver(TargetGraphResolver):
    name = "TargetGraph Transcript Resolver"
    parsers: List[TargetGraphTranscriptParser]

    def __init__(self, file_path: str, **kwargs):
        self.parsers = [TargetGraphTranscriptParser(file_path=file_path)]
        TargetGraphResolver.__init__(self, **kwargs)
