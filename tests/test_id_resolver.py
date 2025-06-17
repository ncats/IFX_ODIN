from typing import List, Dict

from src.interfaces.id_resolver import IdResolver, IdMatch, NoMatchBehavior, MultiMatchBehavior
from src.models.node import Node


class TempResolver(IdResolver):
    def resolve_internal(self, input_nodes: List[Node]) -> Dict[str, List[IdMatch]]:
        return {'test': []}



def test_simple_merge():
    testResolver = TempResolver(types=['Node'], no_match_behavior=NoMatchBehavior.Allow, multi_match_behavior=MultiMatchBehavior.All)
    entries = [Node(id='1'), Node(id='2')]
    oneEquivs = ['EC:1','EC:1a']
    oneAEquivs = ['EC:2']
    id_map: Dict[str, List[IdMatch]] = {
        '1': [IdMatch('1', 'Match:1', equivalent_ids=oneEquivs), IdMatch('1', 'Match:1a', equivalent_ids=oneAEquivs)]
    }

    merged_map = testResolver.get_merged_map(entries, id_map)

    assert len(merged_map) == 3
    assert len(merged_map[IdResolver.MatchKeys.matched]) == 1
    assert merged_map[IdResolver.MatchKeys.matched]['1'].id == 'Match:1'
    assert len(merged_map[IdResolver.MatchKeys.matched]['1'].xref) == 3

    assert len(merged_map[IdResolver.MatchKeys.newborns]) == 1
    assert len(merged_map[IdResolver.MatchKeys.newborns]['1']) == 1
    assert merged_map[IdResolver.MatchKeys.newborns]['1'][0].id == 'Match:1a'
    assert len(merged_map[IdResolver.MatchKeys.newborns]['1'][0].xref) == 3

    assert len(merged_map[IdResolver.MatchKeys.unmatched]) == 1
    assert merged_map[IdResolver.MatchKeys.unmatched]['2'].id == '2'