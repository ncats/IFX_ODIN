import copy
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Dict
from dataclasses import dataclass, asdict, field

from src.interfaces.simple_enum import SimpleEnum
from src.models.node import Node, EquivalentId


class NoMatchBehavior(SimpleEnum):
    Skip = "Skip"
    Allow = "Allow"
    Error = "Error"


class MultiMatchBehavior(SimpleEnum):
    First = "First"
    All = "All"
    Error = "Error"


@dataclass
class IdMatch:
    input: str
    match: str
    equivalent_ids: List[str] = field(default_factory=list)
    context: List[str] = None

    def to_dict(self) -> Dict[str, str]:
        return asdict(self)

class IdResolver(ABC):
    class MatchKeys(Enum):
        matched = "matched"
        newborns = "newborns"
        unmatched = "unmatched"

    name: str
    no_match_behavior: NoMatchBehavior
    multi_match_behavior: MultiMatchBehavior
    add_labels_for_resolver_events: bool
    resolve_cache: Dict[str, List[IdMatch]]

    def __init__(self,
                 add_labels_for_resolver_events = False,
                 no_match_behavior = NoMatchBehavior.Allow,
                 multi_match_behavior = MultiMatchBehavior.All):
        print(f'creating ID resolver: {self.__class__.__name__}')
        self.add_labels_for_resolver_events = add_labels_for_resolver_events
        self.no_match_behavior = NoMatchBehavior.parse(no_match_behavior)
        self.multi_match_behavior = MultiMatchBehavior.parse(multi_match_behavior)
        self.resolve_cache = {}


    def _resolve_internal(self, input_nodes: List[Node]) -> (Dict[str, List[IdMatch]], set):
        output_dict = {}
        un_resolved_nodes = []
        unique_nodes_dict = {node.id: node for node in input_nodes}
        unique_nodes = list(unique_nodes_dict.values())
        for node in unique_nodes:
            if node.id in self.resolve_cache:
                output_dict[node.id] = self.resolve_cache[node.id]
            else:
                un_resolved_nodes.append(node)
        if len(un_resolved_nodes) == 0:
            return output_dict

        new_nodes = self.resolve_internal(un_resolved_nodes)
        for node_id, match_info in new_nodes.items():
            output_dict[node_id] = match_info
            self.resolve_cache[node_id] = match_info

        return output_dict

    def resolve_nodes(self, entries: List):
        id_map = self._resolve_internal(entries)
        updated_count, validated_count = 0, 0

        entity_map = {
            IdResolver.MatchKeys.matched: {},
            IdResolver.MatchKeys.newborns: {},
            IdResolver.MatchKeys.unmatched: {}
        }

        for entry in entries:
            if hasattr(entry, 'id') and entry.id in id_map:
                matches = id_map[entry.id]

                old_id = entry.id
                if matches:
                    if len(matches) > 0:
                        first_match = matches[0]
                        new_id = first_match.match

                        if old_id not in entity_map[IdResolver.MatchKeys.matched]:
                            full_xref_list = list(set([
                                EquivalentId.parse(equiv_id) for m in matches for equiv_id in m.equivalent_ids
                            ]))

                            first_entry = copy.deepcopy(entry)
                            first_entry.id = new_id
                            first_entry.xref = full_xref_list

                            if new_id != old_id:
                                first_entry.old_id = old_id
                                updated_count += 1
                                if self.add_labels_for_resolver_events:
                                    first_entry.add_label("Updated_ID")
                            else:
                                validated_count += 1
                                if self.add_labels_for_resolver_events:
                                    first_entry.add_label("Validated_ID")

                            entity_map[IdResolver.MatchKeys.matched][old_id] = first_entry

                    if len(matches) > 1:
                        if old_id not in entity_map[IdResolver.MatchKeys.newborns]:
                            entity_map[IdResolver.MatchKeys.newborns][old_id] = []
                            for subsequent_match in matches[1:]:
                                new_id = subsequent_match.match
                                new_entry = copy.deepcopy(entry)
                                new_entry.id = new_id
                                new_entry.xref = full_xref_list
                                new_entry.old_id = old_id
                                if self.add_labels_for_resolver_events:
                                    new_entry.add_label("Unmerged_ID")
                                if new_entry.id not in [node.id for node in entity_map[IdResolver.MatchKeys.newborns][old_id]]:
                                    entity_map[IdResolver.MatchKeys.newborns][old_id].append(new_entry)
                else:
                    if old_id not in entity_map[IdResolver.MatchKeys.unmatched]:
                        new_entry = copy.deepcopy(entry)
                        new_entry.old_id = old_id
                        if self.add_labels_for_resolver_events:
                            new_entry.add_label("Unmatched_ID")

                        entity_map[IdResolver.MatchKeys.unmatched][old_id] = new_entry

        print(f"updated {updated_count} ids, validated {validated_count} ids")
        return entity_map

    def parse_entity_map(self, entity_map):
        match_map = {k: [v] for k, v in entity_map[IdResolver.MatchKeys.matched].items()}
        if self.multi_match_behavior == MultiMatchBehavior.All:
            newborn_map = entity_map[IdResolver.MatchKeys.newborns]
            match_map.update(newborn_map)

        if self.no_match_behavior == NoMatchBehavior.Allow:
            unmatched_map = {k: [v] for k, v in entity_map[IdResolver.MatchKeys.unmatched].items()}
            match_map.update(unmatched_map)

        return match_map

    def parse_flat_node_list_from_map(self, entity_map):
        matched_entries = [v for k,v in entity_map[IdResolver.MatchKeys.matched].items()]
        newborn_entries = [node for key, node_list in entity_map[IdResolver.MatchKeys.newborns].items() for node in node_list]
        unmatched_entries = [v for k,v in entity_map[IdResolver.MatchKeys.unmatched].items()]
        if self.multi_match_behavior == MultiMatchBehavior.All:
            matched_entries.extend(newborn_entries)
            print(f"unmerged {len(newborn_entries)} ids - multi_match_behavior = 'All'")
        elif self.multi_match_behavior == MultiMatchBehavior.First:
            print(f"skipping {len(newborn_entries)} unmergable ids - multi_match_behavior = 'First'")
        elif self.multi_match_behavior == MultiMatchBehavior.Error:
            if len(newborn_entries) > 0:
                raise Exception(
                    "node list has degenerate identifiers - multi_match_behavior = 'Error'",
                    newborn_entries)

        if self.no_match_behavior == NoMatchBehavior.Skip:
            print(f"skipping {len(unmatched_entries)} unmatched ids - no_match_behavior = 'Skip'")
        elif self.no_match_behavior == NoMatchBehavior.Allow:
            matched_entries.extend(unmatched_entries)
            print(f"passing along {len(unmatched_entries)} unmatched ids - no_match_behavior = 'Allow'")
        elif self.no_match_behavior == NoMatchBehavior.Error:
            if len(unmatched_entries) > 0:
                raise Exception(
                    "node list has unmatched identifiers - no_match_behavior = 'Error'",
                    unmatched_entries
                )

        return matched_entries

    @abstractmethod
    def resolve_internal(self, input_nodes: List[Node]) -> Dict[str, List[IdMatch]]:
        raise NotImplementedError("derived classes must implement resolve_internal")

