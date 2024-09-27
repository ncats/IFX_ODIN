import copy
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Dict
from dataclasses import dataclass, asdict

from src.models.node import Node


class NoMatchBehavior(Enum):
    Skip = "Skip"
    Allow = "Allow"
    Error = "Error"


class MultiMatchBehavior(Enum):
    First = "First"
    All = "All"
    Error = "Error"


@dataclass
class IdMatch:
    input: str
    match: str
    context: List[str] = None

    def to_dict(self) -> Dict[str, str]:
        return asdict(self)


@dataclass
class IdResolverResult:
    best_matches: List[IdMatch] = None
    other_matches: List[IdMatch] = None

    def to_dict(self) -> Dict[str, str]:
        return asdict(self)

class IdResolver(ABC):
    class MatchKeys(Enum):
        matched = "matched"
        newborns = "newborns"
        unmatched = "unmatched"

    name: str
    no_match_behavior: NoMatchBehavior = NoMatchBehavior.Allow
    multi_match_behavior: MultiMatchBehavior = MultiMatchBehavior.All
    use_equivalent_ids: bool = True
    add_labels_for_resolver_events: bool = False

    resolve_cache: Dict[str, IdResolverResult] = {}

    def _resolve_internal(self, input_nodes: List[Node]) -> (Dict[str, IdResolverResult], set):
        output_dict = {}
        cached_ids = set()
        un_resolved_nodes = []
        unique_nodes_dict = {node.id: node for node in input_nodes}
        unique_nodes = list(unique_nodes_dict.values())
        for node in unique_nodes:
            if node.id in self.resolve_cache:
                output_dict[node.id] = self.resolve_cache[node.id]
                cached_ids.add(node.id)
            else:
                un_resolved_nodes.append(node)
        if len(un_resolved_nodes) == 0:
            return output_dict, cached_ids

        new_nodes = self.resolve_internal(un_resolved_nodes)
        for node_id, match_info in new_nodes.items():
            output_dict[node_id] = match_info
            self.resolve_cache[node_id] = match_info

        return output_dict, cached_ids

    def resolve_nodes(self, entries: List):
        id_map, cached_ids = self._resolve_internal(entries)
        updated_count, validated_count = 0, 0

        entity_map = {
            IdResolver.MatchKeys.matched: {},
            IdResolver.MatchKeys.newborns: {},
            IdResolver.MatchKeys.unmatched: {}
        }

        for entry in entries:
            if hasattr(entry, 'id') and entry.id in id_map:
                matches = id_map[entry.id]

                if matches.best_matches and len(matches.best_matches) > 0:
                    old_id = entry.id
                    new_entry = entry
                    new_entry.id = matches.best_matches[0].match

                    entity_map[IdResolver.MatchKeys.matched][entry.id] = new_entry
                    if new_entry.id != old_id:
                        if entry.id not in cached_ids:
                            new_entry.provenance.append(f"ID updated from {old_id} by {self.name}")
                        if self.add_labels_for_resolver_events:
                            new_entry.add_label("Updated_ID")
                        updated_count += 1
                    else:
                        if entry.id not in cached_ids:
                            new_entry.provenance.append(f"ID validated by {self.name}")
                        if self.add_labels_for_resolver_events:
                            new_entry.add_label("Validated_ID")
                        validated_count += 1

                    for match in matches.best_matches[1:]:
                        new_entry = copy.deepcopy(new_entry)
                        old_id = new_entry.id
                        new_entry.id = match.match
                        if self.add_labels_for_resolver_events:
                            new_entry.add_label("Unmerged_ID")
                        if entry.id not in cached_ids:
                            new_entry.provenance.append(f"ID unmerged from {old_id} by {self.name}")

                        if entry.id not in entity_map[IdResolver.MatchKeys.newborns]:
                            entity_map[IdResolver.MatchKeys.newborns][entry.id] = [new_entry]
                        else:
                            if new_entry.id not in [node.id for node in entity_map[IdResolver.MatchKeys.newborns][entry.id]]:
                                entity_map[IdResolver.MatchKeys.newborns][entry.id].append(new_entry)
                else:
                    new_entry = entry
                    if self.add_labels_for_resolver_events:
                        new_entry.add_label("Unmatched_ID")

                    if entry.id not in cached_ids:
                        new_entry.provenance.append(f"ID not found by {self.name}")
                    entity_map[IdResolver.MatchKeys.unmatched][entry.id] = new_entry
            else:
                raise Exception("ID Resolver had no entry for an input, this shouldn't happen", entry)

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
    def resolve_internal(self, input_nodes: List[Node]) -> Dict[str, IdResolverResult]:
        raise NotImplementedError("derived classes must implement resolve_internal")

