from dataclasses import dataclass
from typing import List

from src.input_adapters.sqlite_ramp.metabolite_adapter import MetaboliteAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.metabolite import Metabolite
from src.models.node import Relationship
from src.models.version import DatabaseVersion

# This adapter reads from two RaMP sqlite databases and defines relationships between metabolite nodes that share input IDs
# It's used for comparing metabolites between versions of RaMP or changes in RaMP entity resolution code

@dataclass
class SharesMetaboliteIDrelationship(Relationship):
    count: int = 0


class MetaboliteSetRelationshipAdapter(InputAdapter):
    name: str = "Metabolite Set Relationship Adapter"

    def get_audit_trail_entries(self, obj: SharesMetaboliteIDrelationship) -> List[str]:
        left_version: DatabaseVersion = self.left_adapter.get_database_version()
        right_version: DatabaseVersion = self.right_adapter.get_database_version()
        return [f"Relationship based on {obj.count} overlapping IDs between {left_version.id} and {right_version.id}"]

    left_adapter: MetaboliteAdapter
    right_adapter: MetaboliteAdapter

    def get_all(self) -> List[SharesMetaboliteIDrelationship]:
        left_mets = self.left_adapter.get_all()
        right_mets = self.right_adapter.get_all()

        reverse_lookup = {}

        for metabolite in left_mets:
            ramp_id = metabolite.id
            equiv_ids = [equiv_id.id for equiv_id in metabolite.xref]
            for id in equiv_ids:
                if id in reverse_lookup:
                    existing_id = reverse_lookup[id]
                    if ramp_id != existing_id:
                        raise Exception(f"this wasn't supposed to happen: both {ramp_id} and {existing_id} mention {id}")
                else:
                    reverse_lookup[id] = ramp_id

        relationship_map = {}
        for metabolite in right_mets:
            ramp_id = metabolite.id
            equiv_ids = [equiv_id.id for equiv_id in metabolite.xref]
            for id in equiv_ids:
                if id not in reverse_lookup:
                    print(f"{id} exists in the 'right' list and not the 'left'. Skipping it...")
                    continue
                else:
                    other_ramp_id = reverse_lookup[id]
                    composite_id = f"{other_ramp_id}|{ramp_id}"
                    if composite_id in relationship_map:
                        val = relationship_map[composite_id]
                        relationship_map[composite_id] = val + 1
                    else:
                        relationship_map[composite_id] = 1

        relationships = []
        for key, count in relationship_map.items():
            start_id, end_id = key.split('|')
            relationships.append(
                SharesMetaboliteIDrelationship(
                    start_node=Metabolite(id=start_id),
                    end_node=Metabolite(id=end_id),
                    count=count))

        return relationships

    def set_left(self, ramp_file: str):
        self.left_adapter = MetaboliteAdapter(sqlite_file=ramp_file)
        return self

    def set_right(self, ramp_file: str):
        self.right_adapter = MetaboliteAdapter(sqlite_file=ramp_file)
        return self
