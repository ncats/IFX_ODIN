import json
from typing import List

from src.interfaces.simple_enum import SimpleEnum

class FieldConflictBehavior(SimpleEnum):
    KeepFirst = "KeepFirst"
    KeepLast = "KeepLast"


class RecordMerger:
    field_conflict_behavior: FieldConflictBehavior

    def __init__(self, field_conflict_behavior: FieldConflictBehavior = FieldConflictBehavior.KeepFirst):
        self.field_conflict_behavior = field_conflict_behavior

    def parse_list_and_field_keys(self, example_record):
        forbidden_keys = ['id', 'start_id', 'end_id']
        list_keys = []
        field_keys = []

        special_handling_fields = ['xref', 'provenance', 'entity_resolution']

        for prop in example_record.keys():
            if prop in special_handling_fields:
                continue
            if prop in forbidden_keys:
                continue
            if isinstance(example_record[prop], list):
                list_keys.append(prop)
            else:
                field_keys.append(prop)

        return field_keys, list_keys

    def get_example_record(self, records: List[dict]):
        example_record = {}
        for rec in records:
            for k, v in rec.items():
                if k not in example_record:
                    example_record[k] = v
        return example_record

    def merge_records(self, records, merged_record_map, nodes_or_edges='nodes'):
        def node_key(record):
            return record['id']
        def edge_key(record):
            return (record['start_id'], record['end_id'])

        key_func = node_key if nodes_or_edges == 'nodes' else edge_key

        created_keys = ['updates', 'creation', 'resolved_ids']
        forbidden_keys = ['id', 'start_id', 'end_id', 'xref', 'provenance', 'entity_resolution', *created_keys]

        example_record = self.get_example_record(records)
        field_keys, list_keys = self.parse_list_and_field_keys(example_record)

        for record in records:
            if key_func(record) not in merged_record_map:
                record['creation'] = record['provenance']
                del record['provenance']
                record['resolved_ids'] = [record['entity_resolution']]
                del record['entity_resolution']

                reckeys = list(record.keys())
                for prop in reckeys:
                    if prop in field_keys:
                        if record[prop] is None:
                            del record[prop]
                    if prop in list_keys:
                        if isinstance(record[prop], list) and len(record[prop]) == 0:
                            del record[prop]
                merged_record_map[key_func(record)] = record
            else:
                existing_node = merged_record_map[key_func(record)]
                if record['entity_resolution'] not in existing_node['resolved_ids']:
                    existing_node['resolved_ids'].append(record['entity_resolution'])

                updates = existing_node.get('updates', [])
                existing_node['updates'] = updates

                for prop in record.keys():
                    if prop in forbidden_keys:
                        continue
                    if prop.startswith('_'):
                        continue
                    if record[prop] is None or (isinstance(record[prop], list) and len(record[prop]) == 0):
                        continue
                    existing_prop_value = existing_node.get(prop)
                    if prop in field_keys:
                        if existing_prop_value is None:
                            updates.append(f"{prop}\tNULL\t{record[prop]}\t{record['provenance']}\t{self.field_conflict_behavior.value}")
                            existing_node[prop] = record[prop]
                        elif record[prop] != existing_prop_value:
                            updates.append(f"{prop}\t{existing_prop_value}\t{record[prop]}\t{record['provenance']}\t{self.field_conflict_behavior.value}")
                            if self.field_conflict_behavior == FieldConflictBehavior.KeepLast:
                                existing_node[prop] = record[prop]
                    elif prop in list_keys:
                        if existing_prop_value is not None and len(existing_prop_value) > 0:
                            updates.append(f"{prop}\t{len(existing_prop_value)} entries already there\t{len(record[prop])} entries being merged\t{record['provenance']}")
                            if isinstance(record[prop][0], dict):
                                combined = existing_prop_value + record[prop]
                                deduped = list({json.dumps(d, sort_keys=True) for d in combined})
                                existing_node[prop] = [json.loads(d) for d in deduped]
                            else:
                                existing_node[prop] = list(set(existing_prop_value + record[prop]))
                        else:
                            updates.append(f"{prop}\tNULL\t{len(record[prop])} entries being merged\t{record['provenance']}")

                            value = record[prop]
                            if value and isinstance(value[0], dict):
                                deduped = list({json.dumps(d, sort_keys=True) for d in value})
                                existing_node[prop] = [json.loads(d) for d in deduped]
                            else:
                                existing_node[prop] = list(set(value))
                    else:
                        raise Exception('key is neither field nor list', prop, record)

        return list(merged_record_map.values())
