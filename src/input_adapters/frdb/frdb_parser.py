import logging
from dataclasses import fields
from datetime import datetime
from decimal import Decimal
from typing import get_origin, get_args, Union, List

from src.constants import Prefix
from src.input_adapters.frdb.frdb_classes import Ligand, Database, Vendor, \
    LigandDatabaseRelationship, LigandVendorRelationship, Condition, LigandConditionRelationship, LigandConditionDetails
from src.models.node import EquivalentId

logger = logging.getLogger(__name__)

# finished : sourcing, conditions

IGNORED_INPUT_KEYS = {'conditions', 'ddi', 'sourcing', 'targets', 'pk', 'toxicity'}

class FRDBParser:

    def get_best_ligand_id(self, ligand_dict: dict):
        order = [
            ('unii', Prefix.UNII),
            ('chembl_id', Prefix.CHEMBL_COMPOUND),
            ('cid', Prefix.PUBCHEM_COMPOUND),
            ('cas', Prefix.CAS),
            ('chemspider_id', Prefix.chemspider),
            ('compound_id', Prefix.FRDB)
        ]

        for field, prefix in order:
            if field in ligand_dict and ligand_dict[field]:
                id = EquivalentId(ligand_dict[field], prefix)
                return id.id_str()
        print(ligand_dict)
        raise ValueError(f"No valid ID found in input: {ligand_dict}")

    def get_best_condition_id(self, condition_dict: dict):
        # some mesh IDs are for the compound!
            # biolink:Disease	1448
            # biolink:PhenotypicFeature	164
            # ?	116
            # biolink:OrganismTaxon	57
            # biolink:ChemicalEntity	20
            # biolink:SmallMolecule	8
            # biolink:AnatomicalEntity	6
            # biolink:GrossAnatomicalStructure	3
            # biolink:Cell	2
            # biolink:ComplexMolecularMixture	1
            # biolink:MolecularMixture	1
        if 'do_id' in condition_dict and condition_dict['do_id'] is not None and condition_dict['do_id'] != 'Unknown':
            return EquivalentId(condition_dict['do_id'], Prefix.DOID).id_str()
        if 'name' in condition_dict and condition_dict['name'] == 'Unknown' and condition_dict['name'] is not None:
            return EquivalentId(condition_dict['name'], Prefix.Name).id_str()
        return None

    def parse_ligands(self, input_obj: dict):
        field_defs = {f.name: f.type for f in fields(Ligand)}
        result = {}

        known_fields = set(field_defs.keys()) | {'compound_id'}  # manually handled
        unexpected_fields = set(input_obj.keys()) - known_fields - IGNORED_INPUT_KEYS
        if unexpected_fields:
            logger.warning(f"Unexpected fields in input: {sorted(unexpected_fields)}")

        for key, value in input_obj.items():
            if key not in field_defs:
                continue

            expected_type = field_defs[key]
            resolved_type = self._resolve_type(expected_type)

            if value is None or value == '':
                result[key] = None
                continue

            if key == "modified_datetime" and isinstance(value, Decimal):
                result[key] = datetime.fromtimestamp(float(value) / 1000.0)
                continue

            try:
                result[key] = self._coerce_value(value, resolved_type)
            except Exception:
                logger.warning(f"Failed to coerce value for field '{key}': {value!r}")
                result[key] = value

        result['id'] = self.get_best_ligand_id(input_obj)

        return Ligand(**result)

    def _resolve_type(self, annotation):
        origin = get_origin(annotation)
        args = get_args(annotation)

        if origin is Union and type(None) in args:
            non_none = [arg for arg in args if arg is not type(None)]
            return non_none[0] if non_none else str
        return annotation

    def _coerce_value(self, value, expected_type):
        origin = get_origin(expected_type)
        args = get_args(expected_type)

        if origin in (list, List):
            item_type = args[0] if args else str
            value = [val for val in value if val != 'None' and val != '' and val is not None and val != 'Unknown']
            if isinstance(value, list):
                return [self._coerce_value(v, item_type) for v in value]
            elif isinstance(value, str):
                return [self._coerce_value(v.strip(), item_type) for v in value.split(',')]
            else:
                return [self._coerce_value(value, item_type)]

        if expected_type == bool:
            if isinstance(value, str):
                return value.lower() in ("true", "1", "yes")
            return bool(value)
        elif expected_type == int:
            return int(float(value))
        elif expected_type == float:
            return float(value)
        elif expected_type == str:
            return str(value)
        elif expected_type == datetime:
            if isinstance(value, (int, float, Decimal)):
                return datetime.fromtimestamp(float(value) / 1000.0)
            elif isinstance(value, str):
                return datetime.fromisoformat(value)
        return value

    def parse_sources(self, ligand_dict: dict, ligand_obj: Ligand):
        if 'sourcing' not in ligand_dict:
            return [], []
        sources = ligand_dict['sourcing']
        nodes = []
        edges = []
        for entry in sources:
            name = entry['sourcing_vendor_name']
            if not name:
                continue
            if entry['sourcing_vendor_type'] == 'database':
                source_obj = Database(id = name)
                rel = LigandDatabaseRelationship(
                    start_node=ligand_obj,
                    end_node=source_obj,
                    substance_id=entry['sourcing_vendor_substance_id']
                )
                if 'sourcing_vendor_substance_url' in entry:
                    rel.substance_url=entry['sourcing_vendor_substance_url']
            else:
                source_obj = Vendor(id = name)
                rel = LigandVendorRelationship(
                    start_node=ligand_obj,
                    end_node=source_obj,
                    substance_id=entry['sourcing_vendor_substance_id']
                )

            nodes.append(source_obj)
            edges.append(rel)
        return nodes, edges

    def parse_conditions(self, ligand_dict: dict, ligand_obj: Ligand):
        if 'conditions' not in ligand_dict:
            return [], []
        conditions = ligand_dict['conditions']
        nodes = []
        edges = []

        condition_field_defs = {f.name: f.type for f in fields(Condition)}
        field_defs = {f.name: f.type for f in fields(Condition)}
        field_defs.update({f.name: f.type for f in fields(LigandConditionDetails)})

        known_fields = set(field_defs.keys())

        for input_obj in conditions:
            condition_id = self.get_best_condition_id(input_obj)
            if condition_id is None:
                continue
            ignored_keys = {'condition_id', 'condition_do', 'condition_mesh'}
            unexpected_fields = set(input_obj.keys()) - known_fields - ignored_keys
            if unexpected_fields:
                logger.warning(f"Unexpected fields in input: {sorted(unexpected_fields)}")
                for field in unexpected_fields:
                    logger.warning(f"{field}: {ligand_dict}")

            condition = {}
            condition['id'] = condition_id
            condition_details = {}

            for key, value in input_obj.items():
                if key not in field_defs:
                    continue

                if key in condition_field_defs:
                    entity_to_use = condition
                else:
                    entity_to_use = condition_details

                expected_type = field_defs[key]
                resolved_type = self._resolve_type(expected_type)

                if value is None or value == '' or value == 'Unknown':
                    entity_to_use[key] = None
                    continue
                try:
                    entity_to_use[key] = self._coerce_value(value, resolved_type)
                except Exception:
                    logger.warning(f"Failed to coerce value for field '{key}': {value!r}")
                    entity_to_use[key] = value

            condition_obj = Condition(**condition)
            nodes.append(condition_obj)
            edges.append(LigandConditionRelationship(
                start_node = ligand_obj,
                end_node = condition_obj,
                details = [LigandConditionDetails(**condition_details)]))

        return nodes, edges