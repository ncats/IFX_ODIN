import gzip
import re
from pathlib import Path
from typing import Generator, Iterable, List, Optional, Union

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.chebi import (
    Application,
    BiologicalRole,
    ChemicalEntity,
    ChemicalRole,
    HasApplicationEdge,
    HasBiologicalRoleEdge,
    HasChemicalRoleEdge,
    HasFunctionalParentEdge,
    HasParentHydrideEdge,
    HasPartEdge,
    HasRoleEdge,
    IsAEdge,
    IsConjugateAcidOfEdge,
    IsConjugateBaseOfEdge,
    IsEnantiomerOfEdge,
    IsSubstituentGroupFromEdge,
    IsTautomerOfEdge,
    Property,
    Role,
    SourceEdge,
    Synonym,
    Term,
    Xref,
)
from src.models.datasource_version_info import DatasourceVersionInfo


CHEBI_PROPERTY_FIELD_MAP = {
    "chemrof:charge": "charge",
    "chemrof:generalized_empirical_formula": "formula",
    "chemrof:mass": "mass",
    "chemrof:monoisotopic_mass": "monoisotopic_mass",
    "chemrof:smiles_string": "smiles",
    "chemrof:inchi_string": "inchi",
    "chemrof:inchi_key_string": "inchi_key",
    "chemrof:wurcs_representation": "wurcs",
}

CHEBI_RELATIONSHIP_EDGE_CLASSES = {
    "is_a": IsAEdge,
    "RO:0018038": HasFunctionalParentEdge,
    "RO:0018033": IsConjugateBaseOfEdge,
    "RO:0018034": IsConjugateAcidOfEdge,
    "BFO:0000051": HasPartEdge,
    "RO:0018039": IsEnantiomerOfEdge,
    "RO:0018036": IsTautomerOfEdge,
    "RO:0018040": HasParentHydrideEdge,
    "RO:0018037": IsSubstituentGroupFromEdge,
}

CHEBI_ROLE_PREDICATE = "RO:0000087"
CHEBI_SUBSTITUENT_GROUP_PREDICATE = "RO:0018037"
CHEBI_ROLE_ROOT_ID = "CHEBI:50906"
CHEBI_CHEMICAL_ENTITY_ROOT_IDS = {"CHEBI:33250", "CHEBI:23367", "CHEBI:59999"}
CHEBI_ROLE_CLASSES = {
    "CHEBI:33232": (Application, HasApplicationEdge),
    "CHEBI:24432": (BiologicalRole, HasBiologicalRoleEdge),
    "CHEBI:51086": (ChemicalRole, HasChemicalRoleEdge),
}
CHEBI_STRUCTURE_PROPERTY_IDS = {
    "chemrof:inchi_key_string",
    "chemrof:inchi_string",
    "chemrof:smiles_string",
}
CHEBI_CHEMICAL_ENTITY_RELATIONSHIP_PREDICATES = {
    "RO:0018038",
    "RO:0018033",
    "RO:0018034",
    "RO:0018039",
    "RO:0018036",
    "RO:0018040",
    "RO:0018037",
}
CHEBI_CHEMICAL_ENTITY_ADJACENCY_PREDICATES = CHEBI_CHEMICAL_ENTITY_RELATIONSHIP_PREDICATES | {
    "BFO:0000051",
}

CHEBI_PREDICATE_LABELS = {
    CHEBI_ROLE_PREDICATE: "has role",
    "RO:0018038": "has functional parent",
    "RO:0018033": "is conjugate base of",
    "RO:0018034": "is conjugate acid of",
    "BFO:0000051": "has part",
    "RO:0018039": "is enantiomer of",
    "RO:0018036": "is tautomer of",
    "RO:0018040": "has parent hydride",
    "RO:0018037": "is substituent group from",
}


class FullOboAdapter(InputAdapter):
    single_source = True

    def __init__(
        self,
        data_source,
        max_terms: Optional[int] = None,
    ):
        self.file_path = str(data_source.file("chebi.obo.gz"))
        self.version_info = data_source.version_info()
        self.max_terms = max_terms

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.ChEBI

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def get_all(self) -> Generator[List[Union[ChemicalEntity, Role, SourceEdge, HasRoleEdge]], None, None]:
        node_types, role_classes_by_id = self._collect_node_metadata()
        batch: List[Union[ChemicalEntity, Role, SourceEdge, HasRoleEdge]] = []
        term_count = 0

        for term_data in self._iter_term_blocks():
            term_count += 1
            if self._should_skip_term(term_data):
                if self.max_terms is not None and term_count >= self.max_terms:
                    break
                continue
            source_term = self._to_term(term_data)
            role_classes = role_classes_by_id.get(source_term.id)
            if role_classes:
                for role_cls in role_classes:
                    batch.append(role_cls(**source_term.__dict__))
            else:
                term_cls = node_types.get(source_term.id, Term)
                batch.append(term_cls(**source_term.__dict__))
            if len(batch) >= self.batch_size:
                yield batch
                batch = []

            if self.max_terms is not None and term_count >= self.max_terms:
                break

        if batch:
            yield batch

        relationship_batch: List[Union[SourceEdge, HasRoleEdge]] = []
        term_count = 0
        for term_data in self._iter_term_blocks():
            term_count += 1
            if self._should_skip_term(term_data):
                if self.max_terms is not None and term_count >= self.max_terms:
                    break
                continue
            for relationship in self._iter_relationships(term_data, node_types, role_classes_by_id):
                relationship_batch.append(relationship)
                if len(relationship_batch) >= self.batch_size:
                    yield relationship_batch
                    relationship_batch = []
            if self.max_terms is not None and term_count >= self.max_terms:
                break
        if relationship_batch:
            yield relationship_batch

    def _collect_node_metadata(self) -> tuple[dict[str, type[Term]], dict[str, list[type[Role]]]]:
        role_ids = set()
        chemical_entity_ids = set()
        is_a_parents: dict[str, set[str]] = {}
        relationship_edges: list[tuple[str, str, str]] = []
        term_ids = set()
        term_count = 0
        for term_data in self._iter_term_blocks():
            term_count += 1
            if self._should_skip_term(term_data):
                if self.max_terms is not None and term_count >= self.max_terms:
                    break
                continue
            source_id = self._required_first(term_data, "id")
            term_ids.add(source_id)
            is_a_parents[source_id] = {
                value.split(" ! ", 1)[0].strip()
                for value in term_data.get("is_a", [])
            }
            for parent_id in is_a_parents[source_id]:
                relationship_edges.append(("is_a", source_id, parent_id))
            for value in term_data.get("relationship", []):
                body, _ = self._split_comment(value)
                parts = body.split(maxsplit=2)
                if len(parts) >= 2 and parts[0] == CHEBI_ROLE_PREDICATE:
                    role_ids.add(parts[1])
                if len(parts) >= 2:
                    predicate, target_id = parts[0], parts[1]
                    relationship_edges.append((predicate, source_id, target_id))
                    if predicate in CHEBI_CHEMICAL_ENTITY_RELATIONSHIP_PREDICATES:
                        chemical_entity_ids.add(source_id)
                        chemical_entity_ids.add(target_id)
            for value in term_data.get("property_value", []):
                predicate = value.split(maxsplit=1)[0]
                if predicate in CHEBI_STRUCTURE_PROPERTY_IDS:
                    chemical_entity_ids.add(source_id)
                    break
            if self.max_terms is not None and term_count >= self.max_terms:
                break
        node_types: dict[str, type[Term]] = {}
        role_classes_by_id = {}
        for term_id in term_ids:
            role_classes = self._categorized_role_classes(term_id, is_a_parents)
            if role_classes:
                role_classes_by_id[term_id] = role_classes
        for term_id in role_ids:
            role_classes_by_id.setdefault(term_id, [Role])
        for term_id in term_ids:
            if self._has_any_ancestor(term_id, {CHEBI_ROLE_ROOT_ID}, is_a_parents):
                role_classes_by_id.setdefault(term_id, [Role])
        for term_id in role_ids:
            node_types[term_id] = role_classes_by_id[term_id][0]
        role_like_ids = set(role_classes_by_id)
        chemical_entity_ids.update(
            term_id
            for term_id in term_ids
            if self._has_any_ancestor(term_id, CHEBI_CHEMICAL_ENTITY_ROOT_IDS, is_a_parents)
        )
        chemical_entity_ids.difference_update(role_like_ids)
        chemical_entity_ids = self._expand_chemical_entity_ids(
            chemical_entity_ids,
            relationship_edges,
            role_like_ids,
        )
        for term_id in chemical_entity_ids:
            if term_id in term_ids and term_id not in node_types:
                node_types[term_id] = ChemicalEntity
        for term_id in term_ids:
            node_types.setdefault(term_id, ChemicalEntity)
        return node_types, role_classes_by_id

    @classmethod
    def _categorized_role_classes(cls, term_id: str, is_a_parents: dict[str, set[str]]) -> list[type[Role]]:
        role_classes = [
            node_cls
            for root_id, (node_cls, _) in CHEBI_ROLE_CLASSES.items()
            if cls._has_any_ancestor(term_id, {root_id}, is_a_parents)
        ]
        return role_classes

    @staticmethod
    def _has_any_ancestor(term_id: str, ancestor_ids: set[str], is_a_parents: dict[str, set[str]]) -> bool:
        if term_id in ancestor_ids:
            return True
        visited = set()
        stack = list(is_a_parents.get(term_id, set()))
        while stack:
            current = stack.pop()
            if current in ancestor_ids:
                return True
            if current in visited:
                continue
            visited.add(current)
            stack.extend(is_a_parents.get(current, set()))
        return False

    @staticmethod
    def _expand_chemical_entity_ids(
        chemical_entity_ids: set[str],
        relationship_edges: list[tuple[str, str, str]],
        excluded_ids: set[str],
    ) -> set[str]:
        expanded_ids = set(chemical_entity_ids)
        changed = True
        while changed:
            changed = False
            for predicate, source_id, target_id in relationship_edges:
                if predicate == "is_a":
                    if (
                        source_id in expanded_ids
                        and target_id not in excluded_ids
                        and target_id not in expanded_ids
                    ):
                        expanded_ids.add(target_id)
                        changed = True
                    continue
                if predicate not in CHEBI_CHEMICAL_ENTITY_ADJACENCY_PREDICATES:
                    continue
                source_is_chemical = source_id in expanded_ids
                target_is_chemical = target_id in expanded_ids
                if not source_is_chemical and not target_is_chemical:
                    continue
                for term_id in (source_id, target_id):
                    if term_id in excluded_ids or term_id in expanded_ids:
                        continue
                    expanded_ids.add(term_id)
                    changed = True
        return expanded_ids

    def _iter_term_blocks(self) -> Iterable[dict[str, list[str]]]:
        current: Optional[dict[str, list[str]]] = None
        with self._open_text(self.file_path) as handle:
            for raw_line in handle:
                line = raw_line.rstrip("\n")
                if line == "[Term]":
                    if current is not None:
                        yield current
                    current = {}
                    continue
                if line.startswith("["):
                    if current is not None:
                        yield current
                    current = None
                    continue
                if current is None or not line or ": " not in line:
                    continue
                tag, value = line.split(": ", 1)
                current.setdefault(tag, []).append(value)
        if current is not None:
            yield current

    @classmethod
    def _should_skip_term(cls, term_data: dict[str, list[str]]) -> bool:
        if cls._first(term_data.get("name")):
            return False
        if cls._first(term_data.get("def")):
            return False
        if term_data.get("synonym") or term_data.get("xref") or term_data.get("property_value"):
            return False
        if term_data.get("is_a") or term_data.get("relationship"):
            return False
        return True

    @staticmethod
    def _open_text(path: str):
        if path.endswith(".gz"):
            return gzip.open(path, "rt", encoding="utf-8", errors="replace")
        return Path(path).open("r", encoding="utf-8", errors="replace")

    @classmethod
    def _to_term(cls, term_data: dict[str, list[str]]) -> Term:
        properties = [cls._parse_property(value) for value in term_data.get("property_value", [])]
        property_values = cls._source_property_values(properties)
        synonyms = [cls._parse_synonym(value) for value in term_data.get("synonym", [])]
        xrefs = [cls._parse_xref(value) for value in term_data.get("xref", [])]
        definition, definition_references = cls._parse_definition(cls._first(term_data.get("def")))

        return Term(
            id=cls._required_first(term_data, "id"),
            name=cls._first(term_data.get("name")),
            definition=definition,
            definition_references=definition_references,
            subsets=list(term_data.get("subset", [])),
            alt_ids=list(term_data.get("alt_id", [])),
            synonyms=synonyms,
            xrefs=xrefs,
            properties=properties,
            is_obsolete=cls._first(term_data.get("is_obsolete")) == "true",
            synonym_text=" | ".join(s.value for s in synonyms) or None,
            xref_text=" | ".join(x.value for x in xrefs) or None,
            **property_values,
        )

    @classmethod
    def _to_typed_term(cls, term_data: dict[str, list[str]], node_types: dict[str, type[Term]]) -> Term:
        term = cls._to_term(term_data)
        term_cls = node_types.get(term.id, Term)
        return term_cls(**term.__dict__)

    @staticmethod
    def _first(values: Optional[list[str]]) -> Optional[str]:
        if not values:
            return None
        return values[0]

    @staticmethod
    def _required_first(term_data: dict[str, list[str]], tag: str) -> str:
        values = term_data.get(tag)
        if not values:
            raise ValueError(f"ChEBI term is missing required OBO tag: {tag}")
        return values[0]

    @classmethod
    def _source_property_values(cls, properties: list[Property]) -> dict[str, Optional[str]]:
        values = {field_name: None for field_name in CHEBI_PROPERTY_FIELD_MAP.values()}
        for prop in properties:
            field_name = CHEBI_PROPERTY_FIELD_MAP.get(prop.predicate)
            if field_name and values[field_name] is None:
                values[field_name] = prop.value
        return values

    @staticmethod
    def _split_comment(value: str) -> tuple[str, Optional[str]]:
        if " ! " not in value:
            return value.strip(), None
        left, comment = value.split(" ! ", 1)
        return left.strip(), comment.strip() or None

    @staticmethod
    def _target_label_from_relationship_comment(predicate: str, comment: Optional[str]) -> Optional[str]:
        if comment is None:
            return None
        label_prefix = CHEBI_PREDICATE_LABELS.get(predicate)
        if label_prefix and comment.startswith(f"{label_prefix} "):
            return comment[len(label_prefix) + 1 :].strip() or None
        return comment

    @classmethod
    def _iter_relationships(
        cls,
        term_data: dict[str, list[str]],
        node_types: dict[str, type[Term]],
        role_classes_by_id: dict[str, list[type[Role]]],
    ) -> Iterable[Union[SourceEdge, HasRoleEdge]]:
        source_id = cls._required_first(term_data, "id")
        source_label = cls._first(term_data.get("name"))
        for value in term_data.get("is_a", []):
            target_id, target_label = cls._split_comment(value)
            yield IsAEdge(
                start_node=cls._node_ref(source_id, node_types),
                end_node=cls._node_ref(target_id, node_types),
                source_predicate="is_a",
                source_label=source_label,
                target_label=target_label,
                raw=value,
            )

        for value in term_data.get("relationship", []):
            body, comment = cls._split_comment(value)
            parts = body.split(maxsplit=2)
            if len(parts) < 2:
                continue
            predicate, target_id = parts[0], parts[1]
            target_label = cls._target_label_from_relationship_comment(predicate, comment)
            if predicate == CHEBI_ROLE_PREDICATE:
                for role_cls, edge_cls in cls._role_targets(target_id, role_classes_by_id):
                    yield edge_cls(
                        start_node=cls._node_ref(source_id, node_types),
                        end_node=role_cls(id=target_id),
                        source_predicate=predicate,
                        source_label=source_label,
                        target_label=target_label,
                        raw=value,
                    )
                continue

            edge_cls = CHEBI_RELATIONSHIP_EDGE_CLASSES.get(predicate)
            if edge_cls is None:
                edge_cls = SourceEdge
            start_node = cls._node_ref(source_id, node_types)
            end_node = cls._node_ref(target_id, node_types)
            if edge_cls is IsSubstituentGroupFromEdge:
                start_node = ChemicalEntity(id=source_id)
            yield edge_cls(
                start_node=start_node,
                end_node=end_node,
                source_predicate=predicate,
                source_label=source_label,
                target_label=target_label,
                raw=value,
            )

    @staticmethod
    def _node_ref(term_id: str, node_types: dict[str, type[Term]]) -> Term:
        return node_types.get(term_id, Term)(id=term_id)

    @staticmethod
    def _role_targets(
        term_id: str,
        role_classes_by_id: dict[str, list[type[Role]]],
    ) -> list[tuple[type[Role], type[HasRoleEdge]]]:
        role_classes = role_classes_by_id.get(term_id, [Role])
        edge_classes_by_node_class = {
            Application: HasApplicationEdge,
            BiologicalRole: HasBiologicalRoleEdge,
            ChemicalRole: HasChemicalRoleEdge,
            Role: HasRoleEdge,
        }
        return [
            (role_cls, edge_classes_by_node_class.get(role_cls, HasRoleEdge))
            for role_cls in role_classes
        ]

    @staticmethod
    def _parse_quoted_value(value: str) -> tuple[Optional[str], str]:
        if not value.startswith('"'):
            return None, value.strip()
        escaped = False
        chars = []
        for index, char in enumerate(value[1:], start=1):
            if escaped:
                chars.append(char)
                escaped = False
                continue
            if char == "\\":
                escaped = True
                continue
            if char == '"':
                return "".join(chars), value[index + 1 :].strip()
            chars.append(char)
        return value.strip('"'), ""

    @classmethod
    def _parse_definition(cls, value: Optional[str]) -> tuple[Optional[str], list[str]]:
        if value is None:
            return None, []
        text, remainder = cls._parse_quoted_value(value)
        return text, cls._parse_references(remainder)

    @classmethod
    def _parse_synonym(cls, value: str) -> Synonym:
        text, remainder = cls._parse_quoted_value(value)
        references = cls._parse_references(remainder)
        before_refs = remainder.split("[", 1)[0].strip()
        parts = before_refs.split()
        scope = parts[0] if parts else None
        synonym_type = parts[1] if len(parts) > 1 else None
        return Synonym(
            value=text or value,
            scope=scope,
            type=synonym_type,
            references=references,
            raw=value,
        )

    @staticmethod
    def _parse_references(value: str) -> list[str]:
        match = re.search(r"\[([^\]]*)\]", value)
        if not match:
            return []
        return [entry.strip() for entry in match.group(1).split(",") if entry.strip()]

    @classmethod
    def _parse_xref(cls, value: str) -> Xref:
        body = value.split("{", 1)[0].strip()
        prefix = body.split(":", 1)[0] if ":" in body else None
        source = None
        source_match = re.search(r'source="([^"]+)"', value)
        if source_match:
            source = source_match.group(1)
        return Xref(value=body, prefix=prefix, source=source, raw=value)

    @classmethod
    def _parse_property(cls, value: str) -> Property:
        parts = value.split(maxsplit=1)
        predicate = parts[0]
        remainder = parts[1] if len(parts) > 1 else ""
        prop_value, after_value = cls._parse_quoted_value(remainder)
        if prop_value is None:
            raw_parts = remainder.split(maxsplit=1)
            prop_value = raw_parts[0] if raw_parts else ""
            after_value = raw_parts[1] if len(raw_parts) > 1 else ""
        datatype = after_value.strip() or None
        return Property(
            predicate=predicate,
            value=prop_value,
            datatype=datatype,
            raw=value,
        )
