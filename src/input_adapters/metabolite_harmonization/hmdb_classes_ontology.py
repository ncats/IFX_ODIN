from datetime import date
from pathlib import Path
from typing import Dict, Generator, Iterable, List, Optional, Set, Tuple, Union
import re
import xml.etree.ElementTree as ET
import zipfile

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.metabolite_harmonization import (
    HmdbMetaboliteOntologyEdge,
    HmdbOntologyMembershipDetail,
    HmdbOntologyParentEdge,
    HmdbOntologyTerm,
    MetaboliteClassificationDetail,
    MetaboliteClassificationEdge,
    MetaboliteClassificationParentEdge,
    MetaboliteClassificationTerm,
    MetaboliteIdentifier,
)


HMDB_XML_MEMBER = "hmdb_metabolites.xml"
HMDB_VERSION = "5.0"
HMDB_VERSION_DATE = date(2021, 11, 17)

HMDB_CLASS_FIELDS = [
    ("ClassyFire_super_class", "taxonomy.super_class", "super_class"),
    ("ClassyFire_class", "taxonomy.class", "class"),
    ("ClassyFire_sub_class", "taxonomy.sub_class", "sub_class"),
]

HMDB_CLASS_PARENT_PAIRS = [
    ("ClassyFire_super_class", "ClassyFire_class"),
    ("ClassyFire_class", "ClassyFire_sub_class"),
]

HMDB_ONTOLOGY_CATEGORY_TERMS = {
    "Biofluid and excreta",
    "Health condition",
    "Industrial application",
    "Organ and components",
    "Source",
    "Subcellular",
    "Tissue and substructures",
}


class _HmdbMetabolitesXmlAdapter(InputAdapter):
    def __init__(
        self,
        data_source=None,
        hmdb_zip_file: Optional[str] = None,
        max_records: Optional[int] = None,
    ):
        if data_source is not None:
            hmdb_zip_file = str(data_source.file("hmdb_metabolites.zip"))
            self.version_info = data_source.version_info()
        else:
            self.version_info = DatasourceVersionInfo(
                version=HMDB_VERSION,
                version_date=HMDB_VERSION_DATE,
                download_date=None,
            )
        if hmdb_zip_file is None:
            raise ValueError(f"{type(self).__name__} requires data_source or hmdb_zip_file")
        self.hmdb_zip_file = Path(hmdb_zip_file)
        self.max_records = max_records

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.HMDB

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def _iter_metabolite_elements(self) -> Iterable[ET.Element]:
        with zipfile.ZipFile(self.hmdb_zip_file) as archive:
            with archive.open(HMDB_XML_MEMBER) as handle:
                count = 0
                for _event, elem in ET.iterparse(handle, events=("end",)):
                    if self._local_name(elem.tag) != "metabolite":
                        continue
                    yield elem
                    count += 1
                    elem.clear()
                    if self.max_records is not None and count >= self.max_records:
                        break

    @staticmethod
    def _clean_text(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = value.strip()
        return value or None

    @classmethod
    def _hmdb_id(cls, value: Optional[str]) -> Optional[str]:
        value = cls._clean_text(value)
        if not value:
            return None
        if value.upper().startswith("HMDB:"):
            return f"HMDB:{value.split(':', 1)[1].strip()}"
        return f"HMDB:{value}"

    @staticmethod
    def _local_name(tag: str) -> str:
        return tag.split("}", 1)[-1]


def _stable_token(value: str) -> str:
    value = re.sub(r"\s+", " ", value.strip())
    value = value.replace("/", "_slash_")
    return value


def _class_id(source: str, level_name: str, class_name: str) -> str:
    return f"{source}.CLASS:{level_name}:{_stable_token(class_name)}"


class HmdbMetaboliteClassificationAdapter(_HmdbMetabolitesXmlAdapter):
    _class_terms: Optional[Dict[str, MetaboliteClassificationTerm]] = None
    _class_parent_edges: Optional[Set[Tuple[str, str]]] = None

    def get_all(self) -> Generator[
        List[
            Union[
                MetaboliteClassificationTerm,
                MetaboliteClassificationEdge,
                MetaboliteClassificationParentEdge,
            ]
        ],
        None,
        None,
    ]:
        yield from self._iter_term_batches()
        yield from self._iter_parent_edge_batches()
        yield from self._iter_membership_edge_batches()

    def _iter_term_batches(self) -> Generator[List[MetaboliteClassificationTerm], None, None]:
        batch: List[MetaboliteClassificationTerm] = []
        terms, _parent_edges = self._load_class_terms_and_parents()
        for term in terms.values():
            batch.append(term)
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _iter_parent_edge_batches(self) -> Generator[List[MetaboliteClassificationParentEdge], None, None]:
        batch: List[MetaboliteClassificationParentEdge] = []
        _terms, parent_edges = self._load_class_terms_and_parents()
        for parent_id, child_id in parent_edges:
            batch.append(
                MetaboliteClassificationParentEdge(
                    start_node=MetaboliteClassificationTerm(id=parent_id),
                    end_node=MetaboliteClassificationTerm(id=child_id),
                    source="HMDB",
                    source_field="taxonomy",
                )
            )
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _iter_membership_edge_batches(self) -> Generator[List[MetaboliteClassificationEdge], None, None]:
        batch: List[MetaboliteClassificationEdge] = []
        emitted: Set[Tuple[str, str, str]] = set()
        for record in self._iter_class_records():
            metabolite_id = record["metabolite_id"]
            leaf_class = self._most_specific_class(record["classes"])
            if leaf_class is None:
                continue
            level_name, class_name = leaf_class
            term_id = _class_id("HMDB", level_name, class_name)
            edge_key = (metabolite_id, term_id, level_name)
            if edge_key in emitted:
                continue
            emitted.add(edge_key)
            batch.append(
                MetaboliteClassificationEdge(
                    start_node=MetaboliteIdentifier(id=metabolite_id),
                    end_node=MetaboliteClassificationTerm(id=term_id),
                    details=[
                        MetaboliteClassificationDetail(
                            source="HMDB",
                            source_field=f"taxonomy.{level_name.removeprefix('ClassyFire_')}",
                            source_id=metabolite_id,
                            level_name=level_name,
                            class_name=class_name,
                        )
                    ],
                )
            )
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    @staticmethod
    def _most_specific_class(classes: Dict[str, str]) -> Optional[Tuple[str, str]]:
        for level_name, _source_field, _xml_tag in reversed(HMDB_CLASS_FIELDS):
            class_name = classes.get(level_name)
            if class_name:
                return level_name, class_name
        return None

    def _iter_class_records(self) -> Iterable[Dict]:
        for elem in self._iter_metabolite_elements():
            record = self._parse_class_record(elem)
            if record["metabolite_id"] and record["classes"]:
                yield record

    def _load_class_terms_and_parents(
        self,
    ) -> Tuple[Dict[str, MetaboliteClassificationTerm], Set[Tuple[str, str]]]:
        if self._class_terms is not None and self._class_parent_edges is not None:
            return self._class_terms, self._class_parent_edges

        terms: Dict[str, MetaboliteClassificationTerm] = {}
        parent_edges: Set[Tuple[str, str]] = set()
        for record in self._iter_class_records():
            for level_name, class_name in record["classes"].items():
                term_id = _class_id("HMDB", level_name, class_name)
                terms.setdefault(
                    term_id,
                    MetaboliteClassificationTerm(
                        id=term_id,
                        source="HMDB",
                        level_name=level_name,
                        name=class_name,
                    ),
                )
            for parent_level, child_level in HMDB_CLASS_PARENT_PAIRS:
                parent_name = record["classes"].get(parent_level)
                child_name = record["classes"].get(child_level)
                if not parent_name or not child_name:
                    continue
                parent_edges.add((
                    _class_id("HMDB", parent_level, parent_name),
                    _class_id("HMDB", child_level, child_name),
                ))

        self._class_terms = terms
        self._class_parent_edges = parent_edges
        return terms, parent_edges

    @classmethod
    def _parse_class_record(cls, elem: ET.Element) -> Dict:
        accession = None
        classes = {}
        for child in list(elem):
            tag = cls._local_name(child.tag)
            if tag == "accession":
                accession = cls._hmdb_id(child.text)
            elif tag == "taxonomy":
                by_tag = {
                    cls._local_name(tax_child.tag): cls._clean_text(tax_child.text)
                    for tax_child in list(child)
                }
                for level_name, _source_field, xml_tag in HMDB_CLASS_FIELDS:
                    value = by_tag.get(xml_tag)
                    if value:
                        classes[level_name] = value
        return {"metabolite_id": accession, "classes": classes}


def _ontology_id(ontology_type: str, term_name: str) -> str:
    return f"HMDB.ONTOLOGY:{_stable_token(ontology_type)}:{_stable_token(term_name)}"


class HmdbOntologyAdapter(_HmdbMetabolitesXmlAdapter):
    _ontology_terms: Optional[Dict[str, Dict]] = None
    _ontology_parent_edges: Optional[Set[Tuple[str, str]]] = None

    def get_all(self) -> Generator[
        List[Union[HmdbOntologyTerm, HmdbOntologyParentEdge, HmdbMetaboliteOntologyEdge]],
        None,
        None,
    ]:
        yield from self._iter_term_batches()
        yield from self._iter_parent_edge_batches()
        yield from self._iter_membership_edge_batches()

    def _iter_term_batches(self) -> Generator[List[HmdbOntologyTerm], None, None]:
        batch: List[HmdbOntologyTerm] = []
        terms, _parent_edges = self._load_ontology_terms_and_parents()
        for term in terms.values():
            batch.append(
                HmdbOntologyTerm(
                    id=term["id"],
                    name=term["name"],
                    definition=term.get("definition"),
                    ontology_type=term["ontology_type"],
                    term_type=term.get("term_type"),
                    level=term.get("level"),
                    source_parent_id=term.get("source_parent_id"),
                    synonyms=term.get("synonyms", []),
                )
            )
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _iter_parent_edge_batches(self) -> Generator[List[HmdbOntologyParentEdge], None, None]:
        batch: List[HmdbOntologyParentEdge] = []
        _terms, parent_edges = self._load_ontology_terms_and_parents()
        for parent_id, child_id in parent_edges:
            batch.append(
                HmdbOntologyParentEdge(
                    start_node=HmdbOntologyTerm(id=parent_id),
                    end_node=HmdbOntologyTerm(id=child_id),
                    source_field="ontology",
                )
            )
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _iter_membership_edge_batches(self) -> Generator[List[HmdbMetaboliteOntologyEdge], None, None]:
        batch: List[HmdbMetaboliteOntologyEdge] = []
        emitted: Set[Tuple[str, str]] = set()
        for record in self._iter_ontology_records():
            metabolite_id = record["metabolite_id"]
            if not metabolite_id:
                continue
            for term_id in record["member_term_ids"]:
                term = record["terms"][term_id]
                edge_key = (metabolite_id, term_id)
                if edge_key in emitted:
                    continue
                emitted.add(edge_key)
                batch.append(
                    HmdbMetaboliteOntologyEdge(
                        start_node=MetaboliteIdentifier(id=metabolite_id),
                        end_node=HmdbOntologyTerm(id=term_id),
                        details=[
                            HmdbOntologyMembershipDetail(
                                source="HMDB",
                                source_field="ontology",
                                source_id=metabolite_id,
                                ontology_term_id=term_id,
                                ontology_type=term["ontology_type"],
                                term_name=term["name"],
                            )
                        ],
                    )
                )
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []
        if batch:
            yield batch

    def _iter_ontology_records(self) -> Iterable[Dict]:
        for elem in self._iter_metabolite_elements():
            record = self._parse_ontology_record(elem)
            if record["terms"]:
                yield record

    def _load_ontology_terms_and_parents(self) -> Tuple[Dict[str, Dict], Set[Tuple[str, str]]]:
        if self._ontology_terms is not None and self._ontology_parent_edges is not None:
            return self._ontology_terms, self._ontology_parent_edges

        terms: Dict[str, Dict] = {}
        parent_edges: Set[Tuple[str, str]] = set()
        for record in self._iter_ontology_records():
            for term_id, term in record["terms"].items():
                existing = terms.get(term_id)
                if existing is None:
                    terms[term_id] = {
                        **term,
                        "synonyms": list(term.get("synonyms", [])),
                    }
                    continue
                existing["synonyms"] = sorted(set(existing.get("synonyms", [])) | set(term.get("synonyms", [])))
                for key, value in term.items():
                    if existing.get(key) in (None, [], "") and value not in (None, [], ""):
                        existing[key] = value
            parent_edges.update(record["parent_edges"])

        self._ontology_terms = terms
        self._ontology_parent_edges = parent_edges
        return terms, parent_edges

    @classmethod
    def _parse_ontology_record(cls, elem: ET.Element) -> Dict:
        accession = None
        ontology_elem = None
        for child in list(elem):
            tag = cls._local_name(child.tag)
            if tag == "accession":
                accession = cls._hmdb_id(child.text)
            elif tag == "ontology":
                ontology_elem = child

        terms: Dict[str, Dict] = {}
        parent_edges: Set[Tuple[str, str]] = set()
        member_term_ids: Set[str] = set()
        if ontology_elem is not None:
            for root in list(ontology_elem):
                if cls._local_name(root.tag) != "root":
                    continue
                cls._collect_ontology_subtree(
                    root,
                    current_type=None,
                    parent_term_id=None,
                    terms=terms,
                    parent_edges=parent_edges,
                    member_term_ids=member_term_ids,
                )
        return {
            "metabolite_id": accession,
            "terms": terms,
            "parent_edges": parent_edges,
            "member_term_ids": member_term_ids,
        }

    @classmethod
    def _collect_ontology_subtree(
        cls,
        elem: ET.Element,
        *,
        current_type: Optional[str],
        parent_term_id: Optional[str],
        terms: Dict[str, Dict],
        parent_edges: Set[Tuple[str, str]],
        member_term_ids: Set[str],
    ) -> None:
        term_name = cls._child_text(elem, "term")
        if not term_name:
            return
        level = cls._child_int(elem, "level")
        term_type = cls._child_text(elem, "type")
        is_anchor = cls._is_ontology_type_anchor(term_name, level)
        ontology_type = cls._ontology_type_for_term(term_name, current_type, level)
        term_id = None
        if ontology_type:
            term_id = _ontology_id(ontology_type, term_name)
            synonyms = cls._child_values(elem, "synonyms", "synonym")
            incoming = {
                "id": term_id,
                "name": term_name,
                "definition": cls._child_text(elem, "definition"),
                "ontology_type": ontology_type,
                "term_type": term_type,
                "level": level,
                "source_parent_id": cls._child_text(elem, "parent_id"),
                "synonyms": synonyms,
            }
            existing = terms.get(term_id)
            if existing is None:
                terms[term_id] = incoming
            else:
                existing["synonyms"] = sorted(set(existing.get("synonyms", [])) | set(synonyms))
                for key, value in incoming.items():
                    if existing.get(key) in (None, [], "") and value not in (None, [], ""):
                        existing[key] = value

            if parent_term_id:
                parent_edges.add((parent_term_id, term_id))
            if term_type == "child":
                member_term_ids.add(term_id)

        descendants = cls._first_child(elem, "descendants")
        if descendants is None:
            return
        next_type = ontology_type if is_anchor else current_type
        next_parent_term_id = term_id if term_id is not None else parent_term_id
        for descendant in list(descendants):
            if cls._local_name(descendant.tag) != "descendant":
                continue
            cls._collect_ontology_subtree(
                descendant,
                current_type=next_type,
                parent_term_id=next_parent_term_id,
                terms=terms,
                parent_edges=parent_edges,
                member_term_ids=member_term_ids,
            )

    @staticmethod
    def _ontology_type_for_term(term_name: str, current_type: Optional[str], level: Optional[int]) -> Optional[str]:
        if HmdbOntologyAdapter._is_ontology_type_anchor(term_name, level):
            return term_name
        return current_type

    @staticmethod
    def _is_ontology_type_anchor(term_name: str, level: Optional[int]) -> bool:
        return term_name in HMDB_ONTOLOGY_CATEGORY_TERMS

    @classmethod
    def _first_child(cls, elem: ET.Element, child_tag: str) -> Optional[ET.Element]:
        for child in list(elem):
            if cls._local_name(child.tag) == child_tag:
                return child
        return None

    @classmethod
    def _child_text(cls, elem: ET.Element, child_tag: str) -> Optional[str]:
        child = cls._first_child(elem, child_tag)
        if child is None:
            return None
        return cls._clean_text(child.text)

    @classmethod
    def _child_int(cls, elem: ET.Element, child_tag: str) -> Optional[int]:
        value = cls._child_text(elem, child_tag)
        if value is None:
            return None
        try:
            return int(value)
        except ValueError:
            return None

    @classmethod
    def _child_values(cls, elem: ET.Element, container_tag: str, item_tag: str) -> List[str]:
        container = cls._first_child(elem, container_tag)
        if container is None:
            return []
        values = []
        seen = set()
        for child in list(container):
            if cls._local_name(child.tag) != item_tag:
                continue
            value = cls._clean_text(child.text)
            if not value or value in seen:
                continue
            seen.add(value)
            values.append(value)
        return values
