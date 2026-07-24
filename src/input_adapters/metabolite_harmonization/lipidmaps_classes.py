from typing import Dict, Generator, Iterable, List, Optional, Set, Tuple, Union
import zipfile

from src.constants import DataSourceName
from src.input_adapters.metabolite_harmonization.lipidmaps import LipidMapsMetaboliteEquivalenceAdapter
from src.models.metabolite_harmonization import (
    MetaboliteClassificationDetail,
    MetaboliteClassificationEdge,
    MetaboliteClassificationParentEdge,
    MetaboliteClassificationTerm,
    MetaboliteIdentifier,
)


LIPIDMAPS_CLASS_FIELDS = [
    ("LipidMaps_category", "CATEGORY"),
    ("LipidMaps_main_class", "MAIN_CLASS"),
    ("LipidMaps_sub_class", "SUB_CLASS"),
    ("LipidMaps_class_level4", "CLASS_LEVEL4"),
]

LIPIDMAPS_CLASS_PARENT_PAIRS = [
    ("LipidMaps_category", "LipidMaps_main_class"),
    ("LipidMaps_main_class", "LipidMaps_sub_class"),
    ("LipidMaps_sub_class", "LipidMaps_class_level4"),
]


def _stable_token(value: str) -> str:
    return " ".join(value.strip().split()).replace("/", "_slash_")


def _class_id(source: str, level_name: str, class_name: str) -> str:
    return f"{source}.CLASS:{level_name}:{_stable_token(class_name)}"


class LipidMapsMetaboliteClassificationAdapter(LipidMapsMetaboliteEquivalenceAdapter):
    _class_terms: Optional[Dict[str, MetaboliteClassificationTerm]] = None
    _class_parent_edges: Optional[Set[Tuple[str, str]]] = None

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.LipidMaps

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
                    source="LipidMaps",
                    source_field="LMSD.sdf",
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
            term_id = _class_id("LipidMaps", level_name, class_name)
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
                            source="LipidMaps",
                            source_field=self._source_field_for_level(level_name),
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
        for level_name, _source_field in reversed(LIPIDMAPS_CLASS_FIELDS):
            class_name = classes.get(level_name)
            if class_name:
                return level_name, class_name
        return None

    def _iter_class_records(self) -> Iterable[Dict]:
        count = 0
        with zipfile.ZipFile(self.sdf_zip_file) as archive:
            with archive.open("structures.sdf") as handle:
                for record in self._iter_sdf_records(handle):
                    parsed = self._parse_class_record(record)
                    if parsed["metabolite_id"] and parsed["classes"]:
                        yield parsed
                        count += 1
                        if self.max_records is not None and count >= self.max_records:
                            return

    def _load_class_terms_and_parents(
        self,
    ) -> Tuple[Dict[str, MetaboliteClassificationTerm], Set[Tuple[str, str]]]:
        if self._class_terms is not None and self._class_parent_edges is not None:
            return self._class_terms, self._class_parent_edges

        terms: Dict[str, MetaboliteClassificationTerm] = {}
        parent_edges: Set[Tuple[str, str]] = set()
        for record in self._iter_class_records():
            for level_name, class_name in record["classes"].items():
                term_id = _class_id("LipidMaps", level_name, class_name)
                terms.setdefault(
                    term_id,
                    MetaboliteClassificationTerm(
                        id=term_id,
                        source="LipidMaps",
                        level_name=level_name,
                        name=class_name,
                        source_id=self._source_id_from_class_name(class_name),
                    ),
                )
            for parent_level, child_level in LIPIDMAPS_CLASS_PARENT_PAIRS:
                parent_name = record["classes"].get(parent_level)
                child_name = record["classes"].get(child_level)
                if not parent_name or not child_name:
                    continue
                parent_edges.add((
                    _class_id("LipidMaps", parent_level, parent_name),
                    _class_id("LipidMaps", child_level, child_name),
                ))

        self._class_terms = terms
        self._class_parent_edges = parent_edges
        return terms, parent_edges

    @classmethod
    def _parse_class_record(cls, lines: List[str]) -> Dict:
        tags: Dict[str, List[str]] = {}
        current_tag = None
        for line in lines:
            if line.startswith("> <") and line.endswith(">"):
                current_tag = line[3:-1]
                tags.setdefault(current_tag, [])
                continue
            if current_tag is not None:
                tags[current_tag].append(line)

        values = {
            tag: cls._clean_text("\n".join(parts))
            for tag, parts in tags.items()
        }
        metabolite_id = cls._lipidmaps_id(values.get("LM_ID"))
        classes = {}
        for level_name, source_field in LIPIDMAPS_CLASS_FIELDS:
            value = values.get(source_field)
            if value:
                classes[level_name] = value
        return {"metabolite_id": metabolite_id, "classes": classes}

    @staticmethod
    def _source_field_for_level(level_name: str) -> str:
        for configured_level, source_field in LIPIDMAPS_CLASS_FIELDS:
            if configured_level == level_name:
                return source_field
        return level_name

    @staticmethod
    def _source_id_from_class_name(class_name: str) -> Optional[str]:
        if "[" not in class_name or "]" not in class_name:
            return None
        return class_name.rsplit("[", 1)[1].split("]", 1)[0].strip() or None
