from pathlib import Path
from typing import Dict, Generator, Iterable, List, Optional, Tuple, Union
import xml.etree.ElementTree as ET
import zipfile

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.metabolite_harmonization import (
    MetabolitePathwayDetail,
    MetabolitePathwayEdge,
    PathwayIdentifier,
    PathwayName,
    MetaboliteIdentifier,
)


HMDB_METABOLITES_XML_MEMBER = "hmdb_metabolites.xml"


class HmdbMetabolitePathwayContextAdapter(InputAdapter):
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
            self.version_info = DatasourceVersionInfo(version=None, version_date=None, download_date=None)
        if hmdb_zip_file is None:
            raise ValueError("HmdbMetabolitePathwayContextAdapter requires data_source or hmdb_zip_file")
        self.hmdb_zip_file = Path(hmdb_zip_file)
        self.max_records = max_records

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.HMDB

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def get_all(self) -> Generator[
        List[Union[PathwayIdentifier, MetabolitePathwayEdge]],
        None,
        None,
    ]:
        yield from self._iter_pathway_node_batches()
        yield from self._iter_edge_batches()

    def _iter_pathway_node_batches(self) -> Generator[List[PathwayIdentifier], None, None]:
        batch: List[PathwayIdentifier] = []
        emitted: set[str] = set()
        for record in self._iter_metabolite_records():
            for pathway in record["pathways"]:
                pathway_id = _pathway_id(pathway)
                if pathway_id is None or pathway_id in emitted:
                    continue
                emitted.add(pathway_id)
                batch.append(_pathway_node(pathway_id, pathway, source_field=pathway["source_field"]))
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []
        if batch:
            yield batch

    def _iter_edge_batches(self) -> Generator[List[MetabolitePathwayEdge], None, None]:
        batch: List[MetabolitePathwayEdge] = []
        emitted: set[Tuple[str, str, str]] = set()
        for record in self._iter_metabolite_records():
            metabolite_id = self._hmdb_metabolite_id(record["accession"])
            if metabolite_id is None:
                continue
            for pathway in record["pathways"]:
                pathway_id = _pathway_id(pathway)
                if pathway_id is None:
                    continue
                edge_key = (metabolite_id, pathway_id, pathway["source_field"])
                if edge_key in emitted:
                    continue
                emitted.add(edge_key)
                batch.append(
                    MetabolitePathwayEdge(
                        start_node=MetaboliteIdentifier(id=metabolite_id),
                        end_node=PathwayIdentifier(id=pathway_id),
                        details=[
                            MetabolitePathwayDetail(
                                source="HMDB",
                                source_field=pathway["source_field"],
                                metabolite_id=metabolite_id,
                                hmdb_metabolite_accession=record["accession"],
                                pathway_id=pathway_id,
                                pathway_name=pathway.get("name"),
                                pathway_category=pathway.get("category"),
                            )
                        ],
                    )
                )
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []
        if batch:
            yield batch

    def _iter_metabolite_records(self) -> Iterable[Dict]:
        count = 0
        with zipfile.ZipFile(self.hmdb_zip_file) as archive:
            with archive.open(HMDB_METABOLITES_XML_MEMBER) as handle:
                for _event, elem in ET.iterparse(handle, events=("end",)):
                    if self._local_name(elem.tag) != "metabolite":
                        continue
                    yield self._parse_metabolite(elem)
                    count += 1
                    elem.clear()
                    if self.max_records is not None and count >= self.max_records:
                        break

    @classmethod
    def _parse_metabolite(cls, elem) -> Dict:
        record = {
            "accession": None,
            "pathways": [],
        }
        for child in list(elem):
            tag = cls._local_name(child.tag)
            if tag == "accession":
                record["accession"] = cls._clean_text(child.text)
            elif tag == "pathways":
                record["pathways"].extend(
                    cls._parse_pathway_container(
                        child,
                        source_field="pathways",
                    )
                )
            elif tag == "biological_properties":
                pathways = cls._child(child, "pathways")
                if pathways is not None:
                    record["pathways"].extend(
                        cls._parse_pathway_container(
                            pathways,
                            source_field="biological_properties.pathways",
                        )
                    )
        return record

    @classmethod
    def _parse_pathway_container(cls, elem, *, source_field: str) -> List[Dict]:
        pathways = []
        seen = set()
        for pathway in list(elem):
            if cls._local_name(pathway.tag) != "pathway":
                continue
            name = cls._child_text(pathway, "name")
            smpdb_id = cls._child_text(pathway, "smpdb_id")
            kegg_map_id = cls._child_text(pathway, "kegg_map_id")
            for pathway_type, source_id, category in (
                ("smpdb", smpdb_id, "smpdb3"),
                ("kegg", kegg_map_id, "kegg"),
            ):
                if not source_id:
                    continue
                key = (pathway_type, source_id)
                if key in seen:
                    continue
                seen.add(key)
                pathways.append(
                    {
                        "type": pathway_type,
                        "source_id": source_id,
                        "name": name,
                        "category": category,
                        "source_field": source_field,
                    }
                )
        return pathways

    @classmethod
    def _hmdb_metabolite_id(cls, value: Optional[str]) -> Optional[str]:
        value = cls._clean_text(value)
        if value is None:
            return None
        if value.upper().startswith("HMDB:"):
            return f"HMDB:{value.split(':', 1)[1].strip()}"
        return f"HMDB:{value}"

    @classmethod
    def _child_text(cls, elem, child_tag: str) -> Optional[str]:
        child = cls._child(elem, child_tag)
        if child is None:
            return None
        return cls._clean_text(child.text)

    @classmethod
    def _child(cls, elem, child_tag: str):
        for child in list(elem):
            if cls._local_name(child.tag) == child_tag:
                return child
        return None

    @staticmethod
    def _clean_text(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = value.strip()
        return value or None

    @staticmethod
    def _local_name(tag: str) -> str:
        return tag.split("}", 1)[-1]


def _pathway_id(pathway: Dict) -> Optional[str]:
    source_id = HmdbMetabolitePathwayContextAdapter._clean_text(pathway.get("source_id"))
    if source_id is None:
        return None
    pathway_type = pathway.get("type")
    if pathway_type == "smpdb":
        return f"SMPDB:{source_id}"
    if pathway_type == "kegg":
        return f"KEGG.PATHWAY:{source_id}"
    return None


def _pathway_node(pathway_id: str, pathway: Dict, *, source_field: str) -> PathwayIdentifier:
    names = []
    if pathway.get("name"):
        names.append(
            PathwayName(
                value=pathway["name"],
                source="HMDB",
                source_field=source_field,
            )
        )
    return PathwayIdentifier(
        id=pathway_id,
        category=pathway.get("category"),
        names=names,
    )
