from pathlib import Path
from typing import Dict, Generator, Iterable, List, Optional, Set, Tuple, Union
import re
import zipfile

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.metabolite_harmonization import (
    MetaboliteIdentifier,
    MetaboliteIdentifierMappingDetail,
    MetaboliteIdentifierMappingEdge,
    MetaboliteName,
)


LIPIDMAPS_SDF_MEMBER = "structures.sdf"

ID_FIELD_PREFIXES = {
    "PUBCHEM_CID": "PUBCHEM.COMPOUND",
    "CHEBI_ID": "CHEBI",
    "HMDB_ID": "HMDB",
    "SWISSLIPIDS_ID": "SwissLipids",
    "LIPIDBANK_ID": "LipidBank",
    "KEGG_ID": "KEGG.COMPOUND",
    "PLANTFA_ID": "PlantFA",
}


class LipidMapsMetaboliteEquivalenceAdapter(InputAdapter):
    def __init__(
        self,
        data_source=None,
        sdf_zip_file: Optional[str] = None,
        max_records: Optional[int] = None,
    ):
        if data_source is not None:
            sdf_zip_file = str(data_source.file("LMSD.sdf.zip"))
            self.version_info = data_source.version_info()
        else:
            self.version_info = DatasourceVersionInfo(
                version=None,
                version_date=None,
                download_date=None,
            )
        if sdf_zip_file is None:
            raise ValueError("LipidMapsMetaboliteEquivalenceAdapter requires data_source or sdf_zip_file")
        self.sdf_zip_file = Path(sdf_zip_file)
        self.max_records = max_records

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.LipidMaps

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def get_all(self) -> Generator[
        List[Union[MetaboliteIdentifier, MetaboliteIdentifierMappingEdge]],
        None,
        None,
    ]:
        yield from self._iter_node_batches()
        yield from self._iter_edge_batches()

    def _iter_node_batches(self) -> Generator[List[MetaboliteIdentifier], None, None]:
        batch: List[MetaboliteIdentifier] = []
        emitted_ids: Set[str] = set()
        emitted_labeled_records: Set[Tuple[str, Tuple[str, ...], Tuple[str, ...]]] = set()

        for record in self._iter_metabolite_records():
            primary_id = self._lipidmaps_id(record["LM_ID"])
            if primary_id is None:
                continue

            name_key = tuple(name.value for name in self._names(record))
            synonym_key = tuple(synonym.value for synonym in self._synonyms(record))
            label_key = (primary_id, name_key, synonym_key)
            if label_key not in emitted_labeled_records:
                batch.append(self._primary_node(primary_id, record))
                emitted_labeled_records.add(label_key)
                emitted_ids.add(primary_id)

            for source_field, values in record["xrefs"].items():
                for value in values:
                    node_id = self._external_id(source_field, value)
                    if node_id is None or node_id == primary_id or node_id in emitted_ids:
                        continue
                    batch.append(MetaboliteIdentifier(id=node_id))
                    emitted_ids.add(node_id)
                    if len(batch) >= self.batch_size:
                        yield batch
                        batch = []

            if len(batch) >= self.batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

    def _iter_edge_batches(self) -> Generator[List[MetaboliteIdentifierMappingEdge], None, None]:
        batch: List[MetaboliteIdentifierMappingEdge] = []
        emitted_details: Set[Tuple[str, str, str, str, str]] = set()

        for record in self._iter_metabolite_records():
            primary_id = self._lipidmaps_id(record["LM_ID"])
            if primary_id is None:
                continue

            for source_field, values in record["xrefs"].items():
                for value in values:
                    end_id = self._external_id(source_field, value)
                    if end_id is None or end_id == primary_id:
                        continue
                    detail_key = (primary_id, end_id, "LipidMaps", source_field, primary_id)
                    if detail_key in emitted_details:
                        continue
                    emitted_details.add(detail_key)
                    batch.append(
                        MetaboliteIdentifierMappingEdge(
                            start_node=MetaboliteIdentifier(id=primary_id),
                            end_node=MetaboliteIdentifier(id=end_id),
                            details=[
                                MetaboliteIdentifierMappingDetail(
                                    source="LipidMaps",
                                    source_field=source_field,
                                    source_id=primary_id,
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
        with zipfile.ZipFile(self.sdf_zip_file) as archive:
            with archive.open(LIPIDMAPS_SDF_MEMBER) as handle:
                for record in self._iter_sdf_records(handle):
                    parsed = self._parse_sdf_record(record)
                    if parsed.get("LM_ID"):
                        yield parsed
                        count += 1
                        if self.max_records is not None and count >= self.max_records:
                            return

    @classmethod
    def _parse_sdf_record(cls, lines: List[str]) -> Dict:
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
        return {
            "LM_ID": values.get("LM_ID"),
            "NAME": values.get("NAME"),
            "SYSTEMATIC_NAME": values.get("SYSTEMATIC_NAME"),
            "ABBREVIATION": values.get("ABBREVIATION"),
            "SYNONYMS": values.get("SYNONYMS"),
            "xrefs": {
                field: cls._unique_clean_values([values.get(field)])
                for field in ID_FIELD_PREFIXES
            },
        }

    @staticmethod
    def _iter_sdf_records(handle) -> Iterable[List[str]]:
        record = []
        for raw_line in handle:
            line = raw_line.decode("utf-8", "replace").rstrip("\r\n")
            if line == "$$$$":
                yield record
                record = []
            else:
                record.append(line)
        if record:
            yield record

    @classmethod
    def _primary_node(cls, primary_id: str, record: Dict) -> MetaboliteIdentifier:
        return MetaboliteIdentifier(
            id=primary_id,
            names=cls._names(record),
            synonyms=cls._synonyms(record),
        )

    @staticmethod
    def _names(record: Dict) -> List[MetaboliteName]:
        names = []
        seen = set()
        for source_field in ["NAME", "SYSTEMATIC_NAME", "ABBREVIATION"]:
            value = LipidMapsMetaboliteEquivalenceAdapter._clean_text(record.get(source_field))
            if not value or value in seen:
                continue
            seen.add(value)
            names.append(
                MetaboliteName(
                    value=value,
                    source="LipidMaps",
                    source_field=source_field,
                )
            )
        return names

    @staticmethod
    def _synonyms(record: Dict) -> List[MetaboliteName]:
        values = record.get("SYNONYMS")
        if not values:
            return []
        synonyms = []
        seen = set()
        for value in re.split(r";", values):
            value = LipidMapsMetaboliteEquivalenceAdapter._clean_text(value)
            if not value or value in seen:
                continue
            seen.add(value)
            synonyms.append(
                MetaboliteName(
                    value=value,
                    source="LipidMaps",
                    source_field="SYNONYMS",
                )
            )
        return synonyms

    @classmethod
    def _external_id(cls, source_field: str, value: Optional[str]) -> Optional[str]:
        prefix = ID_FIELD_PREFIXES.get(source_field)
        value = cls._clean_text(value)
        if not prefix or not value:
            return None
        if prefix == "CHEBI" and value.upper().startswith("CHEBI:"):
            value = value.split(":", 1)[1].strip()
        if prefix == "PUBCHEM.COMPOUND":
            value = re.sub(r"^CID", "", value, flags=re.IGNORECASE)
        return f"{prefix}:{value}"

    @classmethod
    def _lipidmaps_id(cls, value: Optional[str]) -> Optional[str]:
        value = cls._clean_text(value)
        if not value:
            return None
        if value.upper().startswith("LIPIDMAPS:"):
            return f"LIPIDMAPS:{value.split(':', 1)[1].strip()}"
        return f"LIPIDMAPS:{value}"

    @staticmethod
    def _clean_text(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = value.strip()
        return value or None

    @classmethod
    def _unique_clean_values(cls, values: Iterable[Optional[str]]) -> List[str]:
        result = []
        seen = set()
        for value in values:
            value = cls._clean_text(value)
            if not value or value in seen:
                continue
            seen.add(value)
            result.append(value)
        return result
