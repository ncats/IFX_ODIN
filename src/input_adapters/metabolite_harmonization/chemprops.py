from pathlib import Path
from typing import Dict, Generator, Iterable, List, Optional
import csv
import gzip
import zipfile

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.metabolite_harmonization import MetaboliteIdentifier, MetaboliteChemProps


HMDB_STRUCTURES_SDF_MEMBER = "structures.sdf"
LIPIDMAPS_SDF_MEMBER = "structures.sdf"


class _ChemPropsAdapter(InputAdapter):
    def get_all(self) -> Generator[List[MetaboliteIdentifier], None, None]:
        batch: List[MetaboliteIdentifier] = []
        for node in self._iter_nodes():
            batch.append(node)
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _iter_nodes(self) -> Iterable[MetaboliteIdentifier]:
        raise NotImplementedError


class HmdbMetaboliteChemPropsAdapter(_ChemPropsAdapter):
    def __init__(
        self,
        data_source=None,
        structures_zip_file: Optional[str] = None,
        max_records: Optional[int] = None,
    ):
        if data_source is not None:
            structures_zip_file = str(data_source.file("structures.zip"))
            self.version_info = data_source.version_info()
        else:
            self.version_info = DatasourceVersionInfo(version=None, version_date=None, download_date=None)
        if structures_zip_file is None:
            raise ValueError("HmdbMetaboliteChemPropsAdapter requires data_source or structures_zip_file")
        self.structures_zip_file = Path(structures_zip_file)
        self.max_records = max_records

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.HMDB

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def _iter_nodes(self) -> Iterable[MetaboliteIdentifier]:
        count = 0
        with zipfile.ZipFile(self.structures_zip_file) as archive:
            with archive.open(HMDB_STRUCTURES_SDF_MEMBER) as handle:
                for record in _iter_sdf_tag_records(handle):
                    source_id = _prefixed_id("HMDB", record.get("DATABASE_ID"))
                    if source_id is None:
                        continue
                    inchi_key = _clean_text(record.get("INCHI_KEY"))
                    yield MetaboliteIdentifier(
                        id=source_id,
                        chem_props=[
                            MetaboliteChemProps(
                                source="HMDB",
                                source_id=source_id,
                                iso_smiles=_clean_text(record.get("SMILES")),
                                inchi_key_prefix=_inchi_key_prefix(inchi_key),
                                inchi_key=inchi_key,
                                inchi=_clean_text(record.get("INCHI_IDENTIFIER")),
                                mw=_clean_text(record.get("MOLECULAR_WEIGHT")),
                                monoisotopic_mass=_clean_text(record.get("EXACT_MASS")),
                                common_name=_clean_text(record.get("GENERIC_NAME")),
                                molecular_formula=_clean_text(record.get("FORMULA")),
                            )
                        ],
                    )
                    count += 1
                    if self.max_records is not None and count >= self.max_records:
                        return


class ChebiMetaboliteChemPropsAdapter(_ChemPropsAdapter):
    def __init__(
        self,
        data_source=None,
        chebi_sdf_file: Optional[str] = None,
        max_records: Optional[int] = None,
    ):
        if data_source is not None:
            chebi_sdf_file = str(data_source.file())
            self.version_info = data_source.version_info()
        else:
            self.version_info = DatasourceVersionInfo(version=None, version_date=None, download_date=None)
        if chebi_sdf_file is None:
            raise ValueError("ChebiMetaboliteChemPropsAdapter requires data_source or chebi_sdf_file")
        self.chebi_sdf_file = Path(chebi_sdf_file)
        self.max_records = max_records

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.ChEBI

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def _iter_nodes(self) -> Iterable[MetaboliteIdentifier]:
        count = 0
        with _open_text_or_gzip(self.chebi_sdf_file) as handle:
            for record in _iter_sdf_tag_records(handle):
                source_id = _prefixed_id("CHEBI", record.get("ChEBI ID"))
                if source_id is None:
                    continue
                inchi_key = _clean_text(record.get("InChIKey"))
                yield MetaboliteIdentifier(
                    id=source_id,
                    chem_props=[
                        MetaboliteChemProps(
                            source="ChEBI",
                            source_id=source_id,
                            iso_smiles=_clean_text(record.get("SMILES")),
                            inchi_key_prefix=_inchi_key_prefix(inchi_key),
                            inchi_key=inchi_key,
                            inchi=_clean_text(record.get("InChI")),
                            mw=_clean_text(record.get("MASS")),
                            monoisotopic_mass=_clean_text(record.get("Monoisotopic Mass")),
                            common_name=_clean_text(record.get("ChEBI Name")),
                            molecular_formula=_clean_text(record.get("Formulae")),
                        )
                    ],
                )
                count += 1
                if self.max_records is not None and count >= self.max_records:
                    return


class LipidMapsMetaboliteChemPropsAdapter(_ChemPropsAdapter):
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
            self.version_info = DatasourceVersionInfo(version=None, version_date=None, download_date=None)
        if sdf_zip_file is None:
            raise ValueError("LipidMapsMetaboliteChemPropsAdapter requires data_source or sdf_zip_file")
        self.sdf_zip_file = Path(sdf_zip_file)
        self.max_records = max_records

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.LipidMaps

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def _iter_nodes(self) -> Iterable[MetaboliteIdentifier]:
        count = 0
        with zipfile.ZipFile(self.sdf_zip_file) as archive:
            with archive.open(LIPIDMAPS_SDF_MEMBER) as handle:
                for record in _iter_sdf_tag_records(handle):
                    source_id = _prefixed_id("LIPIDMAPS", record.get("LM_ID"))
                    if source_id is None:
                        continue
                    inchi_key = _clean_text(record.get("INCHI_KEY"))
                    yield MetaboliteIdentifier(
                        id=source_id,
                        chem_props=[
                            MetaboliteChemProps(
                                source="LipidMaps",
                                source_id=source_id,
                                iso_smiles=_clean_text(record.get("SMILES")),
                                inchi_key_prefix=_inchi_key_prefix(inchi_key),
                                inchi_key=inchi_key,
                                inchi=_clean_text(record.get("INCHI")),
                                mw=_clean_text(record.get("MASS")),
                                monoisotopic_mass=_clean_text(record.get("EXACT_MASS")),
                                common_name=_first_clean(
                                    record.get("COMMON_NAME"),
                                    record.get("NAME"),
                                    record.get("SYSTEMATIC_NAME"),
                                    record.get("ABBREVIATION"),
                                ),
                                molecular_formula=_clean_text(record.get("FORMULA")),
                            )
                        ],
                    )
                    count += 1
                    if self.max_records is not None and count >= self.max_records:
                        return


class PubchemMetaboliteChemPropsAdapter(_ChemPropsAdapter):
    def __init__(
        self,
        data_source=None,
        molecular_info_file: Optional[str] = None,
        max_records: Optional[int] = None,
    ):
        if data_source is not None:
            molecular_info_file = str(data_source.file("cid_molecular_info.tsv"))
            self.version_info = data_source.version_info()
        else:
            self.version_info = DatasourceVersionInfo(version=None, version_date=None, download_date=None)
        if molecular_info_file is None:
            raise ValueError("PubchemMetaboliteChemPropsAdapter requires data_source or molecular_info_file")
        self.molecular_info_file = Path(molecular_info_file)
        self.max_records = max_records

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.PubChem

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def _iter_nodes(self) -> Iterable[MetaboliteIdentifier]:
        count = 0
        with self.molecular_info_file.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            for row in reader:
                source_id = _clean_text(row.get("pubchem_id"))
                if source_id is None:
                    continue
                isomeric_smiles = _clean_text(row.get("isomeric_smiles"))
                yield MetaboliteIdentifier(
                    id=source_id,
                    chem_props=[
                        MetaboliteChemProps(
                            source="PubChem",
                            source_id=source_id,
                            iso_smiles=isomeric_smiles,
                            canonical_smiles=_clean_text(row.get("canonical_smiles")),
                            isomeric_smiles=isomeric_smiles,
                            inchi_key_prefix=_clean_text(row.get("inchi_key_prefix")),
                            inchi_key=_clean_text(row.get("inchikey")),
                            inchi=_clean_text(row.get("inchi")),
                            mw=_clean_text(row.get("molecular_weight")),
                            monoisotopic_mass=_clean_text(row.get("monoisotopic_mass")),
                            iupac_name=_clean_text(row.get("iupac_name")),
                            molecular_formula=_clean_text(row.get("molecular_formula")),
                        )
                    ],
                )
                count += 1
                if self.max_records is not None and count >= self.max_records:
                    return


def _iter_sdf_tag_records(handle) -> Iterable[Dict[str, str]]:
    lines: List[str] = []
    for raw_line in handle:
        if isinstance(raw_line, bytes):
            line = raw_line.decode("utf-8", "replace").rstrip("\r\n")
        else:
            line = raw_line.rstrip("\r\n")
        if line == "$$$$":
            yield _parse_sdf_tags(lines)
            lines = []
        else:
            lines.append(line)
    if lines:
        yield _parse_sdf_tags(lines)


def _parse_sdf_tags(lines: List[str]) -> Dict[str, str]:
    tags: Dict[str, List[str]] = {}
    current_tag = None
    for line in lines:
        if line.startswith("> <") and line.endswith(">"):
            current_tag = line[3:-1]
            tags.setdefault(current_tag, [])
            continue
        if current_tag is not None:
            tags[current_tag].append(line)
    return {
        tag: "\n".join(value_lines).strip()
        for tag, value_lines in tags.items()
        if "\n".join(value_lines).strip()
    }


def _open_text_or_gzip(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open("r", encoding="utf-8")


def _clean_text(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def _first_clean(*values: Optional[str]) -> Optional[str]:
    for value in values:
        value = _clean_text(value)
        if value:
            return value
    return None


def _prefixed_id(prefix: str, value: Optional[str]) -> Optional[str]:
    value = _clean_text(value)
    if not value:
        return None
    if prefix == "CHEBI" and value.upper().startswith("CHEBI:"):
        value = value.split(":", 1)[1].strip()
    if prefix == "LIPIDMAPS" and value.upper().startswith("LIPIDMAPS:"):
        value = value.split(":", 1)[1].strip()
    if prefix == "HMDB" and value.upper().startswith("HMDB:"):
        value = value.split(":", 1)[1].strip()
    return f"{prefix}:{value}"


def _inchi_key_prefix(inchi_key: Optional[str]) -> Optional[str]:
    inchi_key = _clean_text(inchi_key)
    if not inchi_key:
        return None
    return inchi_key.split("-", 1)[0]
