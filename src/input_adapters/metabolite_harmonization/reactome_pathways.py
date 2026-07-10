from pathlib import Path
from typing import Dict, Generator, Iterable, List, Optional, Set, Tuple, Union
import io
import zipfile

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.metabolite_harmonization import (
    MetaboliteIdentifier,
    MetabolitePathwayDetail,
    MetabolitePathwayEdge,
    PathwayIdentifier,
    PathwayName,
    ReactomePathwayParentDetail,
    ReactomePathwayParentEdge,
    ProteinIdentifier,
    ProteinPathwayDetail,
    ProteinPathwayEdge,
)


REACTOME_SOURCE = "Reactome"
REACTOME_PREFIX = "Reactome"
CHEBI_PREFIX = "CHEBI"
UNIPROTKB_PREFIX = "UniProtKB"
HUMAN_SPECIES = "Homo sapiens"


class ReactomePathwayContextAdapter(InputAdapter):
    def __init__(
        self,
        data_source=None,
        gmt_zip_file: Optional[str] = None,
        pathway_relation_file: Optional[str] = None,
        chebi_mapping_file: Optional[str] = None,
        uniprot_mapping_file: Optional[str] = None,
        max_records: Optional[int] = None,
    ):
        if data_source is not None:
            gmt_zip_file = str(data_source.file("ReactomePathways.gmt.zip"))
            pathway_relation_file = str(data_source.file("ReactomePathwaysRelation.txt"))
            chebi_mapping_file = str(data_source.file("ChEBI2Reactome_All_Levels.txt"))
            uniprot_mapping_file = str(data_source.file("UniProt2Reactome_All_Levels.txt"))
            self.version_info = data_source.version_info()
        else:
            self.version_info = DatasourceVersionInfo(version=None, version_date=None, download_date=None)

        if gmt_zip_file is None:
            raise ValueError("ReactomePathwayContextAdapter requires gmt_zip_file or data_source")
        if pathway_relation_file is None:
            raise ValueError("ReactomePathwayContextAdapter requires pathway_relation_file or data_source")
        if chebi_mapping_file is None:
            raise ValueError("ReactomePathwayContextAdapter requires chebi_mapping_file or data_source")
        if uniprot_mapping_file is None:
            raise ValueError("ReactomePathwayContextAdapter requires uniprot_mapping_file or data_source")

        self.gmt_zip_file = Path(gmt_zip_file)
        self.pathway_relation_file = Path(pathway_relation_file)
        self.chebi_mapping_file = Path(chebi_mapping_file)
        self.uniprot_mapping_file = Path(uniprot_mapping_file)
        self.max_records = max_records

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.Reactome

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def get_all(self) -> Generator[
        List[
            Union[
                MetaboliteIdentifier,
                PathwayIdentifier,
                ProteinIdentifier,
                MetabolitePathwayEdge,
                ProteinPathwayEdge,
                ReactomePathwayParentEdge,
            ]
        ],
        None,
        None,
    ]:
        yield from self._iter_pathway_node_batches()
        yield from self._iter_metabolite_node_batches()
        yield from self._iter_protein_node_batches()
        yield from self._iter_pathway_parent_edge_batches()
        yield from self._iter_metabolite_pathway_edge_batches()
        yield from self._iter_protein_pathway_edge_batches()

    def _iter_pathway_node_batches(self) -> Generator[List[PathwayIdentifier], None, None]:
        batch: List[PathwayIdentifier] = []
        emitted: Set[str] = set()

        for record in self._iter_gmt_pathways():
            pathway_id = self._reactome_id(record["pathway_id"])
            if pathway_id is None or pathway_id in emitted:
                continue
            emitted.add(pathway_id)
            batch.append(
                self._pathway_node(
                    pathway_id=pathway_id,
                    stable_id=record["pathway_id"],
                    name=record.get("pathway_name"),
                    url=self._detail_url(record["pathway_id"]),
                    source_field="ReactomePathways.gmt.zip",
                )
            )
            if len(batch) >= self.batch_size:
                yield batch
                batch = []

        for record in self._iter_mapping_rows(self.chebi_mapping_file, "ChEBI2Reactome_All_Levels.txt"):
            pathway_id = self._reactome_id(record["pathway_id"])
            if pathway_id is None or pathway_id in emitted:
                continue
            emitted.add(pathway_id)
            batch.append(
                self._pathway_node(
                    pathway_id=pathway_id,
                    stable_id=record["pathway_id"],
                    name=record.get("pathway_name"),
                    url=record.get("url"),
                    source_field=record["source_field"],
                )
            )
            if len(batch) >= self.batch_size:
                yield batch
                batch = []

        for record in self._iter_mapping_rows(self.uniprot_mapping_file, "UniProt2Reactome_All_Levels.txt"):
            pathway_id = self._reactome_id(record["pathway_id"])
            if pathway_id is None or pathway_id in emitted:
                continue
            emitted.add(pathway_id)
            batch.append(
                self._pathway_node(
                    pathway_id=pathway_id,
                    stable_id=record["pathway_id"],
                    name=record.get("pathway_name"),
                    url=record.get("url"),
                    source_field=record["source_field"],
                )
            )
            if len(batch) >= self.batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

    def _iter_metabolite_node_batches(self) -> Generator[List[MetaboliteIdentifier], None, None]:
        batch: List[MetaboliteIdentifier] = []
        emitted: Set[str] = set()
        for record in self._iter_mapping_rows(self.chebi_mapping_file, "ChEBI2Reactome_All_Levels.txt"):
            metabolite_id = self._chebi_id(record["subject_id"])
            if metabolite_id is None or metabolite_id in emitted:
                continue
            emitted.add(metabolite_id)
            batch.append(MetaboliteIdentifier(id=metabolite_id))
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _iter_protein_node_batches(self) -> Generator[List[ProteinIdentifier], None, None]:
        batch: List[ProteinIdentifier] = []
        emitted: Set[str] = set()
        for record in self._iter_mapping_rows(self.uniprot_mapping_file, "UniProt2Reactome_All_Levels.txt"):
            protein_id = self._uniprot_id(record["subject_id"])
            if protein_id is None or protein_id in emitted:
                continue
            emitted.add(protein_id)
            batch.append(ProteinIdentifier(id=protein_id))
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _iter_pathway_parent_edge_batches(self) -> Generator[List[ReactomePathwayParentEdge], None, None]:
        batch: List[ReactomePathwayParentEdge] = []
        emitted: Set[Tuple[str, str]] = set()
        for parent_stable_id, child_stable_id in self._iter_pathway_relations():
            parent_id = self._reactome_id(parent_stable_id)
            child_id = self._reactome_id(child_stable_id)
            if parent_id is None or child_id is None:
                continue
            edge_key = (parent_id, child_id)
            if edge_key in emitted:
                continue
            emitted.add(edge_key)
            batch.append(
                ReactomePathwayParentEdge(
                    start_node=PathwayIdentifier(id=parent_id),
                    end_node=PathwayIdentifier(id=child_id),
                    details=[
                        ReactomePathwayParentDetail(
                            source=REACTOME_SOURCE,
                            source_field="ReactomePathwaysRelation.txt",
                            parent_pathway_id=parent_id,
                            child_pathway_id=child_id,
                        )
                    ],
                )
            )
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _iter_metabolite_pathway_edge_batches(
        self,
    ) -> Generator[List[MetabolitePathwayEdge], None, None]:
        edge_details: Dict[Tuple[str, str], List[MetabolitePathwayDetail]] = {}
        emitted_details: Set[Tuple[str, str, str, Optional[str], Optional[str]]] = set()
        for record in self._iter_mapping_rows(self.chebi_mapping_file, "ChEBI2Reactome_All_Levels.txt"):
            metabolite_id = self._chebi_id(record["subject_id"])
            pathway_id = self._reactome_id(record["pathway_id"])
            if metabolite_id is None or pathway_id is None:
                continue
            detail_key = (
                metabolite_id,
                pathway_id,
                record["source_field"],
                record.get("evidence_code"),
                record.get("species"),
            )
            if detail_key in emitted_details:
                continue
            emitted_details.add(detail_key)
            edge_details.setdefault((metabolite_id, pathway_id), []).append(
                MetabolitePathwayDetail(
                    source=REACTOME_SOURCE,
                    source_field=record["source_field"],
                    metabolite_id=metabolite_id,
                    pathway_id=pathway_id,
                    pathway_name=record.get("pathway_name"),
                    url=record.get("url"),
                    evidence_code=record.get("evidence_code"),
                    species=record.get("species"),
                )
            )

        batch: List[MetabolitePathwayEdge] = []
        for (metabolite_id, pathway_id), details in edge_details.items():
            batch.append(
                MetabolitePathwayEdge(
                    start_node=MetaboliteIdentifier(id=metabolite_id),
                    end_node=PathwayIdentifier(id=pathway_id),
                    details=details,
                )
            )
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _iter_protein_pathway_edge_batches(
        self,
    ) -> Generator[List[ProteinPathwayEdge], None, None]:
        edge_details: Dict[Tuple[str, str], List[ProteinPathwayDetail]] = {}
        emitted_details: Set[Tuple[str, str, str, Optional[str], Optional[str]]] = set()
        for record in self._iter_mapping_rows(self.uniprot_mapping_file, "UniProt2Reactome_All_Levels.txt"):
            protein_id = self._uniprot_id(record["subject_id"])
            pathway_id = self._reactome_id(record["pathway_id"])
            if protein_id is None or pathway_id is None:
                continue
            detail_key = (
                protein_id,
                pathway_id,
                record["source_field"],
                record.get("evidence_code"),
                record.get("species"),
            )
            if detail_key in emitted_details:
                continue
            emitted_details.add(detail_key)
            edge_details.setdefault((protein_id, pathway_id), []).append(
                ProteinPathwayDetail(
                    source=REACTOME_SOURCE,
                    source_field=record["source_field"],
                    protein_id=protein_id,
                    pathway_id=pathway_id,
                    pathway_name=record.get("pathway_name"),
                    url=record.get("url"),
                    evidence_code=record.get("evidence_code"),
                    species=record.get("species"),
                )
            )

        batch: List[ProteinPathwayEdge] = []
        for (protein_id, pathway_id), details in edge_details.items():
            batch.append(
                ProteinPathwayEdge(
                    start_node=ProteinIdentifier(id=protein_id),
                    end_node=PathwayIdentifier(id=pathway_id),
                    details=details,
                )
            )
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _iter_gmt_pathways(self) -> Iterable[Dict]:
        with zipfile.ZipFile(self.gmt_zip_file) as archive:
            gmt_members = [info.filename for info in archive.infolist() if info.filename.endswith(".gmt")]
            if not gmt_members:
                raise ValueError(f"No .gmt file found in {self.gmt_zip_file}")
            with archive.open(gmt_members[0]) as handle:
                count = 0
                for line in io.TextIOWrapper(handle, encoding="utf-8"):
                    parts = line.rstrip("\n").split("\t")
                    if len(parts) < 2:
                        continue
                    pathway_id = self._clean_text(parts[1])
                    if not self._is_human_reactome_stable_id(pathway_id):
                        continue
                    yield {
                        "pathway_id": pathway_id,
                        "pathway_name": self._clean_text(parts[0]),
                    }
                    count += 1
                    if self.max_records is not None and count >= self.max_records:
                        return

    def _iter_mapping_rows(self, file_path: Path, source_field: str) -> Iterable[Dict]:
        count = 0
        with open(file_path, "r", encoding="utf-8") as handle:
            for line in handle:
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 6:
                    continue
                subject_id, pathway_id, url, pathway_name, evidence_code, species = parts[:6]
                subject_id = self._clean_text(subject_id)
                pathway_id = self._clean_text(pathway_id)
                species = self._clean_text(species)
                if subject_id is None or not self._is_human_reactome_stable_id(pathway_id):
                    continue
                if species != HUMAN_SPECIES and species != "9606":
                    continue
                yield {
                    "subject_id": subject_id,
                    "pathway_id": pathway_id,
                    "url": self._clean_text(url),
                    "pathway_name": self._clean_text(pathway_name),
                    "evidence_code": self._clean_text(evidence_code),
                    "species": species,
                    "source_field": source_field,
                }
                count += 1
                if self.max_records is not None and count >= self.max_records:
                    return

    def _iter_pathway_relations(self) -> Iterable[Tuple[str, str]]:
        count = 0
        with open(self.pathway_relation_file, "r", encoding="utf-8") as handle:
            for line in handle:
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 2:
                    continue
                parent_id = self._clean_text(parts[0])
                child_id = self._clean_text(parts[1])
                if not self._is_human_reactome_stable_id(parent_id):
                    continue
                if not self._is_human_reactome_stable_id(child_id):
                    continue
                yield parent_id, child_id
                count += 1
                if self.max_records is not None and count >= self.max_records:
                    return

    @classmethod
    def _pathway_node(
        cls,
        *,
        pathway_id: str,
        stable_id: str,
        name: Optional[str],
        url: Optional[str],
        source_field: str,
    ) -> PathwayIdentifier:
        names = []
        if name:
            names.append(
                PathwayName(
                    value=name,
                    source=REACTOME_SOURCE,
                    source_field=source_field,
                )
            )
        return PathwayIdentifier(
            id=pathway_id,
            stable_id=stable_id,
            url=url,
            names=names,
        )

    @classmethod
    def _reactome_id(cls, stable_id: Optional[str]) -> Optional[str]:
        stable_id = cls._clean_text(stable_id)
        if not cls._is_human_reactome_stable_id(stable_id):
            return None
        return f"{REACTOME_PREFIX}:{stable_id}"

    @classmethod
    def _chebi_id(cls, value: Optional[str]) -> Optional[str]:
        value = cls._clean_text(value)
        if value is None:
            return None
        if value.upper().startswith("CHEBI:"):
            value = value.split(":", 1)[1].strip()
        return f"{CHEBI_PREFIX}:{value}"

    @classmethod
    def _uniprot_id(cls, value: Optional[str]) -> Optional[str]:
        value = cls._clean_text(value)
        if value is None:
            return None
        if value.upper().startswith("UNIPROTKB:"):
            value = value.split(":", 1)[1].strip()
        return f"{UNIPROTKB_PREFIX}:{value}"

    @staticmethod
    def _detail_url(stable_id: str) -> str:
        return f"https://reactome.org/content/detail/{stable_id}"

    @staticmethod
    def _is_human_reactome_stable_id(value: Optional[str]) -> bool:
        return bool(value and value.startswith("R-HSA-"))

    @staticmethod
    def _clean_text(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = value.strip()
        return value or None
