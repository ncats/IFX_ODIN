from pathlib import Path
from typing import Dict, Generator, Iterable, List, Optional, Set, Tuple, Union

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.metabolite_harmonization import (
    GeneIdentifier,
    GenePathwayDetail,
    GenePathwayEdge,
    MetaboliteIdentifier,
    MetabolitePathwayDetail,
    MetabolitePathwayEdge,
    PathwayIdentifier,
    PathwayName,
)


PFOCR_SOURCE = "PFOCR"
PFOCR_PREFIX = "PFOCR"
CHEBI_PREFIX = "CHEBI"
NCBI_GENE_PREFIX = "NCBIGene"
PFOCR_GENE_GMT_FILE_NAME = "pfocr_gene_gmt.gmt"
PFOCR_CHEMICAL_GMT_FILE_NAME = "pfocr_chemical_gmt.gmt"


class PfocrPathwayContextAdapter(InputAdapter):
    def __init__(
        self,
        data_source=None,
        chemical_gmt_file: Optional[str] = None,
        gene_gmt_file: Optional[str] = None,
        max_rows: Optional[int] = None,
    ):
        if data_source is not None:
            chemical_gmt_file = str(
                _dataset_file_named(
                    data_source,
                    PFOCR_CHEMICAL_GMT_FILE_NAME,
                    fallback_suffix="-chemical-gmt-Homo_sapiens.gmt",
                )
            )
            gene_gmt_file = str(
                _dataset_file_named(
                    data_source,
                    PFOCR_GENE_GMT_FILE_NAME,
                    fallback_suffix="-gmt-Homo_sapiens.gmt",
                    exclude="chemical",
                )
            )
            self.version_info = data_source.version_info()
        else:
            self.version_info = DatasourceVersionInfo(version=None, version_date=None, download_date=None)

        if chemical_gmt_file is None:
            raise ValueError("PfocrPathwayContextAdapter requires chemical_gmt_file or data_source")
        if gene_gmt_file is None:
            raise ValueError("PfocrPathwayContextAdapter requires gene_gmt_file or data_source")

        self.chemical_gmt_file = Path(chemical_gmt_file)
        self.gene_gmt_file = Path(gene_gmt_file)
        self.max_rows = max_rows
        self._chemical_records_cache: Optional[List[Dict]] = None
        self._gene_records_cache: Optional[List[Dict]] = None

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.PFOCR

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def get_all(self) -> Generator[
        List[Union[
            GeneIdentifier,
            MetaboliteIdentifier,
            PathwayIdentifier,
            GenePathwayEdge,
            MetabolitePathwayEdge,
        ]],
        None,
        None,
    ]:
        yield from self._iter_pathway_node_batches()
        yield from self._iter_metabolite_node_batches()
        yield from self._iter_gene_node_batches()
        yield from self._iter_metabolite_pathway_edge_batches()
        yield from self._iter_gene_pathway_edge_batches()

    def _iter_pathway_node_batches(self) -> Generator[List[PathwayIdentifier], None, None]:
        batch: List[PathwayIdentifier] = []
        emitted: Set[str] = set()
        for record in self._iter_all_records():
            pathway_id = record["pathway_id"]
            if pathway_id in emitted:
                continue
            emitted.add(pathway_id)
            names = [
                PathwayName(value=record["title"], source=PFOCR_SOURCE, source_field=record["source_field"])
            ] if record.get("title") else []
            batch.append(
                PathwayIdentifier(
                    id=pathway_id,
                    stable_id=record["figure_id"],
                    url=self._figure_url(record["figure_id"]),
                    names=names,
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
        for record in self._iter_chemical_records():
            for raw_id in record["member_ids"]:
                metabolite_id = self._chebi_id(raw_id)
                if metabolite_id is None or metabolite_id in emitted:
                    continue
                emitted.add(metabolite_id)
                batch.append(MetaboliteIdentifier(id=metabolite_id))
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []
        if batch:
            yield batch

    def _iter_gene_node_batches(self) -> Generator[List[GeneIdentifier], None, None]:
        batch: List[GeneIdentifier] = []
        emitted: Set[str] = set()
        for record in self._iter_gene_records():
            for raw_id in record["member_ids"]:
                gene_id = self._ncbi_gene_id(raw_id)
                if gene_id is None or gene_id in emitted:
                    continue
                emitted.add(gene_id)
                batch.append(GeneIdentifier(id=gene_id))
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []
        if batch:
            yield batch

    def _iter_metabolite_pathway_edge_batches(self) -> Generator[List[MetabolitePathwayEdge], None, None]:
        batch: List[MetabolitePathwayEdge] = []
        emitted: Set[Tuple[str, str]] = set()
        for record in self._iter_chemical_records():
            for raw_id in record["member_ids"]:
                metabolite_id = self._chebi_id(raw_id)
                if metabolite_id is None:
                    continue
                edge_key = (metabolite_id, record["pathway_id"])
                if edge_key in emitted:
                    continue
                emitted.add(edge_key)
                batch.append(
                    MetabolitePathwayEdge(
                        start_node=MetaboliteIdentifier(id=metabolite_id),
                        end_node=PathwayIdentifier(id=record["pathway_id"]),
                        details=[
                            MetabolitePathwayDetail(
                                source=PFOCR_SOURCE,
                                source_field=record["source_field"],
                                metabolite_id=metabolite_id,
                                pathway_id=record["pathway_id"],
                                pathway_name=record.get("title"),
                                url=self._figure_url(record["figure_id"]),
                                species="Homo sapiens",
                            )
                        ],
                    )
                )
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []
        if batch:
            yield batch

    def _iter_gene_pathway_edge_batches(self) -> Generator[List[GenePathwayEdge], None, None]:
        batch: List[GenePathwayEdge] = []
        emitted: Set[Tuple[str, str]] = set()
        for record in self._iter_gene_records():
            for raw_id in record["member_ids"]:
                gene_id = self._ncbi_gene_id(raw_id)
                if gene_id is None:
                    continue
                edge_key = (gene_id, record["pathway_id"])
                if edge_key in emitted:
                    continue
                emitted.add(edge_key)
                batch.append(
                    GenePathwayEdge(
                        start_node=GeneIdentifier(id=gene_id),
                        end_node=PathwayIdentifier(id=record["pathway_id"]),
                        details=[
                            GenePathwayDetail(
                                source=PFOCR_SOURCE,
                                source_field=record["source_field"],
                                gene_id=gene_id,
                                pathway_id=record["pathway_id"],
                                pathway_name=record.get("title"),
                                url=self._figure_url(record["figure_id"]),
                                species="Homo sapiens",
                            )
                        ],
                    )
                )
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []
        if batch:
            yield batch

    def _iter_all_records(self) -> Iterable[Dict]:
        chemical_records, gene_records = self._load_records()
        yield from chemical_records
        yield from gene_records

    def _iter_chemical_records(self) -> Iterable[Dict]:
        chemical_records, _gene_records = self._load_records()
        yield from chemical_records

    def _iter_gene_records(self) -> Iterable[Dict]:
        _chemical_records, gene_records = self._load_records()
        yield from gene_records

    def _load_records(self) -> Tuple[List[Dict], List[Dict]]:
        if self._chemical_records_cache is not None and self._gene_records_cache is not None:
            return self._chemical_records_cache, self._gene_records_cache

        figure_titles: Dict[str, str] = {}
        self._chemical_records_cache = list(
            self._iter_gmt_records(self.chemical_gmt_file, "pfocr_chemical_gmt", figure_titles)
        )
        self._gene_records_cache = list(
            self._iter_gmt_records(self.gene_gmt_file, "pfocr_gene_gmt", figure_titles)
        )
        return self._chemical_records_cache, self._gene_records_cache

    def _iter_gmt_records(self, file_path: Path, source_field: str, figure_titles: Dict[str, str]) -> Iterable[Dict]:
        with open(file_path, "r", encoding="utf-8") as handle:
            for row_idx, line in enumerate(handle):
                if self.max_rows is not None and row_idx >= self.max_rows:
                    break
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 2:
                    continue
                figure_id = parts[0].strip()
                title = parts[1].strip()
                if not figure_id or not title:
                    continue
                unique_figure_id = self._unique_figure_id(figure_id, title, figure_titles)
                figure_titles[unique_figure_id] = title
                yield {
                    "figure_id": unique_figure_id,
                    "pathway_id": f"{PFOCR_PREFIX}:{unique_figure_id}",
                    "title": title,
                    "member_ids": [value.strip() for value in parts[2:] if value.strip()],
                    "source_field": source_field,
                }

    @classmethod
    def _unique_figure_id(cls, figure_id: str, title: str, figure_titles: Dict[str, str]) -> str:
        existing = figure_titles.get(figure_id)
        if existing is None or existing == title:
            return figure_id
        pieces = figure_id.rsplit(".", 1)
        if len(pieces) == 2 and pieces[1].isdigit():
            base = pieces[0]
            count = int(pieces[1]) + 1
        else:
            base = figure_id
            count = 1
        return cls._unique_figure_id(f"{base}.{count}", title, figure_titles)

    @staticmethod
    def _chebi_id(value: str) -> Optional[str]:
        value = value.strip()
        if not value:
            return None
        if value.lower().startswith("chebi:"):
            value = value.split(":", 1)[1].strip()
        return f"{CHEBI_PREFIX}:{value}"

    @staticmethod
    def _ncbi_gene_id(value: str) -> Optional[str]:
        value = value.strip()
        if not value:
            return None
        if value.upper().startswith("NCBIGENE:"):
            value = value.split(":", 1)[1].strip()
        return f"{NCBI_GENE_PREFIX}:{value}"

    @staticmethod
    def _figure_url(figure_id: str) -> str:
        pmc_id = figure_id.split("__", 1)[0]
        return f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmc_id}/"


def _dataset_file_named(
    data_source,
    file_name: str,
    *,
    fallback_suffix: str,
    exclude: Optional[str] = None,
) -> Path:
    files = data_source.manifest.get("files", []) or []
    if any(file_info.get("path") == file_name for file_info in files):
        return data_source.file(file_name)
    return _dataset_file_matching(data_source, fallback_suffix, exclude=exclude)


def _dataset_file_matching(data_source, suffix: str, *, exclude: Optional[str] = None) -> Path:
    matches = []
    for file_info in data_source.manifest.get("files", []) or []:
        path = file_info.get("path", "")
        if not path.endswith(suffix):
            continue
        if exclude is not None and exclude in path:
            continue
        matches.append(path)
    if len(matches) != 1:
        raise ValueError(f"Expected one PFOCR file ending {suffix}, found {matches}")
    return data_source.file(matches[0])
