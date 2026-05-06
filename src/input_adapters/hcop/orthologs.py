from datetime import date
from typing import Generator, List, Optional, Tuple

from src.constants import DataSourceName
from src.input_adapters.flat_file_adapter import FlatFileAdapter
from src.input_adapters.shared.hcop import HCOPRecordHelper
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.gene import Gene
from src.models.ortholog import GeneOrthologGeneEdge, OrthologGene


class HCOPOrthologAdapter(FlatFileAdapter):
    version_info: DatasourceVersionInfo

    def __init__(self,
                 file_path: str,
                 version_file_path: Optional[str] = None,
                 accepted_species: List[str] = None,
                 drop_blank_ortholog_identity: bool = True):
        FlatFileAdapter.__init__(self, file_path=file_path)
        self.version_info = self._load_version_info(version_file_path)
        self.hcop_helper = HCOPRecordHelper(
            file_path=file_path,
            accepted_species=accepted_species,
            drop_blank_ortholog_identity=drop_blank_ortholog_identity,
        )

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.HCOP

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def get_all(self) -> Generator[List, None, None]:
        batch = []
        seen_ortholog_ids = set()
        seen_edges = set()

        for row in self.hcop_helper.iter_accepted_rows():
            ortholog_id = self.hcop_helper.preferred_ortholog_curie(row)
            human_id = self.hcop_helper.preferred_human_gene_curie(row)
            if ortholog_id is None or human_id is None:
                continue

            if ortholog_id not in seen_ortholog_ids:
                batch.append(self._ortholog_gene_from_row(row, ortholog_id))
                seen_ortholog_ids.add(ortholog_id)

            edge_key = self._edge_key(human_id, ortholog_id, row)
            if edge_key in seen_edges:
                continue

            batch.append(self._edge_from_row(row, human_id, ortholog_id))
            seen_edges.add(edge_key)

            if len(batch) >= self.batch_size:
                yield batch
                batch = []

        yield batch

    def _ortholog_gene_from_row(self, row, ortholog_id: str) -> OrthologGene:
        return OrthologGene(
            id=ortholog_id,
            species=self.hcop_helper.ortholog_species(row),
            symbol=self._clean_optional(self.hcop_helper.ortholog_symbol(row)),
            name=self._clean_optional(self.hcop_helper.ortholog_name(row)),
            source_primary_id=ortholog_id,
            source_db_id=self.hcop_helper.ortholog_db_id(row),
            entrez_gene_id=self.hcop_helper.ortholog_entrez_gene_id(row),
            ensembl_gene_id=self.hcop_helper.ortholog_ensembl_gene_id(row),
        )

    def _edge_from_row(self, row, human_id: str, ortholog_id: str) -> GeneOrthologGeneEdge:
        support_sources = sorted(self.hcop_helper.support_sources(row))
        source_db_id = self.hcop_helper.ortholog_db_id(row)
        ortholog_symbol = self._clean_optional(self.hcop_helper.ortholog_symbol(row))
        ortholog_name = self._clean_optional(self.hcop_helper.ortholog_name(row))
        return GeneOrthologGeneEdge(
            start_node=Gene(id=human_id),
            end_node=OrthologGene(id=ortholog_id),
            species=self.hcop_helper.ortholog_species(row),
            support_sources=support_sources,
            source_db_ids=[source_db_id] if source_db_id else [],
            ortholog_symbols=[ortholog_symbol] if ortholog_symbol else [],
            ortholog_names=[ortholog_name] if ortholog_name else [],
        )

    @staticmethod
    def _edge_key(human_id: str, ortholog_id: str, row) -> Tuple[str, str, str]:
        return human_id, ortholog_id, (row.get("support") or "").strip()

    @staticmethod
    def _clean_optional(value: Optional[str]) -> Optional[str]:
        if value in (None, "", "-"):
            return None
        return value

    def _load_version_info(self, version_file_path: Optional[str]) -> DatasourceVersionInfo:
        version = None
        version_date = None
        download_date = self.download_date
        if version_file_path:
            import csv
            with open(version_file_path, "r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle, delimiter="\t")
                first_row = next(reader, None)
                if first_row:
                    version = first_row.get("version") or None
                    version_date_str = first_row.get("version_date") or None
                    download_date_str = first_row.get("download_date") or None
                    version_date = date.fromisoformat(version_date_str) if version_date_str else None
                    download_date = date.fromisoformat(download_date_str) if download_date_str else download_date
        return DatasourceVersionInfo(version=version, version_date=version_date, download_date=download_date)
