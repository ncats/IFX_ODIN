import csv
from datetime import date
from typing import Generator, List, Optional

from src.constants import DataSourceName, Prefix
from src.input_adapters.flat_file_adapter import FlatFileAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.gene import Gene
from src.models.mouse_phenotype import GeneMousePhenotypeEdge, MousePhenotype, MousePhenotypeDetail
from src.models.node import EquivalentId


class HMDHumanPhenotypeAdapter(FlatFileAdapter):
    version_info: DatasourceVersionInfo

    FIELDNAMES = [
        "human_symbol",
        "human_entrez_gene_id",
        "mouse_symbol",
        "mouse_mgi_marker_id",
        "mp_ids",
        "unused",
    ]

    def __init__(self, file_path: str = None, version_file_path: Optional[str] = None, data_source=None):
        if data_source is not None:
            file_path = str(data_source.file("HMD_HumanPhenotype.rpt"))
        if file_path is None:
            raise ValueError("HMDHumanPhenotypeAdapter requires file_path or data_source")
        FlatFileAdapter.__init__(self, file_path=file_path)
        self.version_info = data_source.version_info() if data_source is not None else self._load_version_info(version_file_path)

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.MGI

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def get_all(self) -> Generator[List, None, None]:
        batch = []
        seen_phenotypes = set()

        with open(self.file_path, "r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle, fieldnames=self.FIELDNAMES, delimiter="\t")
            for row in reader:
                gene_id = self._human_gene_id(row)
                phenotype_ids = self._mouse_phenotype_ids(row)
                if gene_id is None or not phenotype_ids:
                    continue

                for phenotype_id in phenotype_ids:
                    if phenotype_id not in seen_phenotypes:
                        batch.append(MousePhenotype(id=phenotype_id))
                        seen_phenotypes.add(phenotype_id)

                    batch.append(
                        GeneMousePhenotypeEdge(
                            start_node=Gene(
                                id=gene_id,
                                symbol=self._clean_optional(row.get("human_symbol")),
                            ),
                            end_node=MousePhenotype(id=phenotype_id),
                            details=[self._detail_from_row(row)],
                        )
                    )

                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []

        yield batch

    @staticmethod
    def _human_gene_id(row: dict) -> Optional[str]:
        gene_id = (row.get("human_entrez_gene_id") or "").strip()
        if gene_id:
            return EquivalentId(id=gene_id, type=Prefix.NCBIGene).id_str()

        symbol = (row.get("human_symbol") or "").strip()
        if symbol:
            return EquivalentId(id=symbol, type=Prefix.Symbol).id_str()

        return None

    @staticmethod
    def _mouse_phenotype_ids(row: dict) -> List[str]:
        return [
            mp_id.strip()
            for mp_id in (row.get("mp_ids") or "").split(",")
            if mp_id.strip()
        ]

    @staticmethod
    def _detail_from_row(row: dict) -> MousePhenotypeDetail:
        return MousePhenotypeDetail(
            source=DataSourceName.MGI.value,
            source_id=HMDHumanPhenotypeAdapter._clean_optional(row.get("mouse_mgi_marker_id")),
        )

    @staticmethod
    def _clean_optional(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = value.strip()
        if value in ("", "-"):
            return None
        return value

    def _load_version_info(self, version_file_path: Optional[str]) -> DatasourceVersionInfo:
        version = None
        version_date = None
        download_date = self.download_date
        if version_file_path:
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
