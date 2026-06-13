import csv
import gzip
from typing import Generator, List, Optional

from src.constants import DataSourceName
from src.input_adapters.flat_file_adapter import FlatFileAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.mouse_phenotype import MousePhenotype, MousePhenotypeDetail, OrthologGeneMousePhenotypeEdge
from src.models.ortholog import OrthologGene


class IMPCPhenotypeAdapter(FlatFileAdapter):
    version_info: DatasourceVersionInfo

    def __init__(self, data_source):
        file_path = str(data_source.file("genotype-phenotype-assertions-IMPC.csv.gz"))
        FlatFileAdapter.__init__(self, file_path=file_path)
        self.version_info = data_source.version_info()

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.IMPC

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def get_all(self) -> Generator[List, None, None]:
        batch = []
        seen_phenotypes = set()

        with gzip.open(self.file_path, "rt", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                ortholog_id = self._ortholog_gene_id(row)
                phenotype_id = self._mouse_phenotype_id(row)
                if ortholog_id is None or phenotype_id is None:
                    continue

                if phenotype_id not in seen_phenotypes:
                    batch.append(self._mouse_phenotype_from_row(row, phenotype_id))
                    seen_phenotypes.add(phenotype_id)

                batch.append(self._edge_from_row(row, ortholog_id, phenotype_id))

                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []

        yield batch

    @staticmethod
    def _ortholog_gene_id(row: dict) -> Optional[str]:
        marker_accession_id = (row.get("marker_accession_id") or "").strip()
        if marker_accession_id and marker_accession_id != "-":
            return marker_accession_id
        return None

    @staticmethod
    def _mouse_phenotype_id(row: dict) -> Optional[str]:
        mp_term_id = (row.get("mp_term_id") or "").strip()
        if mp_term_id and mp_term_id != "-":
            return mp_term_id
        return None

    @staticmethod
    def _mouse_phenotype_from_row(row: dict, phenotype_id: str) -> MousePhenotype:
        return MousePhenotype(
            id=phenotype_id,
            name=IMPCPhenotypeAdapter._clean_optional(row.get("mp_term_name")),
        )

    def _edge_from_row(self, row: dict, ortholog_id: str, phenotype_id: str) -> OrthologGeneMousePhenotypeEdge:
        return OrthologGeneMousePhenotypeEdge(
            start_node=OrthologGene(id=ortholog_id),
            end_node=MousePhenotype(id=phenotype_id),
            details=[self._detail_from_row(row)],
        )

    @staticmethod
    def _detail_from_row(row: dict) -> MousePhenotypeDetail:
        return MousePhenotypeDetail(
            source=DataSourceName.IMPC.value,
            source_id=IMPCPhenotypeAdapter._clean_optional(row.get("resource_name")),
            top_level_term_id=IMPCPhenotypeAdapter._clean_optional(row.get("top_level_mp_term_id")),
            top_level_term_name=IMPCPhenotypeAdapter._clean_optional(row.get("top_level_mp_term_name")),
            p_value=IMPCPhenotypeAdapter._parse_float(row.get("p_value")),
            percentage_change=IMPCPhenotypeAdapter._clean_optional(row.get("percentage_change")),
            effect_size=IMPCPhenotypeAdapter._clean_optional(row.get("effect_size")),
            procedure_name=IMPCPhenotypeAdapter._clean_optional(row.get("procedure_name")),
            parameter_name=IMPCPhenotypeAdapter._clean_optional(row.get("parameter_name")),
            gp_assoc=True,
            statistical_method=IMPCPhenotypeAdapter._clean_optional(row.get("statistical_method")),
            sex=IMPCPhenotypeAdapter._clean_optional(row.get("sex")),
        )

    @staticmethod
    def _clean_optional(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = value.strip()
        if value in ("", "-"):
            return None
        return value

    @staticmethod
    def _parse_float(value: Optional[str]) -> Optional[float]:
        value = IMPCPhenotypeAdapter._clean_optional(value)
        if value is None:
            return None
        try:
            return float(value)
        except ValueError:
            return None
