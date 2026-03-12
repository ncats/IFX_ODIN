import csv
from typing import Dict, Generator, List, Optional, Union

from src.constants import DataSourceName, Prefix
from src.input_adapters.shared.expression_adapter_base import ExpressionAdapterBase
from src.models.expression import ExpressionDetail, ProteinTissueExpressionEdge
from src.models.node import EquivalentId
from src.models.protein import Protein
from src.models.tissue import Tissue


class HPMExpressionAdapter(ExpressionAdapterBase):
    def __init__(self, data_file_path: str, uberon_map_file_path: str, version_file_path: str):
        super().__init__(
            data_file_path=data_file_path,
            version_file_path=version_file_path,
            uberon_map_file_path=uberon_map_file_path,
        )

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.HPM

    def get_all(self) -> Generator[List[Union[Tissue, Protein, ProteinTissueExpressionEdge]], None, None]:
        uberon_map = self._load_uberon_map()
        tissue_columns = self._load_tissue_columns()
        yield self._build_tissue_nodes(tissue_columns, uberon_map)
        yield from self._iter_expression_batches(tissue_columns, uberon_map)

    def _load_tissue_columns(self) -> List[str]:
        with open(self.data_file_path, "r") as f:
            reader = csv.DictReader(f)
            return [col for col in reader.fieldnames if col not in ("Accession", "RefSeq Accession")]

    def _build_tissue_nodes(self, tissue_columns: List[str], uberon_map: Dict[str, Optional[str]]) -> List[Tissue]:
        return [
            Tissue(id=self._tissue_id(col, uberon_map), name=col)
            for col in tissue_columns
        ]

    def _iter_expression_batches(
        self, tissue_columns: List[str], uberon_map: Dict[str, Optional[str]]
    ) -> Generator[List[Union[Protein, ProteinTissueExpressionEdge]], None, None]:
        batch: List[Union[Protein, ProteinTissueExpressionEdge]] = []

        with open(self.data_file_path, "r") as f:
            reader = csv.DictReader(f)

            for row in reader:
                raw_refseq = row.get("RefSeq Accession", "").strip()
                if not raw_refseq:
                    continue

                refseq_id = raw_refseq.split(".")[0]
                protein_id = EquivalentId(id=refseq_id, type=Prefix.RefSeq).id_str()

                values = []
                for col in tissue_columns:
                    try:
                        values.append(float(row[col]))
                    except (ValueError, KeyError):
                        values.append(0.0)

                tau = self._compute_tau(values)
                rank_map = self._normalized_rank(dict(zip(tissue_columns, values)))

                protein = Protein(id=protein_id, calculated_properties={"hpm_tau": tau})
                edges = []
                for col, value in zip(tissue_columns, values):
                    uberon_id = uberon_map.get(col.lower())
                    tissue_id = self._tissue_id(col, uberon_map)
                    detail = ExpressionDetail(
                        source="HPM Protein",
                        tissue=col,
                        uberon_id=uberon_id,
                        number_value=value,
                        expressed=(value > 0),
                        source_rank=rank_map.get(col),
                    )
                    edges.append(
                        ProteinTissueExpressionEdge(
                            start_node=protein,
                            end_node=Tissue(id=tissue_id),
                            details=[detail],
                        )
                    )

                batch.append(protein)
                batch.extend(edges)
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []

        if batch:
            yield batch