import csv
import io
import zipfile
from collections import defaultdict
from typing import Dict, Generator, List, Optional, Tuple, Union

from src.constants import DataSourceName, Prefix
from src.input_adapters.shared.expression_adapter_base import ExpressionAdapterBase
from src.models.expression import ExpressionDetail, ProteinTissueExpressionEdge
from src.models.node import EquivalentId
from src.models.protein import Protein
from src.models.tissue import Tissue

# Ordinal encoding for IHC levels (used for tau and rank).
# Levels outside this map (N/A, Ascending, Descending, Not representative)
# are excluded from tau computation and stored as-is in qual_value.
QUAL_MAP = {
    "Not detected": 0,
    "Low": 1,
    "Medium": 2,
    "High": 3,
}


class _HPAExpressionBase(ExpressionAdapterBase):
    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.HPA

    def _iter_rows(self):
        if self.data_file_path.endswith(".zip"):
            with zipfile.ZipFile(self.data_file_path, "r") as zf:
                tsv_name = next(n for n in zf.namelist() if n.endswith(".tsv"))
                with zf.open(tsv_name) as raw:
                    reader = csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8"), delimiter="\t")
                    yield from reader
        else:
            with open(self.data_file_path, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f, delimiter="\t")
                yield from reader

    @staticmethod
    def _protein_id(ensg: str) -> str:
        return EquivalentId(id=ensg, type=Prefix.ENSEMBL).id_str()


class HPAProteinExpressionAdapter(_HPAExpressionBase):
    """IHC-based protein expression from normal_ihc_data.tsv.

    Columns: Gene, Gene name, Tissue, IHC tissue name, Cell type, Level, Reliability
    One edge per Gene/Tissue with one ExpressionDetail per Cell type.
    Tau is computed from max ordinal level (Not detected=0 … High=3) per tissue.
    Non-ordinal levels (N/A, Ascending, Descending, Not representative) are stored
    in qual_value but excluded from tau/rank computation.
    UBERON IDs come from manual_uberon_map.tsv, keyed by 'tissue - cell type' then 'tissue'.
    """

    def get_all(self) -> Generator[List[Union[Tissue, Protein, ProteinTissueExpressionEdge]], None, None]:
        uberon_map = self._load_uberon_map()
        gene_data, unique_tissues = self._load_grouped()
        yield [Tissue(id=self._tissue_id(t, uberon_map), name=t) for t in unique_tissues]
        yield from self._iter_batches(gene_data, uberon_map)

    def _load_grouped(self) -> Tuple[
        Dict[str, List[Tuple[str, str, str, str]]],
        List[str],
    ]:
        # gene_id -> [(tissue, cell_type, level, reliability)]
        gene_data: Dict[str, List[Tuple[str, str, str, str]]] = defaultdict(list)
        tissue_order: Dict[str, int] = {}

        for row in self._iter_rows():
            gene = row.get("Gene", "").strip()
            tissue = row.get("Tissue", "").strip()
            cell_type = row.get("Cell type", "").strip()
            level = row.get("Level", "").strip()
            reliability = row.get("Reliability", "").strip()

            if not gene or not tissue or not level:
                continue
            if reliability == "Uncertain" or level not in QUAL_MAP:
                continue

            gene_data[gene].append((tissue, cell_type, level, reliability))
            if tissue not in tissue_order:
                tissue_order[tissue] = len(tissue_order)

        unique_tissues = sorted(tissue_order, key=lambda t: tissue_order[t])
        return gene_data, unique_tissues

    def _iter_batches(
        self,
        gene_data: Dict[str, List[Tuple[str, str, str, str]]],
        uberon_map: Dict[str, Optional[str]],
    ) -> Generator[List[Union[Protein, ProteinTissueExpressionEdge]], None, None]:
        batch: List[Union[Protein, ProteinTissueExpressionEdge]] = []

        for gene_id, rows in gene_data.items():
            protein_id = self._protein_id(gene_id)
            protein = Protein(id=protein_id)

            # Group by canonical Tissue name
            by_tissue: Dict[str, List[Tuple[str, str, str]]] = defaultdict(list)
            for tissue, cell_type, level, reliability in rows:
                by_tissue[tissue].append((cell_type, level, reliability))

            # Tau uses max ordinal per tissue (all levels are guaranteed ordinal after filtering)
            max_ordinals: Dict[str, float] = {
                tissue: float(max(QUAL_MAP[ct[1]] for ct in cell_types))
                for tissue, cell_types in by_tissue.items()
            }

            tau = self._compute_tau(list(max_ordinals.values()))
            rank_map = self._normalized_rank(max_ordinals)
            protein.calculated_properties = {"hpa_ihc_tau": tau}

            edges = []
            for tissue, cell_types in by_tissue.items():
                tissue_id = self._tissue_id(tissue, uberon_map)
                details = []
                for cell_type, level, reliability in cell_types:
                    tc_key = f"{tissue.lower()} - {cell_type.lower()}"
                    uberon_id = uberon_map.get(tc_key) or uberon_map.get(tissue.lower())
                    details.append(ExpressionDetail(
                        source="HPA Protein",
                        tissue=tissue,
                        uberon_id=uberon_id,
                        cell_type=cell_type,
                        qual_value=level,
                        evidence=reliability,
                        expressed=(level != "Not detected"),
                        source_rank=rank_map.get(tissue),
                    ))
                edges.append(
                    ProteinTissueExpressionEdge(
                        start_node=protein,
                        end_node=Tissue(id=tissue_id),
                        details=details,
                    )
                )

            batch.append(protein)
            batch.extend(edges)
            if len(batch) >= self.batch_size:
                yield batch
                batch = []

        if batch:
            yield batch


class HPARnaExpressionAdapter(_HPAExpressionBase):
    """RNA expression from rna_tissue_hpa.tsv.

    Columns: Gene, Gene name, Tissue, TPM, pTPM, nTPM
    One edge per Gene/Tissue using nTPM as the expression value.
    Tau computed from nTPM values across tissues per gene.
    UBERON IDs come from manual_uberon_map.tsv, keyed by tissue name.
    """

    def get_all(self) -> Generator[List[Union[Tissue, Protein, ProteinTissueExpressionEdge]], None, None]:
        uberon_map = self._load_uberon_map()
        gene_data, unique_tissues = self._load_grouped()
        yield [Tissue(id=self._tissue_id(t, uberon_map), name=t) for t in unique_tissues]
        yield from self._iter_batches(gene_data, uberon_map)

    def _load_grouped(self) -> Tuple[
        Dict[str, List[Tuple[str, float]]],
        List[str],
    ]:
        # gene_id -> [(tissue, nTPM)]
        gene_data: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
        tissue_order: Dict[str, int] = {}

        for row in self._iter_rows():
            gene = row.get("Gene", "").strip()
            tissue = row.get("Tissue", "").strip()
            raw_ntpm = row.get("nTPM", "").strip()

            if not gene or not tissue or not raw_ntpm:
                continue

            try:
                ntpm = float(raw_ntpm)
            except ValueError:
                continue

            gene_data[gene].append((tissue, ntpm))
            if tissue not in tissue_order:
                tissue_order[tissue] = len(tissue_order)

        unique_tissues = sorted(tissue_order, key=lambda t: tissue_order[t])
        return gene_data, unique_tissues

    def _iter_batches(
        self,
        gene_data: Dict[str, List[Tuple[str, float]]],
        uberon_map: Dict[str, Optional[str]],
    ) -> Generator[List[Union[Protein, ProteinTissueExpressionEdge]], None, None]:
        batch: List[Union[Protein, ProteinTissueExpressionEdge]] = []

        for gene_id, rows in gene_data.items():
            protein_id = self._protein_id(gene_id)
            protein = Protein(id=protein_id)

            ntpm_by_tissue: Dict[str, float] = {tissue: ntpm for tissue, ntpm in rows}
            tau = self._compute_tau(list(ntpm_by_tissue.values()))
            rank_map = self._normalized_rank(ntpm_by_tissue)
            protein.calculated_properties = {"hpa_rna_tau": tau}

            edges = []
            for tissue, ntpm in ntpm_by_tissue.items():
                tissue_id = self._tissue_id(tissue, uberon_map)
                uberon_id = uberon_map.get(tissue.lower())
                detail = ExpressionDetail(
                    source="HPA RNA",
                    tissue=tissue,
                    uberon_id=uberon_id,
                    number_value=ntpm,
                    expressed=(ntpm > 0),
                    source_rank=rank_map.get(tissue),
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