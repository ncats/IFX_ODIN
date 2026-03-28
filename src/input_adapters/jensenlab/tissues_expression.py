import csv
import os
from collections import defaultdict
from datetime import date, datetime
from typing import Dict, Generator, List, Optional, Set, Tuple, Union

import obonet

from src.constants import DataSourceName, Prefix
from src.input_adapters.shared.expression_adapter_base import ExpressionAdapterBase
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.expression import ExpressionDetail, ProteinTissueExpressionEdge
from src.models.node import EquivalentId
from src.models.protein import Protein
from src.models.tissue import Tissue


class JensenLabTissuesExpressionAdapter(InputAdapter):
    def __init__(
        self,
        data_file_path: str,
        version_file_path: str,
        obo_file_path: Optional[str] = None,
        max_rows: Optional[int] = None,
    ):
        self.data_file_path = data_file_path
        self.version_file_path = version_file_path
        self.max_rows = max_rows
        self._valid_tissue_ids: Optional[Set[str]] = (
            self._load_valid_tissue_ids(obo_file_path) if obo_file_path else None
        )

    @staticmethod
    def _load_valid_tissue_ids(obo_file_path: str) -> Set[str]:
        """BTO and CLDB IDs that are human-grounded in uberon.obo.

        uberon.obo is multi-species. We restrict to UBERON/CL terms that carry
        an FMA xref — FMA (Foundational Model of Anatomy) is human-specific, so
        this mirrors the old TCRD getOntologyMap() and excludes mouse, zebrafish,
        and other non-human tissues that also appear in the OBO.
        """
        graph = obonet.read_obo(obo_file_path)
        valid: Set[str] = set()
        for _node_id, node_data in graph.nodes(data=True):
            xrefs = node_data.get("xref", [])
            has_fma = any(
                isinstance(x, str) and x.strip().upper().startswith("FMA:")
                for x in xrefs
            )
            if not has_fma:
                continue
            for xref in xrefs:
                if not isinstance(xref, str):
                    continue
                upper = xref.strip().upper()
                if upper.startswith("BTO:") or upper.startswith("CLDB:"):
                    valid.add(xref.strip().split(" ")[0])
        return valid

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.JensenLabTissues

    def get_version(self) -> DatasourceVersionInfo:
        download_date = None
        if os.path.exists(self.data_file_path):
            download_date = datetime.fromtimestamp(os.path.getmtime(self.data_file_path)).date()

        version_date = None
        if os.path.exists(self.version_file_path):
            with open(self.version_file_path) as f:
                reader = csv.DictReader(f, delimiter="\t")
                row = next(reader, None)
                if row and row.get("version_date"):
                    try:
                        version_date = date.fromisoformat(row["version_date"].strip())
                    except ValueError:
                        pass

        return DatasourceVersionInfo(version_date=version_date, download_date=download_date)

    def get_all(self) -> Generator[List[Union[Tissue, ProteinTissueExpressionEdge]], None, None]:
        gene_map = self._load_gene_map()
        yield self._build_tissue_nodes(gene_map)
        yield from self._iter_expression_batches(gene_map)

    def _load_gene_map(self) -> Dict[str, List[Dict]]:
        gene_map: Dict[str, List[Dict]] = defaultdict(list)
        fieldnames = ["gene_id", "gene_name", "ontology_id", "tissue", "confidence"]
        kept_rows = 0
        with open(self.data_file_path, "r") as f:
            reader = csv.DictReader(f, fieldnames=fieldnames, delimiter="\t")
            for row in reader:
                gene_id = row.get("gene_id", "").strip()
                if not (gene_id.startswith("ENSP") or gene_id.startswith("ENSG") or gene_id.startswith("ENST")):
                    continue
                bto_id = row.get("ontology_id", "").strip()
                if self._valid_tissue_ids is not None and bto_id not in self._valid_tissue_ids:
                    continue
                if self.max_rows is not None and kept_rows >= self.max_rows:
                    break
                gene_map[gene_id].append(row)
                kept_rows += 1
        return gene_map

    def _build_tissue_nodes(self, gene_map: Dict[str, List[Dict]]) -> List[Tissue]:
        seen: Dict[str, Tissue] = {}
        for rows in gene_map.values():
            for row in rows:
                bto_id = row.get("ontology_id", "").strip()
                tissue_name = row.get("tissue", "").strip()
                if bto_id and bto_id not in seen:
                    seen[bto_id] = Tissue(id=bto_id, name=tissue_name)
        return list(seen.values())

    def _iter_expression_batches(
        self, gene_map: Dict[str, List[Dict]]
    ) -> Generator[List[ProteinTissueExpressionEdge], None, None]:
        batch: List[ProteinTissueExpressionEdge] = []

        for gene_id, rows in gene_map.items():
            protein_id = EquivalentId(id=gene_id, type=Prefix.ENSEMBL).id_str()
            protein_obj = Protein(id=protein_id)

            tissue_data: Dict[str, Tuple[str, float]] = {}
            for row in rows:
                tissue = row.get("tissue", "").strip()
                bto_id = row.get("ontology_id", "").strip()
                try:
                    confidence = float(row.get("confidence", 0))
                except (ValueError, TypeError):
                    confidence = 0.0
                tissue_data[tissue] = (bto_id, confidence)

            rank_map = ExpressionAdapterBase._normalized_rank(
                {t: c for t, (_, c) in tissue_data.items()}
            )

            for tissue, (bto_id, confidence) in tissue_data.items():
                tissue_id = bto_id if bto_id else tissue
                detail = ExpressionDetail(
                    source="JensenLab",
                    tissue=tissue,
                    source_id=gene_id,
                    source_tissue_id=bto_id if bto_id else None,
                    number_value=confidence,
                    expressed=(confidence > 0),
                    source_rank=rank_map.get(tissue),
                )
                batch.append(
                    ProteinTissueExpressionEdge(
                        start_node=protein_obj,
                        end_node=Tissue(id=tissue_id),
                        details=[detail],
                    )
                )

            if len(batch) >= self.batch_size:
                yield batch
                batch = []

        if batch:
            yield batch
