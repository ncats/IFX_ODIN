import csv
from collections import OrderedDict
from datetime import date
from typing import Dict, Generator, Iterable, List, Optional, Union

from src.constants import DataSourceName, Prefix
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.external_link import ExternalLinkDetail, ExternalLinkProvider, ProteinExternalLinkEdge
from src.models.node import EquivalentId, Node, Relationship
from src.models.protein import Protein
from src.shared.targetgraph_parser import TargetGraphProteinParser


PROVIDERS = {
    "PubChem": {
        "name": "PubChem",
        "description": "PubChem is an open chemistry database at the National Institutes of Health (NIH), that also contains a vast amount of data related to proteins and genes.",
    },
    "ARCHS4": {
        "name": "ARCHS4",
        "description": "ARCHS4 provides access to gene-function predictions based on RNA-seq co-expression, and gene expression levels across cell and tissues.",
    },
    "GlyGen": {
        "name": "GlyGen",
        "description": "GlyGen is a data integration and dissemination project for carbohydrate and glycoconjugate related data.",
    },
    "Dark Kinome": {
        "name": "Dark Kinase Knowledgebase",
        "description": "The Dark Kinase Knowledgebase is an an online compendium of knowledge and experimental results of understudied kinases.",
    },
    "RESOLUTE": {
        "name": "RESOLUTE",
        "description": "RESOLUTE is a public-private partnership with the goal of escalating research on solute carriers (SLCs) and to establish SLCs as a tractable target class for medical research and development.",
    },
    "TIGA": {
        "name": "Target Illumination GWAS Analytics (TIGA)",
        "description": "TIGA scores and ranks GWAS discovered associations according to the quantity and quality of the evidence supporting the association.",
    },
    "LinkedOmicsKB": {
        "name": "LinkedOmicsKB",
        "description": "The Clinical Proteomic Tumor Analysis Consortium (CPTAC) provides multi-omics data for cancer research based on more than 1000 primary tumors and 10 cancer types.",
    },
}


class PharosExternalLinkAdapter(InputAdapter):
    def __init__(
        self,
        protein_data_source,
        glygen_data_source,
        dark_kinome_data_source,
        resolute_data_source,
        linkedomics_data_source,
        tiga_data_source,
        canonical_only: bool = True,
        max_rows: Optional[int] = None,
    ):
        self.protein_file_path = str(protein_data_source.file("protein_ids.tsv"))
        self.glygen_file_path = str(glygen_data_source.file("glygen_proteins.csv"))
        self.dark_kinome_file_path = str(dark_kinome_data_source.file("dark_kinome_kinases.tsv"))
        self.resolute_file_path = str(resolute_data_source.file("resolute_genes.tsv"))
        self.linkedomics_file_path = str(linkedomics_data_source.file("linkedomics_genes.tsv"))
        self.tiga_stats_file_path = str(tiga_data_source.file("tiga_gene-trait_stats.tsv"))
        self.canonical_only = canonical_only
        self.max_rows = max_rows
        self.registry_version_infos = [
            (f"{data_source.source}/{data_source.dataset}", data_source.version_info())
            for data_source in [
                glygen_data_source,
                dark_kinome_data_source,
                resolute_data_source,
                linkedomics_data_source,
                tiga_data_source,
            ]
        ]
        self.version_info = self._build_version_info()

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.PharosLinkouts

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        providers = [self._provider(source) for source in PROVIDERS]
        yield providers

        edges: "OrderedDict[tuple[str, str], ProteinExternalLinkEdge]" = OrderedDict()
        for start_id, source, detail in self._iter_linkouts():
            self._add_edge(edges, start_id, source, detail)
            if len(edges) >= self.batch_size:
                yield list(edges.values())
                edges.clear()

        if edges:
            yield list(edges.values())

    def _iter_linkouts(self) -> Iterable[tuple[str, str, ExternalLinkDetail]]:
        emitted = 0
        for entry in self._iter_static_protein_linkouts():
            yield entry
            emitted += 1
            if self.max_rows is not None and emitted >= self.max_rows:
                return

        for entry in self._iter_glygen_linkouts():
            yield entry
            emitted += 1
            if self.max_rows is not None and emitted >= self.max_rows:
                return

        for entry in self._iter_symbol_file_linkouts(
            self.dark_kinome_file_path,
            source="Dark Kinome",
            source_id_type="symbol",
        ):
            yield entry
            emitted += 1
            if self.max_rows is not None and emitted >= self.max_rows:
                return

        for entry in self._iter_symbol_file_linkouts(
            self.resolute_file_path,
            source="RESOLUTE",
            source_id_type="symbol",
        ):
            yield entry
            emitted += 1
            if self.max_rows is not None and emitted >= self.max_rows:
                return

        for entry in self._iter_tiga_linkouts():
            yield entry
            emitted += 1
            if self.max_rows is not None and emitted >= self.max_rows:
                return

        for entry in self._iter_symbol_file_linkouts(
            self.linkedomics_file_path,
            source="LinkedOmicsKB",
            source_id_type="symbol",
        ):
            yield entry
            emitted += 1
            if self.max_rows is not None and emitted >= self.max_rows:
                return

    def _iter_static_protein_linkouts(self) -> Iterable[tuple[str, str, ExternalLinkDetail]]:
        parser = TargetGraphProteinParser(file_path=self.protein_file_path)
        for row in parser.all_rows():
            if self.canonical_only and TargetGraphProteinParser.get_is_canonical(row) is not True:
                continue
            protein_id = TargetGraphProteinParser.get_id(row)
            uniprot_id = (TargetGraphProteinParser.get_uniprot_id(row) or "").strip()
            if uniprot_id:
                yield (
                    protein_id,
                    "PubChem",
                    ExternalLinkDetail(
                        url=f"https://pubchem.ncbi.nlm.nih.gov/protein/{uniprot_id}",
                        source_id=uniprot_id,
                        source_id_type="uniprot",
                    ),
                )

            symbol = (TargetGraphProteinParser.get_symbol(row) or "").strip()
            if symbol and "|" not in symbol:
                yield (
                    protein_id,
                    "ARCHS4",
                    ExternalLinkDetail(
                        url=f"https://archs4.org/gene/{symbol}",
                        source_id=symbol,
                        source_id_type="symbol",
                    ),
                )

    def _iter_glygen_linkouts(self) -> Iterable[tuple[str, str, ExternalLinkDetail]]:
        if not self.glygen_file_path:
            return
        for row in self._iter_csv(self.glygen_file_path):
            source_id = (row.get("uniprot_canonical_ac") or "").strip()
            if not source_id:
                continue
            base_uniprot = source_id.split("-", 1)[0]
            if not base_uniprot:
                continue
            yield (
                EquivalentId(id=base_uniprot, type=Prefix.UniProtKB).id_str(),
                "GlyGen",
                ExternalLinkDetail(
                    url=f"https://glygen.org/protein/{base_uniprot}",
                    source_id=source_id,
                    source_id_type="uniprot_isoform" if source_id != base_uniprot else "uniprot",
                ),
            )

    def _iter_symbol_file_linkouts(
        self,
        file_path: Optional[str],
        source: str,
        source_id_type: str,
    ) -> Iterable[tuple[str, str, ExternalLinkDetail]]:
        if not file_path:
            return
        for row in self._iter_tsv(file_path):
            symbol = (row.get("symbol") or "").strip()
            url = (row.get("url") or "").strip()
            if not symbol or not url:
                continue
            yield (
                EquivalentId(id=symbol, type=Prefix.Symbol).id_str(),
                source,
                ExternalLinkDetail(url=url, source_id=symbol, source_id_type=source_id_type),
            )

    def _iter_tiga_linkouts(self) -> Iterable[tuple[str, str, ExternalLinkDetail]]:
        if not self.tiga_stats_file_path:
            return
        seen = set()
        for row in self._iter_tsv(self.tiga_stats_file_path):
            ensg = (row.get("ensemblId") or "").strip()
            if not ensg or ensg in seen:
                continue
            seen.add(ensg)
            yield (
                EquivalentId(id=ensg, type=Prefix.ENSEMBL).id_str(),
                "TIGA",
                ExternalLinkDetail(
                    url=f"https://unmtid-shinyapps.net/shiny/tiga/?gene={ensg}",
                    source_id=ensg,
                    source_id_type="ensembl_gene",
                ),
            )

    def _add_edge(
        self,
        edges: Dict[tuple[str, str], ProteinExternalLinkEdge],
        start_id: str,
        source: str,
        detail: ExternalLinkDetail,
    ) -> None:
        key = (start_id, source)
        if key not in edges:
            edges[key] = ProteinExternalLinkEdge(
                start_node=Protein(id=start_id),
                end_node=self._provider(source),
                source=source,
                url=detail.url,
                source_id=detail.source_id,
                source_id_type=detail.source_id_type,
                details=[],
            )
        if detail not in edges[key].details:
            edges[key].details.append(detail)

    def _provider(self, source: str) -> ExternalLinkProvider:
        provider = PROVIDERS[source]
        return ExternalLinkProvider(
            id=f"external_link_provider:{source}",
            source=source,
            name=provider["name"],
            description=provider["description"],
        )

    def _build_version_info(self) -> DatasourceVersionInfo:
        versions = [
            f"{label}:{info.version}"
            for label, info in self.registry_version_infos
            if info.version
        ]
        version_dates = [info.version_date for _, info in self.registry_version_infos if info.version_date]
        download_dates = [info.download_date for _, info in self.registry_version_infos if info.download_date]
        return DatasourceVersionInfo(
            version=";".join(versions) or None,
            version_date=max(version_dates, default=None),
            download_date=max(download_dates, default=date.today()),
        )

    @staticmethod
    def _iter_csv(file_path: str) -> Iterable[dict]:
        with open(file_path, "r", encoding="utf-8", errors="replace", newline="") as handle:
            yield from csv.DictReader(handle)

    @staticmethod
    def _iter_tsv(file_path: str) -> Iterable[dict]:
        with open(file_path, "r", encoding="utf-8", errors="replace", newline="") as handle:
            yield from csv.DictReader(handle, delimiter="\t")
