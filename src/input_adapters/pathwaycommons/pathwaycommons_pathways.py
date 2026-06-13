import gzip
import re
from typing import Generator, List, Optional

from src.constants import DataSourceName, Prefix
from src.input_adapters.flat_file_adapter import FlatFileAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.gene import Gene
from src.models.node import EquivalentId
from src.models.pathway import GenePathwayEdge, Pathway

EXCLUDED_SOURCES = {"reactome", "wikipathways"}

_URL_PREFIXES_TO_STRIP = [
    "http://bioregistry.io/",
    "https://identifiers.org/",
]


def _clean_pathway_id(raw_id: str) -> tuple:
    """Returns (clean_id, source_id, url).
    Strips known resolver URL prefixes so the id is in compact 'prefix:localid' form.
    source_id is the local part after the last ':'.
    url preserves the original HTTP address when present.
    """
    url = raw_id if raw_id.startswith("http") else None
    clean_id = raw_id
    for prefix in _URL_PREFIXES_TO_STRIP:
        if raw_id.startswith(prefix):
            clean_id = raw_id[len(prefix):]
            break
    source_id = clean_id.split(":")[-1] if ":" in clean_id else clean_id
    return clean_id, source_id, url


class PathwayCommonsBaseAdapter(FlatFileAdapter):

    def __init__(self, data_source, max_rows: Optional[int] = None):
        file_path = str(data_source.file("pc-hgnc.gmt.gz"))
        FlatFileAdapter.__init__(self, file_path=file_path)
        self.max_rows = max_rows
        self.version_info = data_source.version_info()

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.PathwayCommons

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def _iter_parsed_lines(self):
        """Yields (clean_id, source_id, name, datasource, url, genes) tuples from the GMT file."""
        with gzip.open(self.file_path, "rt", encoding="utf-8") as handle:
            for idx, line in enumerate(handle):
                if self.max_rows is not None and idx >= self.max_rows:
                    break
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 2:
                    continue
                raw_id = parts[0].strip()
                meta = parts[1].strip()
                genes = [g.strip() for g in parts[2:] if g.strip()]

                name_match = re.search(r"name: ([^;]+)", meta)
                src_match = re.search(r"datasource: ([^;]+)", meta)
                if not name_match or not src_match:
                    continue

                name = name_match.group(1).strip()
                datasource = src_match.group(1).strip()

                if datasource in EXCLUDED_SOURCES:
                    continue

                clean_id, source_id, url = _clean_pathway_id(raw_id)
                yield clean_id, source_id, name, datasource, url, genes


class PathwayCommonsPathwayAdapter(PathwayCommonsBaseAdapter):

    def get_all(self) -> Generator[List[Pathway], None, None]:
        seen = set()
        pathways: List[Pathway] = []
        for clean_id, source_id, name, datasource, url, _ in self._iter_parsed_lines():
            if clean_id in seen:
                continue
            seen.add(clean_id)
            pathways.append(
                Pathway(
                    id=clean_id,
                    source_id=source_id,
                    type="PathwayCommons",
                    original_datasource=datasource,
                    name=name,
                    url=url
                )
            )
        yield pathways


class PathwayCommonsGenePathwayEdgeAdapter(PathwayCommonsBaseAdapter):

    def get_all(self) -> Generator[List[GenePathwayEdge], None, None]:
        edges: List[GenePathwayEdge] = []
        for clean_id, _, _, _, _, genes in self._iter_parsed_lines():
            for gene in genes:
                gene_id = EquivalentId(id=gene, type=Prefix.Symbol)
                edges.append(
                    GenePathwayEdge(
                        start_node=Gene(id=gene_id.id_str()),
                        end_node=Pathway(id=clean_id),
                        source="PathwayCommons"
                    )
                )
        yield edges
