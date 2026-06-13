import re
from typing import Generator, List, Optional

from src.constants import DataSourceName, Prefix
from src.input_adapters.flat_file_adapter import FlatFileAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import EquivalentId
from src.models.gene import Gene
from src.models.pathway import GenePathwayEdge, Pathway


class WikiPathwaysBaseAdapter(FlatFileAdapter):

    def __init__(self, data_source, max_rows: Optional[int] = None):
        file_path = str(data_source.file())
        FlatFileAdapter.__init__(self, file_path=file_path)
        self.max_rows = max_rows
        self.version_info = data_source.version_info()

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.WikiPathways

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def _iter_parsed_lines(self):
        """Yields (name, wpid, url, genes) tuples from the GMT file."""
        with open(self.file_path, "r", encoding="utf-8") as handle:
            for idx, line in enumerate(handle):
                if self.max_rows is not None and idx >= self.max_rows:
                    break
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 3:
                    continue
                meta = parts[0]
                url = parts[1]
                genes = [g.strip() for g in parts[2:] if g.strip()]
                match = re.match(r"(.*?)%.*?(WP\d+)%Homo sapiens", meta)
                if not match:
                    continue
                name, wpid = match.groups()
                yield name.strip(), wpid, url, genes


class WikiPathwaysPathwayAdapter(WikiPathwaysBaseAdapter):

    def get_all(self) -> Generator[List[Pathway], None, None]:
        pathways: List[Pathway] = []
        for name, wpid, url, _ in self._iter_parsed_lines():
            pathways.append(
                Pathway(
                    id=wpid,
                    source_id=wpid,
                    type="WikiPathways",
                    name=name,
                    url=url
                )
            )
        yield pathways


class WikiPathwaysGenePathwayEdgeAdapter(WikiPathwaysBaseAdapter):

    def get_all(self) -> Generator[List[GenePathwayEdge], None, None]:
        edges: List[GenePathwayEdge] = []
        for _, wpid, _, genes in self._iter_parsed_lines():
            for gene_id in genes:
                if not gene_id.isdigit():
                    continue
                gene_equivalent_id = EquivalentId(id=gene_id, type=Prefix.NCBIGene)
                edges.append(
                    GenePathwayEdge(
                        start_node=Gene(id=gene_equivalent_id.id_str()),
                        end_node=Pathway(id=wpid),
                        source="WikiPathways"
                    )
                )
        yield edges
