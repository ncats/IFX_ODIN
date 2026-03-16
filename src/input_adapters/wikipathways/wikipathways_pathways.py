import csv
import re
from datetime import date
from typing import Generator, List, Optional

from src.constants import DataSourceName, Prefix
from src.input_adapters.flat_file_adapter import FlatFileAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import EquivalentId
from src.models.pathway import Pathway, ProteinPathwayRelationship
from src.models.protein import Protein


class WikiPathwaysBaseAdapter(FlatFileAdapter):

    def __init__(self, file_path: str, version_file_path: Optional[str] = None):
        FlatFileAdapter.__init__(self, file_path=file_path)
        version = None
        version_date = None
        if version_file_path:
            with open(version_file_path, "r", encoding="utf-8") as vf:
                reader = csv.DictReader(vf, delimiter="\t")
                first_row = next(reader, None)
                if first_row:
                    version = first_row.get("version") or None
                    version_date = first_row.get("version_date") or None
        self.version_info = DatasourceVersionInfo(
            version=version,
            version_date=date.fromisoformat(version_date) if version_date else None,
            download_date=self.download_date
        )

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.WikiPathways

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def _iter_parsed_lines(self):
        """Yields (name, wpid, url, genes) tuples from the GMT file."""
        with open(self.file_path, "r", encoding="utf-8") as handle:
            for line in handle:
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


class WikiPathwaysProteinPathwayEdgeAdapter(WikiPathwaysBaseAdapter):

    def get_all(self) -> Generator[List[ProteinPathwayRelationship], None, None]:
        edges: List[ProteinPathwayRelationship] = []
        for _, wpid, _, genes in self._iter_parsed_lines():
            for gene_id in genes:
                if not gene_id.isdigit():
                    continue
                protein_id = EquivalentId(id=gene_id, type=Prefix.NCBIGene)
                edges.append(
                    ProteinPathwayRelationship(
                        start_node=Protein(id=protein_id.id_str()),
                        end_node=Pathway(id=wpid),
                        source="WikiPathways"
                    )
                )
        yield edges
