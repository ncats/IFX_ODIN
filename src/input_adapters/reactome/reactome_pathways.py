import io
import zipfile
from typing import Generator, List

from src.constants import DataSourceName, Prefix
from src.input_adapters.flat_file_adapter import FlatFileAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import EquivalentId
from src.models.pathway import Pathway, ProteinPathwayEdge, PathwayParentEdge
from src.models.protein import Protein


class ReactomeBaseAdapter(FlatFileAdapter):
    version_info: DatasourceVersionInfo

    def __init__(self, data_source, file_name: str):
        file_path = str(data_source.file(file_name))
        FlatFileAdapter.__init__(self, file_path=file_path)
        self.version_info = data_source.version_info()

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.Reactome

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info


class ReactomePathwayAdapter(ReactomeBaseAdapter):

    def __init__(self, data_source):
        ReactomeBaseAdapter.__init__(
            self,
            data_source=data_source,
            file_name="ReactomePathways.gmt.zip",
        )

    def get_all(self) -> Generator[List[Pathway], None, None]:
        pathways: List[Pathway] = []
        for line in self._iter_gmt_lines():
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2:
                continue
            name = parts[0].strip()
            reactome_id = parts[1].strip()
            if not reactome_id.startswith("R-HSA-"):
                continue
            pathways.append(
                Pathway(
                    id=reactome_id,
                    source_id=reactome_id,
                    type="Reactome",
                    name=name,
                    url=f"https://reactome.org/content/detail/{reactome_id}"
                )
            )
        yield pathways

    def _iter_gmt_lines(self):
        if self.file_path.endswith(".zip"):
            with zipfile.ZipFile(self.file_path) as zf:
                gmt_names = [name for name in zf.namelist() if name.endswith(".gmt")]
                if not gmt_names:
                    raise ValueError(f"No .gmt file found in {self.file_path}")
                with zf.open(gmt_names[0]) as handle:
                    for line in io.TextIOWrapper(handle, encoding="utf-8"):
                        yield line
        else:
            with open(self.file_path, "r") as handle:
                for line in handle:
                    yield line

class ReactomePathwayParentEdgeAdapter(ReactomeBaseAdapter):

    def __init__(self, data_source):
        ReactomeBaseAdapter.__init__(
            self,
            data_source=data_source,
            file_name="ReactomePathwaysRelation.txt",
        )

    def get_all(self) -> Generator[List[PathwayParentEdge], None, None]:
        edges: List[PathwayParentEdge] = []
        with open(self.file_path, "r") as handle:
            for line in handle:
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 2:
                    continue
                parent_id = parts[0].strip()
                child_id = parts[1].strip()
                if not parent_id.startswith("R-HSA-") or not child_id.startswith("R-HSA-"):
                    continue
                edges.append(
                    PathwayParentEdge(
                        start_node=Pathway(id=parent_id),
                        end_node=Pathway(id=child_id),
                        source="Reactome"
                    )
                )
        yield edges


class ReactomeProteinPathwayEdgeAdapter(ReactomeBaseAdapter):

    def __init__(self, data_source):
        ReactomeBaseAdapter.__init__(
            self,
            data_source=data_source,
            file_name="UniProt2Reactome_All_Levels.txt",
        )

    def get_all(self) -> Generator[List[ProteinPathwayEdge], None, None]:
        edges: List[ProteinPathwayEdge] = []
        with open(self.file_path, "r") as handle:
            for line in handle:
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 6:
                    continue
                uniprot_id = parts[0].strip()
                pathway_id = parts[1].strip()
                species = parts[5].strip()
                if not pathway_id.startswith("R-HSA-"):
                    continue
                if "Homo sapiens" not in species and "9606" not in species:
                    continue
                protein_id = EquivalentId(id=uniprot_id, type=Prefix.UniProtKB)
                edges.append(
                    ProteinPathwayEdge(
                        start_node=Protein(id=protein_id.id_str()),
                        end_node=Pathway(id=pathway_id),
                        source="Reactome"
                    )
                )
        yield edges
