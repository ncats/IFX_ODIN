from typing import List, Generator, Dict
from src.constants import DataSourceName, TARGET_GRAPH_VERSION
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.generif import GeneRif
from src.shared.targetgraph_parser import TargetGraphGeneRIFParser


class GeneRifNodeAdapter(InputAdapter, TargetGraphGeneRIFParser):

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.TargetGraphNCBI

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version=TARGET_GRAPH_VERSION,
            download_date=self.download_date
        )

    def get_all(self) -> Generator[List[GeneRif], None, None]:
        gene_rifs_map: Dict[str, GeneRif] = {}

        for line in self.all_rows():
            text = TargetGraphGeneRIFParser.get_generif_text(line)
            rif_id = str(hash(text))
            pmids = TargetGraphGeneRIFParser.get_generif_pmids(line)
            if rif_id in gene_rifs_map:
                gene_rifs_map[rif_id].pmids.update(pmids)
            else:
                rif_obj = GeneRif(
                    id=rif_id,
                    text=text,
                    pmids=set(pmids)
                )
                gene_rifs_map[rif_id] = rif_obj

        yield list(gene_rifs_map.values())
