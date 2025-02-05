from typing import List
from src.constants import DataSourceName, TARGET_GRAPH_VERSION
from src.interfaces.input_adapter import NodeInputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.generif import GeneRif
from src.shared.targetgraph_parser import TargetGraphGeneRIFParser


class GeneRifNodeAdapter(NodeInputAdapter, TargetGraphGeneRIFParser):

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.TargetGraphNCBI

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version=TARGET_GRAPH_VERSION,
            download_date=self.download_date
        )

    def get_all(self) -> List[GeneRif]:
        gene_rifs = []

        for line in self.all_rows():
            text = TargetGraphGeneRIFParser.get_generif_text(line)
            rif_id = str(hash(text))
            pmids = set(TargetGraphGeneRIFParser.get_generif_pmids(line))
            rif_obj = GeneRif(
                id=rif_id,
                text=text,
                pmids=pmids
            )
            rif_date = TargetGraphGeneRIFParser.get_generif_update_time(line)
            gene_rifs.append(rif_obj)

        return gene_rifs
