from typing import List, Generator
from src.constants import Prefix, DataSourceName, TARGET_GRAPH_VERSION
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.gene import Gene
from src.models.generif import GeneGeneRifRelationship, GeneRif
from src.models.node import EquivalentId
from src.shared.targetgraph_parser import TargetGraphGeneRIFParser


class GeneGeneRifEdgeAdapter(InputAdapter, TargetGraphGeneRIFParser):
    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.TargetGraph

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version=TARGET_GRAPH_VERSION,
            download_date=self.download_date
        )

    def get_all(self) -> Generator[List[GeneGeneRifRelationship], None, None]:
        rif_edges: List[GeneGeneRifRelationship] = []
        for line in self.all_rows():
            rif_id = self.get_hash_id(line)
            rif_date = TargetGraphGeneRIFParser.get_generif_update_time(line)
            gene_id = EquivalentId(id = TargetGraphGeneRIFParser.get_generif_gene_id(line), type=Prefix.NCBIGene)

            pmids = TargetGraphGeneRIFParser.get_generif_pmids(line)

            rif_edges.append(
                GeneGeneRifRelationship(
                    start_node=Gene(id=gene_id.id_str()),
                    end_node=GeneRif(id=rif_id),
                    gene_id=int(gene_id.id),
                    date=rif_date,
                    pmids=pmids
                ))
        yield rif_edges
