from typing import List, Generator, Union

from src.constants import DataSourceName, TARGET_GRAPH_VERSION
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.gene import Gene
from src.models.node import Node, Relationship
from src.shared.targetgraph_parser import TargetGraphGeneParser


class GeneNodeAdapter(InputAdapter, TargetGraphGeneParser):
    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version=TARGET_GRAPH_VERSION,
            download_date=self.download_date
        )

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.TargetGraph

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        gene_list = []
        for line in self.all_rows():
            id = TargetGraphGeneParser.get_id(line)
            gene_obj = Gene(id=id)
            gene_obj.full_name = TargetGraphGeneParser.get_gene_name(line)
            gene_obj.location = TargetGraphGeneParser.get_gene_location(line)
            gene_obj.created = TargetGraphGeneParser.get_creation_date(line)
            gene_obj.updated = TargetGraphGeneParser.get_updated_time(line)
            gene_obj.type = TargetGraphGeneParser.get_gene_type(line)
            gene_obj.pubmed_ids = TargetGraphGeneParser.get_pubmed_ids(line)
            gene_obj.mapping_ratio = TargetGraphGeneParser.get_mapping_ratio(line)
            gene_obj.symbol = TargetGraphGeneParser.get_symbol(line)

            gene_obj.extra_properties = {
                "Name_Provenance": line.get('Description_Provenance', None),
                "Location_Provenance": line.get('Location_Provenance', None),
                "Ensembl_ID_Provenance": line.get('Ensembl_ID_Provenance', None),
                "NCBI_ID_Provenance": line.get('NCBI_ID_Provenance', None),
                "HGNC_ID_Provenance": line.get('HGNC_ID_Provenance', None),
                "Symbol_Provenance": line.get('Symbol_Provenance', None),
            }
            gene_list.append(gene_obj)
        yield gene_list

