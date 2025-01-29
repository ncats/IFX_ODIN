from typing import List
from src.interfaces.input_adapter import NodeInputAdapter
from src.models.gene import Gene
from src.models.node import Node
from src.shared.targetgraph_parser import TargetGraphGeneParser


class GeneNodeAdapter(NodeInputAdapter, TargetGraphGeneParser):
    name = "TargetGraph Gene Adapter"

    def get_all(self) -> List[Node]:
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
        return gene_list

    def get_audit_trail_entries(self, obj: Gene) -> List[str]:
        prov_list = [f"Node Created based on TargetGraph csv file, last updated: {obj.updated}",
                     f"ID concordance index: {round(obj.mapping_ratio, 2)}"]
        return prov_list
