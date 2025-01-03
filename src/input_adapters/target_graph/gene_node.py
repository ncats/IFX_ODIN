from typing import List

from src.constants import Prefix
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
            equiv_ids = self.get_equivalent_ids(line)
            gene_obj = Gene(id=id, xref=equiv_ids)
            gene_obj.full_name = TargetGraphGeneParser.get_gene_name(line)
            gene_obj.field_provenance['full_name'] = TargetGraphGeneParser.get_description_provenance(line)
            gene_obj.location = TargetGraphGeneParser.get_gene_location(line)
            gene_obj.field_provenance['location'] = TargetGraphGeneParser.get_location_provenance(line)
            gene_obj.created = TargetGraphGeneParser.get_creation_date(line)
            gene_obj.updated = TargetGraphGeneParser.get_updated_time(line)
            gene_obj.type = TargetGraphGeneParser.get_gene_type(line)
            gene_obj.pubmed_ids = TargetGraphGeneParser.get_pubmed_ids(line)
            gene_obj.mapping_ratio = TargetGraphGeneParser.get_mapping_ratio(line)

            symbol_ids = [s for s in gene_obj.xref if s.type == Prefix.Symbol]
            if len(symbol_ids) > 0:
                gene_obj.symbol = symbol_ids[0].id
            synonyms = [s.id for s in gene_obj.xref if s.type == Prefix.Synonym]
            if len(synonyms) > 0:
                gene_obj.synonym = list(set(synonyms))
            gene_obj.xref = list(set([s.id_str() for s in gene_obj.xref]))

            gene_list.append(gene_obj)
        return gene_list

    def get_audit_trail_entries(self, obj: Gene) -> List[str]:
        prov_list = []
        prov_list.append(f"Node Created based on TargetGraph csv file, last updated: {obj.updated}")
        prov_list.append(f"ID concordance index: {round(obj.mapping_ratio, 2)}")
        prov_list.append(f"full_name is from: {', '.join(obj.field_provenance['full_name'])}")
        prov_list.append(f"location is from: {', '.join(obj.field_provenance['location'])}")

        return prov_list
