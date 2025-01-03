from typing import List, Union

from src.constants import Prefix
from src.interfaces.input_adapter import RelationshipInputAdapter
from src.models.gene import Gene
from src.models.generif import GeneGeneRifRelationship, GeneRif
from src.models.node import EquivalentId
from src.models.transcript import Transcript, GeneTranscriptRelationship
from src.shared.targetgraph_parser import TargetGraphGeneRIFParser


class GeneGeneRifEdgeAdapter(RelationshipInputAdapter, TargetGraphGeneRIFParser):
    name = "TargetGraph Gene to GeneRIF Edge Adapter"

    def get_all(self) -> List[GeneGeneRifRelationship]:
        relationships = []

        for line in self.all_rows():
            text = TargetGraphGeneRIFParser.get_generif_text(line)
            rif_id = str(hash(text))
            rif_date = TargetGraphGeneRIFParser.get_generif_update_time(line)
            gene_id = EquivalentId(id = TargetGraphGeneRIFParser.get_generif_gene_id(line), type=Prefix.NCBIGene)

            relationships.append(
                GeneGeneRifRelationship(
                    start_node=Gene(id=gene_id.id_str()),
                    end_node=GeneRif(id=rif_id),
                    gene_id=int(gene_id.id),
                    date=rif_date
                )
            )

        return relationships

    def get_audit_trail_entries(self, obj: Union[Transcript, GeneTranscriptRelationship]) -> List[str]:
        prov_list = []
        prov_list.append(f"Edge Created based on TargetGraph csv file")
        return prov_list
