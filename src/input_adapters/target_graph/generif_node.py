from typing import List, Union

from src.interfaces.input_adapter import NodeInputAdapter
from src.models.generif import GeneRif
from src.models.transcript import Transcript, GeneTranscriptRelationship
from src.shared.targetgraph_parser import TargetGraphGeneRIFParser


class GeneRifNodeAdapter(NodeInputAdapter, TargetGraphGeneRIFParser):
    name = "TargetGraph GeneRIF Adapter"

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

    def get_audit_trail_entries(self, obj: Union[Transcript, GeneTranscriptRelationship]) -> List[str]:
        prov_list = []
        prov_list.append(f"Node Created based on TargetGraph csv file")
        return prov_list


