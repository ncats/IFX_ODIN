from typing import Union, List
from src.constants import Prefix
from src.input_adapters.sql_adapter import MySqlAdapter
from src.interfaces.input_adapter import NodeInputAdapter
from src.input_adapters.mysql_pharos.tables import GeneRif as mysql_generif, GeneRif2Pubmed as mysql_generif2pubmed, \
    Protein as mysql_protein
from src.models.generif import ProteinGeneRifRelationship, GeneRif
from src.models.protein import Protein


class GeneRifAdapter(NodeInputAdapter, MySqlAdapter):
    name = "Pharos Gene RIF Adapter"

    def get_audit_trail_entries(self, obj: Union[GeneRif, ProteinGeneRifRelationship]) -> List[str]:
        if isinstance(obj, GeneRif):
            return [f"Gene RIF from {self.credentials.schema}"]
        return [f"Gene RIF Association from {self.credentials.schema}"]

    def get_all(self):
        rif_results = self.get_session().query(
            mysql_generif.text,
            mysql_generif2pubmed.pubmed_id
        ).join(
            mysql_generif2pubmed, mysql_generif2pubmed.generif_id == mysql_generif.id
        )

        rif_dict = dict()
        for row in rif_results:
            text, pmid = row
            id = str(hash(text))
            if id not in rif_dict:
                new_rif = GeneRif(
                    id = id,
                    text=text
                )
                rif_dict[id] = new_rif
            rif_dict[id].pmids.add(pmid)

        rif_association_results = self.get_session().query(
            mysql_generif.text,
            mysql_generif.gene_id,
            mysql_generif.date,
            mysql_protein.uniprot
        ).join(mysql_protein, mysql_generif.protein_id == mysql_protein.id)

        relationships = []
        for row in rif_association_results:
            text, gene_id, date, uniprot = row
            rif_id = str(hash(text))
            relationships.append(
                ProteinGeneRifRelationship(
                    start_node=Protein(id=f"{Prefix.UniProtKB}:{uniprot}"),
                    end_node= GeneRif(id=rif_id),
                    gene_id=gene_id,
                    date=date
                )
            )

        return [*list(rif_dict.values()), *relationships]
