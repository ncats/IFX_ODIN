from typing import List, Union
from src.constants import Prefix
from src.interfaces.input_adapter import NodeInputAdapter, RelationshipInputAdapter
from src.models.gene import Gene
from src.models.node import Node, Relationship, EquivalentId
from src.models.protein import Protein
from src.models.transcript import TranscriptProteinRelationship, Transcript, GeneProteinRelationship, \
    IsoformProteinRelationship

from src.shared.targetgraph_parser import TargetGraphProteinParser


class TGProteinFileBase(TargetGraphProteinParser):
    def get_all_combined(self):
        protein_list = []
        transcript_relationships = []
        gene_relationships = []
        isoform_relationships = []

        for line in self.all_rows():
            id = TargetGraphProteinParser.get_id(line)
            protein_obj = Protein(id=id)
            protein_obj.created = TargetGraphProteinParser.get_creation_date(line)
            protein_obj.updated = TargetGraphProteinParser.get_updated_time(line)
            protein_obj.name = TargetGraphProteinParser.get_name(line)
            protein_obj.symbol = TargetGraphProteinParser.get_symbol(line)
            protein_obj.ensembl_id = TargetGraphProteinParser.get_ensembl_id(line)
            protein_obj.refseq_id = TargetGraphProteinParser.get_refseq_id(line)
            protein_obj.sequence = TargetGraphProteinParser.get_sequence(line)

            protein_obj.uniprot_id = TargetGraphProteinParser.get_uniprot_id(line)
            protein_obj.uniprot_annotationScore = TargetGraphProteinParser.get_uniprot_annotationScore(line)
            protein_obj.uniprot_function = TargetGraphProteinParser.get_function(line)

            protein_obj.extra_properties = {
                "protein_name_match_score": line.get('match_score', None),
                "name_match_method": line.get('match_method', None),
                "Ensembl_ID_Provenance": line.get('Ensembl_ID_Provenance', None),
                "RefSeq_ID_Provenance": line.get('RefSeq_ID_Provenance', None),
                "Uniprot_ID_Provenance": line.get('UniProt_ID_Provenance', None),
                "uniprot_isoform": line.get('SPARQL_uniprot_isoform', None),
                "ensembl_canonical": TargetGraphProteinParser.get_boolean_or_none(line, 'ensembl_canonical'),
                "uniprot_canonical": TargetGraphProteinParser.get_boolean_or_none(line, 'uniprot_canonical'),
                "uniprot_entryType": TargetGraphProteinParser.get_uniprot_entryType(line),
                "mapping_ratio": TargetGraphProteinParser.get_mapping_ratio(line)
            }

            transcript_ensembl_id = TargetGraphProteinParser.get_transcript_id(line)

            transcript_id = EquivalentId(id=transcript_ensembl_id, type=Prefix.ENSEMBL)

            transcript_relationships.append(
                TranscriptProteinRelationship(
                    start_node=Transcript(id=transcript_id.id_str()),
                    end_node=protein_obj,
                    updated=protein_obj.updated
                )
            )

            gene_ncbi_id = TargetGraphProteinParser.get_gene_id(line)
            gene_id = EquivalentId(id=gene_ncbi_id, type=Prefix.NCBIGene)

            gene_relationships.append(
                GeneProteinRelationship(
                    start_node=Gene(id=gene_id.id_str()),
                    end_node=protein_obj,
                    updated=protein_obj.updated
                )
            )
            protein_list.append(protein_obj)

            canonical_id = TargetGraphProteinParser.get_isoform_id(line)
            if canonical_id is not None and canonical_id.startswith('IFXProtein:'):
                canonical_protein = Protein(id=canonical_id)
                isoform_relationships.append(
                    IsoformProteinRelationship(
                        start_node=protein_obj,
                        end_node=canonical_protein,
                        updated=protein_obj.updated
                    )
                )

        return protein_list, transcript_relationships, gene_relationships, isoform_relationships


class ProteinNodeAdapter(NodeInputAdapter, TGProteinFileBase):
    name = "TargetGraph Protein Adapter"

    def __init__(self, file_path: str, additional_id_file_path: str):
        TGProteinFileBase.__init__(self, file_path=file_path, additional_id_file_path=additional_id_file_path)

    def get_audit_trail_entries(self, obj: Protein) -> List[str]:
        prov_list = []
        prov_list.append(f"Node Created based on TargetGraph csv file, last updated: {obj.updated}")
        return prov_list

    def get_all(self) -> List[Protein]:
        protein_list, _, _, _ = self.get_all_combined()
        return protein_list


class ProteinRelationshipAdapter(RelationshipInputAdapter, TGProteinFileBase):
    def get_audit_trail_entries(self, obj: Union[
        TranscriptProteinRelationship, GeneProteinRelationship, IsoformProteinRelationship]) -> List[str]:
        prov_list = []
        prov_list.append(f"Edge Created based on TargetGraph csv file, last updated: {obj.updated}")
        return prov_list


class TranscriptProteinEdgeAdapter(ProteinRelationshipAdapter):
    name = "TargetGraph Transcript to Protein Edge Adapter"

    def get_all(self) -> List[Union[Node, Relationship]]:
        _, transcript_relationships, _, _ = self.get_all_combined()
        return transcript_relationships


class GeneProteinEdgeAdapter(ProteinRelationshipAdapter):
    name = "TargetGraph Gene to Protein Edge Adapter"

    def get_all(self) -> List[Union[Node, Relationship]]:
        _, _, gene_relationships, _ = self.get_all_combined()
        return gene_relationships


class IsoformProteinEdgeAdapter(ProteinRelationshipAdapter):
    name = "TargetGraph Protein to Protein Isoform Edge Adapter"

    def get_all(self) -> List[Union[Node, Relationship]]:
        _, _, _, isoform_relationships = self.get_all_combined()
        return isoform_relationships
