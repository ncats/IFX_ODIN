from typing import List, Union, Generator
from src.constants import Prefix, DataSourceName, TARGET_GRAPH_VERSION
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.gene import Gene
from src.models.node import Node, Relationship, EquivalentId
from src.models.protein import Protein
from src.models.transcript import TranscriptProteinEdge, Transcript, GeneProteinEdge, \
    IsoformProteinEdge

from src.shared.targetgraph_parser import TargetGraphProteinParser


class TGProteinFileBase(TargetGraphProteinParser):
    @staticmethod
    def build_protein_obj(line):
        id = TargetGraphProteinParser.get_id(line)
        protein_obj = Protein(id=id)
        protein_obj.uniprot_reviewed = TargetGraphProteinParser.get_uniprot_reviewed(line)
        protein_obj.created = TargetGraphProteinParser.get_creation_date(line)
        protein_obj.updated = TargetGraphProteinParser.get_updated_time(line)
        protein_obj.name = TargetGraphProteinParser.get_name(line)
        # protein_obj.symbol = TargetGraphProteinParser.get_symbol(line)  # don't use this, it's sometimes a multi-valued symbol
        protein_obj.ensembl_id = TargetGraphProteinParser.get_ensembl_id(line)
        protein_obj.refseq_id = TargetGraphProteinParser.get_refseq_id(line)

        protein_obj.uniprot_id = TargetGraphProteinParser.get_uniprot_id(line)
        protein_obj.uniprot_annotationScore = TargetGraphProteinParser.get_uniprot_annotationScore(line)

        protein_obj.protein_name_score = line.get('protein_name_score', None)
        protein_obj.protein_name_method = line.get('protein_name_method', None)
        protein_obj.Ensembl_ID_Provenance = line.get('Ensembl_ID_Provenance', None)
        protein_obj.RefSeq_ID_Provenance = line.get('RefSeq_ID_Provenance', None)
        protein_obj.Uniprot_ID_Provenance = line.get('UniProt_ID_Provenance', None)
        protein_obj.uniprot_isoform = line.get('uniprot_isoform', None)
        protein_obj.ensembl_canonical = line.get('ensembl_canonical', None)
        protein_obj.ncbi_id = TargetGraphProteinParser.get_gene_id(line)
        protein_obj.uniprot_canonical = TargetGraphProteinParser.get_boolean_or_none(line, 'uniprot_canonical')
        protein_obj.uniprot_entryType = TargetGraphProteinParser.get_uniprot_entryType(line)
        protein_obj.mapping_ratio = TargetGraphProteinParser.get_mapping_ratio(line)
        return protein_obj

    def get_all_combined(self, reviewed_only = False, collapse_reviewed_targets = False):
        protein_list = []
        transcript_relationships = []
        gene_relationships = []
        isoform_relationships = []
        rows = list(self.all_rows())
        if reviewed_only and collapse_reviewed_targets:
            for group in self.build_reviewed_representative_groups(rows):
                representative_row = group["representative_row"]
                protein_obj = self.build_protein_obj(representative_row)
                protein_list.append(protein_obj)

                transcript_ids = set()
                gene_ids = set()
                for line in group["group_rows"]:
                    transcript_ids.update(TargetGraphProteinParser.get_transcript_ids(line))
                    gene_ncbi_id = TargetGraphProteinParser.get_gene_id(line)
                    if gene_ncbi_id:
                        gene_ids.add(gene_ncbi_id)

                for transcript_ensembl_id in transcript_ids:
                    transcript_id = EquivalentId(id=transcript_ensembl_id, type=Prefix.ENSEMBL)
                    transcript_relationships.append(
                        TranscriptProteinEdge(
                            start_node=Transcript(id=transcript_id.id_str()),
                            end_node=protein_obj,
                            created=protein_obj.created,
                            updated=protein_obj.updated
                        )
                    )

                for gene_ncbi_id in gene_ids:
                    gene_id = EquivalentId(id=gene_ncbi_id, type=Prefix.NCBIGene)
                    gene_relationships.append(
                        GeneProteinEdge(
                            start_node=Gene(id=gene_id.id_str()),
                            end_node=protein_obj,
                            created=protein_obj.created,
                            updated=protein_obj.updated
                        )
                    )
        else:
            for line in rows:
                protein_obj = self.build_protein_obj(line)
                if reviewed_only and not protein_obj.uniprot_reviewed:
                    continue

                transcript_ensembl_ids = TargetGraphProteinParser.get_transcript_ids(line)

                for transcript_ensembl_id in transcript_ensembl_ids:
                    transcript_id = EquivalentId(id=transcript_ensembl_id, type=Prefix.ENSEMBL)

                    transcript_relationships.append(
                        TranscriptProteinEdge(
                            start_node=Transcript(id=transcript_id.id_str()),
                            end_node=protein_obj,
                            created=protein_obj.created,
                            updated=protein_obj.updated
                        )
                    )

                gene_ncbi_id = TargetGraphProteinParser.get_gene_id(line)
                gene_id = EquivalentId(id=gene_ncbi_id, type=Prefix.NCBIGene)

                gene_relationships.append(
                    GeneProteinEdge(
                        start_node=Gene(id=gene_id.id_str()),
                        end_node=protein_obj,
                        created=protein_obj.created,
                        updated=protein_obj.updated
                    )
                )
                protein_list.append(protein_obj)

                canonical_id = TargetGraphProteinParser.get_isoform_id(line)
                if canonical_id is not None and canonical_id.startswith('IFXProtein:'):
                    canonical_protein = Protein(id=canonical_id)
                    isoform_relationships.append(
                        IsoformProteinEdge(
                            start_node=protein_obj,
                            end_node=canonical_protein,
                            created=protein_obj.created,
                            updated=protein_obj.updated
                        )
                    )

        return protein_list, transcript_relationships, gene_relationships, isoform_relationships


class ProteinNodeAdapter(InputAdapter, TGProteinFileBase):
    reviewed_only: bool
    collapse_reviewed_targets: bool

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.TargetGraph

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version=TARGET_GRAPH_VERSION,
            download_date=self.download_date
        )

    def __init__(self, file_path: str, additional_id_file_path: str = None,
                 reviewed_only = False, collapse_reviewed_targets = False):
        TGProteinFileBase.__init__(self, file_path=file_path, additional_id_file_path=additional_id_file_path)
        self.collapse_reviewed_targets = collapse_reviewed_targets
        self.reviewed_only = reviewed_only or collapse_reviewed_targets

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        protein_list, _, _, _ = self.get_all_combined(self.reviewed_only, self.collapse_reviewed_targets)
        yield protein_list


class ProteinEdgeAdapter(InputAdapter, TGProteinFileBase):

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.TargetGraph

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version=TARGET_GRAPH_VERSION,
            download_date=self.download_date
        )



class TranscriptProteinEdgeAdapter(ProteinEdgeAdapter):

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        _, transcript_relationships, _, _ = self.get_all_combined()
        yield transcript_relationships


class GeneProteinEdgeAdapter(ProteinEdgeAdapter):

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        _, _, gene_relationships, _ = self.get_all_combined()
        yield gene_relationships


class IsoformProteinEdgeAdapter(ProteinEdgeAdapter):

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        _, _, _, isoform_relationships = self.get_all_combined()
        yield isoform_relationships
