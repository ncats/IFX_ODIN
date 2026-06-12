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
        protein_obj.is_canonical = TargetGraphProteinParser.get_is_canonical(line)
        protein_obj.uniprot_canonical = protein_obj.is_canonical
        protein_obj.canonical_isoform_status = TargetGraphProteinParser.get_canonical_isoform_status(line)
        protein_obj.uniprot_entryType = TargetGraphProteinParser.get_uniprot_entryType(line)
        protein_obj.mapping_ratio = TargetGraphProteinParser.get_mapping_ratio(line)
        return protein_obj

    def get_all_combined(self, reviewed_only = False, canonical_only = False):
        protein_list = []
        transcript_relationships = []
        gene_relationships = []
        isoform_relationships = []
        for line in self.all_rows():
            protein_obj = self.build_protein_obj(line)
            if reviewed_only and not protein_obj.uniprot_reviewed:
                continue
            if canonical_only and protein_obj.is_canonical is not True:
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
            if gene_ncbi_id:
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
    canonical_only: bool

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.TargetGraph

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def __init__(self, file_path: str = None, additional_id_file_path: str = None,
                 data_source=None, additional_ids_data_source=None,
                 reviewed_only = False, canonical_only = False):
        self.version_info = (
            data_source.version_info() if data_source is not None
            else DatasourceVersionInfo(version=TARGET_GRAPH_VERSION)
        )
        if data_source is not None:
            file_path = str(data_source.file("protein_ids.tsv"))
        if additional_ids_data_source is not None:
            additional_id_file_path = str(additional_ids_data_source.file("uniprotkb_mapping_20260507.csv"))
        if file_path is None:
            raise ValueError("ProteinNodeAdapter requires file_path or data_source")
        TGProteinFileBase.__init__(self, file_path=file_path, additional_id_file_path=additional_id_file_path)
        self.canonical_only = canonical_only
        self.reviewed_only = reviewed_only

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        protein_list, _, _, _ = self.get_all_combined(self.reviewed_only, self.canonical_only)
        yield protein_list


class ProteinEdgeAdapter(InputAdapter, TGProteinFileBase):

    def __init__(self, file_path: str = None, additional_id_file_path: str = None,
                 data_source=None, additional_ids_data_source=None):
        self.version_info = (
            data_source.version_info() if data_source is not None
            else DatasourceVersionInfo(version=TARGET_GRAPH_VERSION)
        )
        if data_source is not None:
            file_path = str(data_source.file("protein_ids.tsv"))
        if additional_ids_data_source is not None:
            additional_id_file_path = str(additional_ids_data_source.file("uniprotkb_mapping_20260507.csv"))
        if file_path is None:
            raise ValueError(f"{self.__class__.__name__} requires file_path or data_source")
        TGProteinFileBase.__init__(self, file_path=file_path, additional_id_file_path=additional_id_file_path)

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.TargetGraph

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info



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
