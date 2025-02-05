from typing import List
from src.constants import Prefix, DataSourceName, TARGET_GRAPH_VERSION
from src.interfaces.input_adapter import RelationshipInputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.gene import Gene
from src.models.node import EquivalentId
from src.models.transcript import GeneTranscriptRelationship, Transcript
from src.shared.targetgraph_parser import TargetGraphTranscriptParser


class GeneTranscriptEdgeAdapter(RelationshipInputAdapter, TargetGraphTranscriptParser):
    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.TargetGraph

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version=TARGET_GRAPH_VERSION,
            download_date=self.download_date
        )

    def get_all(self) -> List[GeneTranscriptRelationship]:
        relationships = []

        for line in self.all_rows():
            transcript_id = TargetGraphTranscriptParser.get_id(line)
            transcript_obj = Transcript(id=transcript_id)

            created = TargetGraphTranscriptParser.get_creation_date(line)
            updated = TargetGraphTranscriptParser.get_updated_time(line)

            ensg_id = TargetGraphTranscriptParser.get_associated_ensg_id(line)
            ncbi_id = TargetGraphTranscriptParser.get_associated_ncbi_id(line)

            if ensg_id is not None and len(ensg_id) > 0:
                gene_id = EquivalentId(id=ensg_id, type=Prefix.ENSEMBL)
            else:
                if ncbi_id is None or len(ncbi_id) == 0:
                    raise Exception("no associated gene", line)
                gene_id = EquivalentId(id=ncbi_id, type=Prefix.NCBIGene)

            relationships.append(
                GeneTranscriptRelationship(
                    start_node=Gene(id=gene_id.id_str()),
                    end_node=transcript_obj,
                    created=created,
                    updated=updated
                )
            )

        return relationships
