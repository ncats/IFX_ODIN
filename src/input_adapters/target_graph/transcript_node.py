from typing import List, Generator
from src.constants import DataSourceName, TARGET_GRAPH_VERSION
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.transcript import Transcript
from src.shared.targetgraph_parser import TargetGraphTranscriptParser


class TranscriptNodeAdapter(InputAdapter, TargetGraphTranscriptParser):

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.TargetGraph

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version=TARGET_GRAPH_VERSION,
            download_date=self.download_date
        )

    def get_all(self) -> Generator[List[Transcript], None, None]:
        transcript_list = []

        for line in self.all_rows():
            id = TargetGraphTranscriptParser.get_id(line)
            transcript_obj = Transcript(id=id)
            transcript_obj.created = TargetGraphTranscriptParser.get_creation_date(line)
            transcript_obj.updated = TargetGraphTranscriptParser.get_updated_time(line)

            transcript_obj.location = TargetGraphTranscriptParser.get_transcript_location(line)
            transcript_obj.type = TargetGraphTranscriptParser.get_transcript_type(line)
            transcript_obj.support_level = TargetGraphTranscriptParser.get_transcript_support_level(line)
            transcript_obj.is_canonical = TargetGraphTranscriptParser.get_transcript_is_canonical(line)
            transcript_obj.MANE_select = TargetGraphTranscriptParser.get_mane_select(line)
            transcript_obj.status = TargetGraphTranscriptParser.get_transcript_status(line)
            transcript_obj.ensembl_version = TargetGraphTranscriptParser.get_transcript_version(line)
            transcript_obj.Ensembl_Transcript_ID_Provenance = line.get('Ensembl_Transcript_ID_Provenance', None)
            transcript_obj.RefSeq_Provenance = line.get('RefSeq_Provenance', None)

            transcript_list.append(transcript_obj)

        yield transcript_list
