from typing import List

from src.interfaces.input_adapter import NodeInputAdapter
from src.models.transcript import Transcript
from src.shared.targetgraph_parser import TargetGraphTranscriptParser


class TranscriptNodeAdapter(NodeInputAdapter, TargetGraphTranscriptParser):
    name = "TargetGraph Transcript Adapter"

    def get_all(self) -> List[Transcript]:
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

            transcript_obj.extra_properties = {
                "Ensembl_Transcript_ID_Provenance": line.get('Ensembl_Transcript_ID_Provenance', None),
                "RefSeq_Provenance": line.get('RefSeq_Provenance', None)
            }

            transcript_list.append(transcript_obj)

        return transcript_list

    def get_audit_trail_entries(self, obj: Transcript) -> List[str]:
        prov_list = []
        prov_list.append(f"Node Created based on TargetGraph csv file, last updated: {obj.updated}")
        return prov_list

