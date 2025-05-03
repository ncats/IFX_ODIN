from dataclasses import dataclass
from enum import Enum
from typing import Union

from src.interfaces.simple_enum import SimpleEnum
from src.models.analyte import Analyte
from src.models.gene import Audited
from src.models.node import Relationship
from src.models.reaction import Reaction


class TDL(SimpleEnum):
    Tclin = 'Tclin'
    Tchem = 'Tchem'
    Tbio = 'Tbio'
    Tdark = 'Tdark'

class IDGFamily(SimpleEnum):
    Other = 'Other'
    Enzyme = 'Enzyme'
    TranscriptionFactor = 'Transcription Factor'
    Kinase = 'Kinase'
    Transporter = 'Transporter'
    oGPCR = 'oGPCR'
    GPCR = 'GPCR'
    IonChannel = 'Ion Channel'
    Epigenetic = 'Epigenetic'
    NuclearReceptor = 'Nuclear Receptor'
    TFEpigenetic = 'TF-Epigenetic'

    @classmethod
    def parse(cls, tcrd_value: Union[str, Enum]):
        if tcrd_value is None or tcrd_value == '':
            return None
        if tcrd_value == 'IC':
            return IDGFamily.IonChannel
        if tcrd_value == 'TF; Epigenetic':
            return IDGFamily.TFEpigenetic
        if tcrd_value == 'TF':
            return IDGFamily.TranscriptionFactor
        if tcrd_value == 'NR':
            return IDGFamily.NuclearReceptor

        for member in IDGFamily:
            if member.value == tcrd_value:
                return member

        raise Exception(f"Unknown IDG Family: {tcrd_value}")

@dataclass
class Protein(Audited, Analyte):
    protein_type: str = None
    description: str = None
    symbol: str = None
    ensembl_id: str = None
    refseq_id: str = None
    uniprot_id: str = None
    sequence: str = None
    gene_name: str = None
    tdl: TDL = None
    name: str = None
    idg_family: IDGFamily = None
    antibody_count: int = None
    pm_score: float = None
    uniprot_annotationScore: int = None
    uniprot_reviewed: bool = None
    uniprot_function: str = None
    protein_name_match_score: str = None
    name_match_method: str = None
    Ensembl_ID_Provenance: str = None
    RefSeq_ID_Provenance: str = None
    Uniprot_ID_Provenance: str = None
    uniprot_isoform: str = None
    ensembl_canonical: bool = None
    uniprot_canonical: bool = None
    uniprot_entryType: str = None
    mapping_ratio: float = None


@dataclass
class ProteinReactionRelationship(Relationship):
    start_node: Protein
    end_node: Reaction
    is_reviewed: bool = None
