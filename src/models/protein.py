from dataclasses import dataclass

from src.interfaces.simple_enum import SimpleEnum
from src.models.analyte import Analyte
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

    @staticmethod
    def parse(tcrd_value: str):
        if tcrd_value is None or tcrd_value == '':
            return IDGFamily.Other
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
class Protein(Analyte):
    protein_type: str = None
    description: str = None
    sequence: str = None
    symbol: str = None
    gene_name: str = None
    tdl: TDL = None
    name: str = None
    idg_family: IDGFamily = None
    antibody_count: int = None
    pm_score: float = None


@dataclass
class ProteinReactionRelationship(Relationship):
    start_node: Protein
    end_node: Reaction
    is_reviewed: bool = None
