from dataclasses import dataclass
from enum import Enum
from typing import Union, Optional

from src.core.decorators import facets
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
@facets(
    category_fields=["protein_type", "tdl", "idg_family", "uniprot_reviewed", "uniprot_canonical"],
    numeric_fields=["pm_score", "antibody_count"])
class Protein(Audited, Analyte):
    protein_type: Optional[str] = None
    description: Optional[str] = None
    symbol: Optional[str] = None
    ensembl_id: Optional[str] = None
    refseq_id: Optional[str] = None
    uniprot_id: Optional[str] = None
    sequence: Optional[str] = None
    gene_name: Optional[str] = None
    tdl: Optional[TDL] = None
    name: Optional[str] = None
    idg_family: Optional[IDGFamily] = None
    antibody_count: Optional[int] = None
    pm_score: Optional[float] = None
    uniprot_annotationScore: Optional[int] = None
    uniprot_reviewed: Optional[bool] = None
    uniprot_function: Optional[str] = None
    Ensembl_ID_Provenance: Optional[str] = None
    RefSeq_ID_Provenance: Optional[str] = None
    Uniprot_ID_Provenance: Optional[str] = None
    ensembl_canonical: Optional[str] = None
    uniprot_canonical: Optional[bool] = None
    uniprot_entryType: Optional[str] = None
    mapping_ratio: Optional[float] = None
    protein_name_score: Optional[str] = None
    protein_name_method: Optional[str] = None
    uniprot_isoform: Optional[str] = None


@dataclass
class ProteinReactionRelationship(Relationship):
    start_node: Protein
    end_node: Reaction
    is_reviewed: bool = None
