import re
from abc import abstractmethod, ABC
from datetime import datetime
from typing import Dict, List, Optional

from src.constants import Prefix
from src.models.gene import GeneticLocation, Strand
from src.models.node import EquivalentId
from src.models.transcript import TranscriptLocation
from src.shared.csv_parser import CSVParser


def split_and_trim_str(str, delimiter=","):
    if str is None:
        return None
    return [p.strip() for p in str.split(delimiter)]


def remove_suffix(id_to_use: str, delimiter='-') -> Optional[str]:
    if id_to_use is None:
        return None
    parts = id_to_use.split(delimiter)
    if len(parts) > 2:
        raise Exception(f'possible invalid ID - trying to remove the suffix from this: {id_to_use}')
    return parts[0]


def remove_decimal(id_to_use: str) -> Optional[str]:
    return remove_suffix(id_to_use, '.')


def try_append_id(id_list, prop_dict, id_field, prov_field, source, prefix, removePrefix=False, removeDecimal=False,
                  splitIDs=False, removeVersion=False):
    val = prop_dict.get(id_field, None)
    if val == None:
        return
    cell_ids = [val]
    if splitIDs:
        cell_ids = split_and_trim_str(prop_dict.get(id_field, ''), '|')
    for id in cell_ids:
        if id is not None and len(id) > 0:
            id_to_use = id
            if removePrefix:
                parts = id_to_use.split(':')
                id_to_use = parts[len(parts) - 1]
            if removeDecimal:
                id_to_use = remove_decimal(id_to_use)
            if removeVersion:
                id_to_use = remove_suffix(id_to_use)
            if prov_field is not None and len(prov_field) > 0:
                sources = split_and_trim_str(prov_field)
            elif source is not None and len(source) > 0:
                sources = split_and_trim_str(source)
            else:
                sources = None
            id_list.append(EquivalentId(id=id_to_use, type=prefix, source=sources))


class TargetGraphParser(CSVParser, ABC):

    @staticmethod
    @abstractmethod
    def get_id(prop_dict: Dict) -> str:
        pass

    def get_equivalent_ids(self, prop_dict: Dict) -> List[EquivalentId]:
        pass

    @staticmethod
    def get_creation_date(prop_dict: Dict) -> datetime:
        return TargetGraphParser.parse_excel_date(prop_dict['createdAt'])

    @staticmethod
    def get_updated_time(prop_dict: Dict) -> datetime:
        return TargetGraphParser.parse_excel_date(prop_dict['updatedAt'])

    @staticmethod
    def get_symbol(prop_dict: Dict) -> Optional[str]:
        return prop_dict.get('consolidated_symbol', None)

    @staticmethod
    def get_mapping_ratio(prop_dict: Dict) -> Optional[float]:
        mapping_ratio = prop_dict.get('Total_Mapping_Ratio', None)
        if mapping_ratio is not None and len(mapping_ratio) > 0:
            return float(mapping_ratio)
        return None

    @staticmethod
    def get_uniprot_annotationScore(prop_dict: Dict) -> Optional[int]:
        annotationScore = prop_dict.get('uniprot_annotationScore', None)
        if annotationScore is not None and len(annotationScore) > 0:
            return int(float(annotationScore))
        return None

    @staticmethod
    def get_uniprot_entryType(prop_dict: Dict) -> Optional[str]:
        return prop_dict.get('uniprot_entryType', None)

    @staticmethod
    def get_boolean_or_none(prop_dict: Dict, key: str) -> Optional[bool]:
        val = prop_dict.get(key, None)
        if val is not None and len(val) > 0:
            return True if float(val) == 1 else False
        return None

    @staticmethod
    def get_uniprot_reviewed(prop_dict: Dict) -> bool:
        return TargetGraphParser.get_uniprot_entryType(prop_dict) == "UniProtKB reviewed (Swiss-Prot)"

    @staticmethod
    def get_sequence(prop_dict: Dict) -> Optional[str]:
        return prop_dict.get('uniprot_sequence', None)

    @staticmethod
    def get_function(prop_dict: Dict) -> Optional[str]:
        return prop_dict.get('uniprot_FUNCTION', None)


class TargetGraphGeneParser(TargetGraphParser):

    @staticmethod
    def get_id(prop_dict: Dict) -> str:
        return prop_dict['ncats_gene_id']

    def get_equivalent_ids(self, prop_dict: Dict) -> List[EquivalentId]:

        ids = []
        try_append_id(ids, prop_dict, 'consolidated_gene_id', 'Ensembl_ID_Provenance', None, Prefix.ENSEMBL)
        try_append_id(ids, prop_dict, 'consolidated_hgnc_id', 'HGNC_ID_Provenance', None, Prefix.HGNC, True)
        try_append_id(ids, prop_dict, 'consolidated_NCBI_id', 'NCBI_ID_Provenance', None, Prefix.NCBIGene, False, True)
        try_append_id(ids, prop_dict, 'consolidated_symbol', 'Symbol_Provenance', None, Prefix.Symbol)
        try_append_id(ids, prop_dict, 'ncbi_mim_id', None, 'ncbi', Prefix.OMIM, True)
        try_append_id(ids, prop_dict, 'hgnc_omim_id', None, "hgnc", Prefix.OMIM)
        try_append_id(ids, prop_dict, 'hgnc_vega_id', None, 'hgnc', Prefix.Vega)
        try_append_id(ids, prop_dict, 'ncbi_miR_id', None, 'ncbi', Prefix.miRBase, True)
        try_append_id(ids, prop_dict, 'ncbi_imgt_id', None, 'ncbi', Prefix.IMGT, True)
        try_append_id(ids, prop_dict, 'hgnc_prev_symbol', None, 'hgnc', Prefix.OldSymbol, False, False, True)
        try_append_id(ids, prop_dict, 'hgnc_ccds_id', None, 'hgnc', Prefix.CCDS, False, False, True)
        try_append_id(ids, prop_dict, 'consolidated_synonyms', None, None, Prefix.Synonym, False, False, True)
        try_append_id(ids, prop_dict, 'hgnc_orphanet_id', None, 'hgnc', Prefix.orphanet)
        return ids

    @staticmethod
    def get_pubmed_ids(prop_dict: Dict) -> Optional[List[int]]:
        pubmed_id = prop_dict.get('hgnc_pubmed_id', None)
        if pubmed_id is not None and len(pubmed_id) > 0:
            ids = split_and_trim_str(pubmed_id, '|')
            return [int(id) for id in ids]
        return None

    @staticmethod
    def get_gene_name(prop_dict: Dict) -> Optional[str]:
        return prop_dict.get('consolidated_description', None)

    @staticmethod
    def get_gene_location(prop_dict: Dict) -> Optional[GeneticLocation]:
        def extract_chromosome(location):
            match = re.match(r'(\d+)', location)
            return int(match.group(1)) if match else None

        location = prop_dict.get('consolidated_location', None)
        strand = prop_dict.get('ensembl_strand', None)
        loc = GeneticLocation()
        has_data = False
        if location is not None and len(location) > 0:
            loc.location = location
            loc.chromosome = extract_chromosome(loc.location)
            has_data = True
        if strand is not None and len(strand) > 0:
            loc.strand = Strand.parse(strand)
            has_data = True
        if has_data:
            return loc
        return None

    @staticmethod
    def get_gene_type(prop_dict: Dict) -> Optional[str]:
        return prop_dict.get('consolidated_gene_type', None)


class TargetGraphTranscriptParser(TargetGraphParser):

    @staticmethod
    def get_id(prop_dict: Dict) -> str:
        return prop_dict['ncats_transcript_id']

    def get_equivalent_ids(self, prop_dict: Dict) -> List[EquivalentId]:
        ids = []
        try_append_id(ids, prop_dict, 'ensembl_transcript_name', None, 'ensembl', Prefix.TranscriptSymbol)
        try_append_id(ids, prop_dict, 'ensembl_transcript_id', 'Ensembl_Transcript_ID_Provenance', None, Prefix.ENSEMBL)
        try_append_id(ids, prop_dict, 'ensembl_refseq_NM', 'RefSeq_Provenance', None, Prefix.RefSeq, splitIDs=True)
        try_append_id(ids, prop_dict, 'ensembl_refseq_MANEselect', None, 'ensembl', Prefix.RefSeq, removeDecimal=True)
        try_append_id(ids, prop_dict, 'refseq_rna_id', 'RefSeq_Provenance', None, Prefix.RefSeq, removeDecimal=True)
        return ids

    @staticmethod
    def get_transcript_location(prop_dict: Dict) -> Optional[TranscriptLocation]:
        start = prop_dict.get('ensembl_trans_bp_start', None)
        end = prop_dict.get('ensembl_trans_bp_end', None)
        length = prop_dict.get('ensembl_trans_length', None)
        if start is not None and len(start) > 0:
            loc = TranscriptLocation(start=int(float(start)), end=int(float(end)), length=int(float(length)))
            return loc
        return None

    @staticmethod
    def get_transcript_type(prop_dict: Dict) -> Optional[str]:
        return prop_dict.get('ensembl_transcript_type', None)

    @staticmethod
    def get_transcript_support_level(prop_dict: Dict) -> Optional[str]:
        return prop_dict.get('ensembl_transcript_tsl', None)

    @staticmethod
    def get_transcript_is_canonical(prop_dict: Dict) -> Optional[bool]:
        val = prop_dict.get('ensembl_canonical', None)
        return True if val == 1 else None

    @staticmethod
    def get_mane_select(prop_dict: Dict) -> Optional[str]:
        return prop_dict.get('ensembl_refseq_MANEselect', None)

    @staticmethod
    def get_transcript_status(prop_dict: Dict) -> Optional[str]:
        return prop_dict.get('refseq_status', None)

    @staticmethod
    def get_transcript_version(prop_dict: Dict) -> Optional[str]:
        val = prop_dict.get('ensembl_transcript_id_version', None)
        if val is not None and len(val) > 0:
            pieces = val.split('.')
            if len(pieces) == 2:
                return pieces[1]
        return None

    @staticmethod
    def get_associated_ensg_id(prop_dict: Dict) -> Optional[str]:
        return prop_dict.get('ensembl_gene_id', None)

    @staticmethod
    def get_associated_ncbi_id(prop_dict: Dict) -> Optional[str]:
        return prop_dict.get('refseq_ncbi_id', None)


class TargetGraphAddtlProteinIDParser(CSVParser):
    column_prefix_map = {
        'uniprot_xref_ChEMBL': {"prefix": Prefix.CHEMBL_PROTEIN},
        'uniprot_ccds_id': {"prefix": Prefix.CCDS, "removeDecimal": True},
        'uniprot_xref_ProteomicsDB': {"prefix": Prefix.ProteomicsDB},
        'uniprot_xref_PIR': {"prefix": Prefix.PIR},
        'uniprot_xref_DIP': {"prefix": Prefix.DIP},
        'uniprot_xref_SwissLipids': {"prefix": Prefix.SLP, "removePrefix": True},
        'uniprot_xref_DisProt': {"prefix": Prefix.DisProt},
        'uniprot_xref_IDEAL': {"prefix": Prefix.IDEAL},
        'uniprot_xref_GuidetoPHARMACOLOGY': {"prefix": Prefix.GTOPDB}
    }

    def get_id_list(self, prop_dict: dict):
        ids = []
        for key, id_details in self.column_prefix_map.items():
            try_append_id(ids,
                          prop_dict,
                          key,
                          None,
                          'uniprot',
                          id_details['prefix'],
                          id_details.get('removePrefix', False),
                          id_details.get('removeDecimal', False)
                          )
        return ids

    def get_main_id(self, prop_dict: dict):
        return prop_dict.get('uniprot_id', None)


class TargetGraphProteinParser(TargetGraphParser):
    additional_id_map: Dict[str, list]

    def __init__(self, file_path: str, additional_id_file_path: str = None):
        TargetGraphParser.__init__(self, file_path=file_path)
        self.additional_id_map = {}
        if additional_id_file_path is not None:
            parser = TargetGraphAddtlProteinIDParser(file_path=additional_id_file_path)
            for line in parser.all_rows():
                self.additional_id_map[parser.get_main_id(line)] = parser.get_id_list(line)

    @staticmethod
    def get_id(prop_dict: Dict) -> str:
        return prop_dict['ncats_protein_id']

    @staticmethod
    def get_ensembl_id(prop_dict: Dict) -> Optional[str]:
        return prop_dict.get('consolidated_ensembl_protein_id', None)

    @staticmethod
    def get_refseq_id(prop_dict: Dict) -> Optional[str]:
        return prop_dict.get('consolidated_refseq_protein', None)

    @staticmethod
    def get_uniprot_id(prop_dict: Dict) -> Optional[str]:
        return prop_dict.get('consolidated_uniprot_id', None)

    @staticmethod
    def get_name(prop_dict: Dict) -> Optional[str]:
        return prop_dict.get('combined_protein_name', None)

    def get_equivalent_ids(self, prop_dict: Dict) -> List[EquivalentId]:
        ids = []
        try_append_id(ids, prop_dict, 'SPARQL_uniprot_isoform', None, 'uniprot', Prefix.UniProtKB, removeVersion=True)
        try_append_id(ids, prop_dict, 'consolidated_uniprot_id', 'UniProt_ID_Provenance', None, Prefix.UniProtKB)
        try_append_id(ids, prop_dict, 'consolidated_ensembl_protein_id', 'Ensembl_ID_Provenance', None, Prefix.ENSEMBL,
                      removeDecimal=True)
        try_append_id(ids, prop_dict, 'consolidated_refseq_protein', 'RefSeq_ID_Provenance', None, Prefix.RefSeq,
                      removeDecimal=True, splitIDs=True)
        try_append_id(ids, prop_dict, 'consolidated_symbol', None, None, Prefix.Symbol)
        try_append_id(ids, prop_dict, 'uniprot_secondaryAccessions', None, 'uniprot', Prefix.UniProtKB, splitIDs=True)
        try_append_id(ids, prop_dict, 'uniprot_uniProtkbId', None, 'uniprot', Prefix.UniProtKB)
        try_append_id(ids, prop_dict, 'combined_protein_name', None, 'uniprot', Prefix.Name)
        uniprot_id = prop_dict.get('consolidated_uniprot_id', None)
        extra_ids = self.additional_id_map.get(uniprot_id, None)
        if extra_ids is not None:
            return [*ids, *extra_ids]
        return ids

    @staticmethod
    def get_transcript_id(prop_dict: Dict) -> Optional[str]:
        transcript_id = prop_dict.get('consolidated_ensembl_transcript_id', None)
        return remove_decimal(transcript_id)

    @staticmethod
    def get_gene_id(prop_dict: Dict) -> Optional[str]:
        ncbi_id = prop_dict.get('uniprot_NCBI_id', None)
        return remove_decimal(ncbi_id)

    @staticmethod
    def get_isoform_id(prop_dict: Dict) -> Optional[str]:
        return prop_dict.get('canonical_isoform', None)


class TargetGraphGeneRIFParser(TargetGraphParser):

    @staticmethod
    def get_id(prop_dict: Dict) -> str:
        pass

    def get_equivalent_ids(self, prop_dict: Dict) -> List[EquivalentId]:
        pass

    @staticmethod
    def get_generif_gene_id(prop_dict: Dict) -> str:
        return prop_dict['Gene ID']

    @staticmethod
    def get_generif_update_time(prop_dict: Dict) -> datetime:
        return TargetGraphParser.parse_excel_date(prop_dict['last update timestamp'])

    @staticmethod
    def get_generif_text(prop_dict: Dict) -> Optional[str]:
        return prop_dict.get('GeneRIF text', None)

    @staticmethod
    def get_generif_pmids(prop_dict: Dict) -> List[str]:
        val = prop_dict.get('PubMed ID (PMID) list', '')
        return split_and_trim_str(val, '|')
