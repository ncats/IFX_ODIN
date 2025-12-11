from typing import Union, List

from src.models.protein import Protein
from src.output_adapters.sql_converters.output_converter_base import SQLOutputConverter
from src.shared.sqlalchemy_tables.pounce.Base import Base as PounceBase
from src.shared.sqlalchemy_tables.pounce.BaseProteinAnnotation import BaseProteinAnnotation


class PounceOutputConverter(SQLOutputConverter):
    def get_object_converters(self, obj_cls) -> Union[callable, List[callable], None]:
        if obj_cls == Protein:
            return self.protein_converter
        return None

    def protein_converter(self, obj: dict) -> Union[BaseProteinAnnotation, List[BaseProteinAnnotation]]:
        proteins = [BaseProteinAnnotation(
            uniprot_acc=obj.get('uniprot_id'),
            is_primary_acc=1,
            uniprot_protein_id=obj.get('gene_name'),
            protein_name=obj.get('name'),
            gene_name=obj.get('symbol'),
            hgnc_gene_symbol=obj.get('symbol'),
            provenance = obj['provenance'],
        )]

        isoforms = obj.get('isoforms')
        if isoforms is not None:
            for isoform in isoforms:
                proteins.append(BaseProteinAnnotation(
                    uniprot_acc=isoform.get('id'),
                    is_primary_acc=1,
                    uniprot_protein_id=obj.get('gene_name'),
                    protein_name=f"Isoform {isoform.get('name')} of {obj.get('name')}",
                    gene_name=obj.get('symbol'),
                    hgnc_gene_symbol=obj.get('symbol'),
                    provenance = obj['provenance'],
                ))

        secondary_accessions = obj.get('secondary_uniprot_ids')
        if secondary_accessions is not None:
            for sec_acc in secondary_accessions:
                proteins.append(BaseProteinAnnotation(
                    uniprot_acc=sec_acc,
                    is_primary_acc=0,
                    uniprot_protein_id=obj.get('gene_name'),
                    protein_name=obj.get('name'),
                    gene_name=obj.get('symbol'),
                    hgnc_gene_symbol=obj.get('symbol'),
                    provenance = obj['provenance'],
                ))

        return proteins

    def __init__(self):
        super().__init__(sql_base=PounceBase)