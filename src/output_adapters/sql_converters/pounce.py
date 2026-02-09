from typing import Union, List
from src.models.gene import Gene
from src.models.go_term import ProteinGoTermRelationship
from src.models.pounce.biosample import Biosample
from src.models.pounce.biospecimen import BioSpecimen
from src.models.pounce.exposure import Exposure, BiosampleExposureEdge
from src.models.pounce.project import Project, Person, ProjectPersonEdge
from src.models.protein import Protein
from src.output_adapters.sql_converters.output_converter_base import SQLOutputConverter
from src.shared.sqlalchemy_tables.pounce.Base import Base as PounceBase
from src.shared.sqlalchemy_tables.pounce.BaseGeneAnnotation import BaseGeneAnnotation
from src.shared.sqlalchemy_tables.pounce.BaseProteinAnnotation import BaseProteinAnnotation
from src.shared.sqlalchemy_tables.pounce.GoAssociation import GoAssociation
from src.shared.sqlalchemy_tables.pounce.Investigator import Investigator
from src.shared.sqlalchemy_tables.pounce.Project import Project as mysqlProject
from src.shared.sqlalchemy_tables.pounce.Biosample import Biosample as mysqlBiosample
from src.shared.sqlalchemy_tables.pounce.Exposure import Exposure as mysqlExposure, ExposureNames, BiosampleExposure
from src.shared.sqlalchemy_tables.pounce.Demographics import Demographics as mysqlDemographics
from src.shared.sqlalchemy_tables.pounce.Biospecimen import Biospecimen as mysqlBiospecimen, BiospecimenDisease
from src.shared.sqlalchemy_tables.pounce.ProjectInvestigator import ProjectInvestigator
from src.shared.sqlalchemy_tables.pounce.ProjectTypes import ProjectTypes, ProjectGroups, ProjectKeywords


class PounceOutputConverter(SQLOutputConverter):

    def get_preload_queries(self, session):
        return [
            {
                "table": 'investigator',
                "data": session.query(Investigator.id, Investigator.name).all()
            },
            {
                "table": "biosample",
                "data": session.query(
                    mysqlBiosample.id,
                    mysqlBiosample.project_id,
                    mysqlBiospecimen.original_id,
                    mysqlBiosample.original_id)
                .where(mysqlBiosample.biospecimen_id == mysqlBiospecimen.id)
                .all(),
                "id_format_function": lambda row : f"{Project.display_id_from_id(row[1])}-{row[3]}-{row[2]}"
            },
            {
                "table": 'biospecimen',
                "data": session.query(mysqlBiospecimen.id, mysqlBiospecimen.project_id, mysqlBiospecimen.original_id).all(),
                "id_format_function": lambda row : f"{Project.display_id_from_id(row[1])}-{row[2]}"
            }
        ]

    def get_object_converters(self, obj_cls) -> Union[callable, List[callable], None]:
        if obj_cls == Protein:
            return self.protein_converter
        if obj_cls == ProteinGoTermRelationship:
            return self.go_association_converter
        if obj_cls == Gene:
            return self.gene_converter

        if obj_cls == Project:
            return self.project_converter
        if obj_cls == Person:
            return self.person_converter
        if obj_cls == ProjectPersonEdge:
            return self.proj_person_converter

        if obj_cls == Biosample:
            return [self.biosample_converter, self.demographics_converter]
        if obj_cls == BioSpecimen:
            return [self.biospecimen_converter, self.biospecimen_disease_converter]

        if obj_cls == Exposure:
            return [self.exposure_converter, self.exposure_names_converter]
        if obj_cls == BiosampleExposureEdge:
            return self.biosample_exposure_converter

        return None

    def project_converter(self, obj: dict) -> List[Union[mysqlProject, ProjectTypes]]:
        project_id = Project.id_from_display_id(obj["id"])
        proj = mysqlProject(
            id=project_id,
            project_display_id=obj["id"],
            name=obj["name"],
            description=obj["description"],
            rare_disease_focus=obj["rare_disease_focus"],
            start_date=obj.get("date").strftime("%Y%m%d"),
            privacy_level=obj['access'],
            biosample_preparation=obj['sample_preparation'],
            provenance=obj['provenance'],
        )

        project_types = [ProjectTypes(
            project_type=type,
            project_id=project_id,
            provenance=obj['provenance'],
        ) for type in obj["project_type"]]

        project_groups = [ProjectGroups(
            name=group,
            project_id=project_id,
            provenance=obj['provenance'],
        ) for group in obj["lab_groups"]]

        project_keywords = [ProjectKeywords(
            keyword=keyword,
            project_id=project_id,
            provenance=obj['provenance'],
        ) for keyword in obj["keywords"]]

        return [proj, *project_types, *project_groups, *project_keywords]

    def person_converter(self, obj: dict):
        return Investigator(
            id=self.resolve_id('investigator', obj.get("id")),
            name=obj.get("id"),
            email=obj.get("email"),
            provenance=obj['provenance'])

    def proj_person_converter(self, obj: dict) -> List[Union[Investigator, ProjectInvestigator]]:
        project_id = obj.get('start_node').get('id')
        person_id = obj.get('end_node').get('id')
        return ProjectInvestigator(
            project_id=Project.id_from_display_id(project_id),
            investigator_id=self.resolve_id('investigator', person_id),
            role=obj.get('role'),
            provenance=obj['provenance']
        )

    def biosample_exposure_converter(self, obj: dict) -> BiosampleExposure:
        sample_id = self.resolve_id('biosample', obj.get("start_id"))
        exposure_id = self.resolve_id('exposure', obj.get("end_id"))
        return BiosampleExposure(
            biosample_id=sample_id,
            exposure_id=exposure_id,
            provenance=obj['provenance']
        )

    def exposure_converter(self, obj: dict) -> mysqlExposure:
        exposure_id = self.resolve_id('exposure', obj.get("id"))
        return mysqlExposure(
            id = exposure_id,
            type=obj.get("type"),
            category = (obj.get('category', {}) or {}).get('name'),
            category_value = (obj.get('category', {}) or {}).get('value'),
            concentration = obj.get('concentration'),
            concentration_unit = obj.get('concentration_unit'),
            duration = obj.get('duration'),
            duration_unit = obj.get('duration_unit'),
            start_time = obj.get('start_time'),
            end_time = obj.get('end_time'),
            growth_media = obj.get('growth_media'),
            condition_category = (obj.get('condition_category', {}) or {}).get('name'),
            condition_category_value = (obj.get('condition_category', {}) or {}).get('value'),
            provenance=obj['provenance']
        )

    def exposure_names_converter(self, obj: dict) -> Union[None, List[str]]:
        exposure_id = self.resolve_id('exposure', obj.get("id"))
        names = obj.get('names')
        if names is None or len(names) == 0:
            return None
        return [ExposureNames(exposure_id=exposure_id, name=name) for name in names]

    def demographics_converter(self, obj: dict):
        biosample_id = self.resolve_id('biosample', obj.get("id")),
        demographics_obj = obj.get('demographics')
        if demographics_obj is None:
            return None
        if (demographics_obj.get('age') is None
                and demographics_obj.get('race') is None
                and demographics_obj.get('ethnicity') is None
                and demographics_obj.get('sex') is None
                and demographics_obj.get('category') is None):
            return None
        return mysqlDemographics(
            biosample_id = biosample_id,
            age = demographics_obj.get('age'),
            race = demographics_obj.get('race'),
            ethnicity = demographics_obj.get('ethnicity'),
            sex = demographics_obj.get('sex'),
            category = (demographics_obj.get('category', {}) or {}).get('name'),
            category_value = (demographics_obj.get('category', {}) or {}).get('value'),
        )

    def biospecimen_disease_converter(self, obj: dict) -> List[BiospecimenDisease]:
        biospecimen_id = self.resolve_id('biospecimen', obj.get("id"))
        if 'diseases' not in obj or obj.get('diseases') is None:
            return None
        return [BiospecimenDisease(
            biospecimen_id = biospecimen_id,
            disease=disease
        ) for disease in obj.get('diseases', [])]

    def biospecimen_converter(self, obj: dict) -> mysqlBiospecimen:
        proj_id, orig_biospecimen_id = obj["id"].split('-')
        biospecimen_id = self.resolve_id('biospecimen', obj.get("id")),
        return mysqlBiospecimen(
            id = biospecimen_id,
            original_id=orig_biospecimen_id,
            project_id=Project.id_from_display_id(proj_id),
            type=obj["type"],
            description=obj["description"],
            organism=obj["organism"],
            organism_category=(obj.get('organism_category', {}) or {}).get('name'),
            organism_category_value=(obj.get('organism_category', {}) or {}).get('value'),
            disease_category=(obj.get('disease_category', {}) or {}).get('name'),
            disease_category_value=(obj.get('disease_category', {}) or {}).get('value'),
            phenotype_category=(obj.get('phenotype_category', {}) or {}).get('name'),
            phenotype_category_value=(obj.get('phenotype_category', {}) or {}).get('value'),
            provenance=obj['provenance']
        )

    def biosample_converter(self, obj: dict) -> mysqlBiosample:
        proj_id, orig_biosample_id, orig_biospecimen_id = obj["id"].split('-')
        biosample_id = self.resolve_id('biosample', obj.get("id")),
        biospecimen_id = self.resolve_id('biospecimen', f"{proj_id}-{orig_biospecimen_id}")
        return mysqlBiosample(
            id = biosample_id,
            project_id=Project.id_from_display_id(proj_id),
            biospecimen_id=biospecimen_id,
            original_id=orig_biosample_id,
            type=obj["type"],
            provenance=obj['provenance']
        )



    def gene_converter(self, obj: dict) -> BaseGeneAnnotation:
        base_annotation = BaseGeneAnnotation(
            ensembl_gene_id=obj.get('id'),
            gene_ext_id=obj.get('symbol'),
            hgnc_gene_symbol=obj.get('symbol'),
            chromosome=obj.get('chromosome'),
            start_pos=obj.get('start_position'),
            end_pos=obj.get('end_position'),
            strand=obj.get('chromosome_strand'),
            gene_biotype=obj.get('biotype'),
            provenance=obj['provenance'],
        )
        return base_annotation

    def go_association_converter(self, obj: dict):
        go_associations = []
        uniprot_acc = obj.get('start_node').get('uniprot_id')
        hgnc_gene_symbol = obj.get('start_node').get('symbol')
        go_category = obj.get('end_node').get('type')
        go_id = obj.get('end_node').get('id')
        go_term = obj.get('end_node').get('term')
        evidence_code_source = ";".join(
            [f"{e.get('abbreviation')}:{e.get('assigned_by')}" for e in obj.get('evidence')])
        provenance = obj['provenance']
        go_associations.append(GoAssociation(
            uniprot_acc=uniprot_acc,
            is_primary_acc=1,
            hgnc_gene_symbol=hgnc_gene_symbol,
            go_category=go_category,
            go_id=go_id,
            go_term=go_term,
            evidence_code_source=evidence_code_source,
            provenance=provenance,
        ))
        secondary_accessions = obj.get('start_node').get('secondary_uniprot_ids', []) or []
        for secondary_accession in secondary_accessions:
            go_associations.append(GoAssociation(
                uniprot_acc=secondary_accession,
                is_primary_acc=0,
                hgnc_gene_symbol=hgnc_gene_symbol,
                go_category=go_category,
                go_id=go_id,
                go_term=go_term,
                evidence_code_source=evidence_code_source,
                provenance=provenance,
            ))
        return go_associations

    def protein_converter(self, obj: dict) -> Union[BaseProteinAnnotation, List[BaseProteinAnnotation]]:
        proteins = [BaseProteinAnnotation(
            uniprot_acc=obj.get('uniprot_id'),
            is_primary_acc=1,
            uniprot_protein_id=obj.get('gene_name'),
            protein_name=obj.get('name'),
            gene_name=obj.get('symbol'),
            hgnc_gene_symbol=obj.get('symbol'),
            provenance=obj['provenance'],
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
                    provenance=obj['provenance'],
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
                    provenance=obj['provenance'],
                ))

        return proteins

    def __init__(self):
        super().__init__(sql_base=PounceBase)
