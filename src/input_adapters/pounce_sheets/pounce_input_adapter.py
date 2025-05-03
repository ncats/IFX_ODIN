import os
from datetime import datetime, date
from typing import Generator, List, Union

from src.constants import DataSourceName, Prefix
from src.input_adapters.excel_sheet_adapter import ExcelsheetParser
from src.input_adapters.pounce_sheets.constants import ProjectWorkbook, ExperimentWorkbook
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.gene import Gene
from src.models.metabolite import Metabolite
from src.models.node import Node, Relationship, EquivalentId
from src.models.pounce.data import Sample, ExperimentSampleRelationship, Biospecimen, SampleBiospecimenRelationship, \
    SampleAnalyteRelationship
from src.models.pounce.experiment import Experiment
from src.models.pounce.investigator import Investigator, ProjectInvestigatorRelationship, Role
from src.models.pounce.project import Project, ProjectPrivacy, ProjectType, ProjectTypeRelationship
from src.models.pounce.project_experiment_relationship import ProjectExperimentRelationship
from src.models.protein import Protein


class PounceInputAdapter(InputAdapter):
    experiment_file: str
    project_file: str
    experiment_parser: ExcelsheetParser
    project_parser: ExcelsheetParser

    def __init__(self, experiment_file: str, project_file: str):
        self.experiment_file = experiment_file
        self.project_file = project_file

        self.experiment_parser = ExcelsheetParser(file_path=experiment_file)
        self.project_parser = ExcelsheetParser(file_path=project_file)

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        print(f"reading: {self.project_file}")
        proj_name = self.get_project_name()
        proj_obj = Project(
            id = proj_name,
            name=proj_name,
            description=self.get_project_description(),
            lab_groups=self.get_project_lab_groups(),
            date=self.get_project_date(),
            privacy_level=self.get_project_privacy_level(),
            keywords=self.get_project_keywords(),
            project_sheet=self.project_file,
            experiment_sheet=self.experiment_file
        )
        yield from self.get_other_project_nodes_and_edges(proj_obj)

        expt_name = self.get_experiment_name()
        expt_obj = Experiment(
            id=expt_name,
            name=expt_name,
            type=self.get_experiment_type(),
            category=self.get_experiment_category(),
            description=self.get_experiment_description(),
            design=self.get_experiment_design(),
            run_date=self.get_experiment_date()
        )

        extra_columns = self.project_parser.get_other_properties(
            sheet_name=ProjectWorkbook.ExperimentSheet.name,
            skip_keys=[
                ProjectWorkbook.ExperimentSheet.Key.experiment_name,
                ProjectWorkbook.ExperimentSheet.Key.date,
                ProjectWorkbook.ExperimentSheet.Key.experiment_design,
                ProjectWorkbook.ExperimentSheet.Key.experiment_category,
                ProjectWorkbook.ExperimentSheet.Key.description,
                ProjectWorkbook.ExperimentSheet.Key.platform_type,
                ProjectWorkbook.ExperimentSheet.Key.point_of_contact,
                ProjectWorkbook.ExperimentSheet.Key.point_of_contact_email,
                ProjectWorkbook.ExperimentSheet.Key.lead_data_generator,
                ProjectWorkbook.ExperimentSheet.Key.lead_data_generator_email,
                ProjectWorkbook.ExperimentSheet.Key.lead_informatician,
                ProjectWorkbook.ExperimentSheet.Key.lead_informatician_email
            ])

        for key, val in extra_columns.items():
            if val is None or val == '':
                continue
            setattr(expt_obj, key.replace(' ', '_'), row[val])


        yield [expt_obj, ProjectExperimentRelationship(start_node=proj_obj, end_node=expt_obj)]

        yield from self.get_other_experiment_nodes_and_edges(expt_obj)



    def get_other_experiment_nodes_and_edges(self, expt_obj):

        contact_str = self.project_parser.get_one_string(sheet_name=ProjectWorkbook.ExperimentSheet.name,
                                                         data_key=ProjectWorkbook.ExperimentSheet.Key.point_of_contact)
        contact_email = self.project_parser.get_one_string(sheet_name=ProjectWorkbook.ExperimentSheet.name,
                                                           data_key=ProjectWorkbook.ExperimentSheet.Key.point_of_contact_email)
        contact_obj = Investigator(id=contact_str, email=contact_email)

        yield [contact_obj, ProjectInvestigatorRelationship(start_node=expt_obj, end_node=contact_obj, roles=[Role.Contact])]

        data_gen_str = self.project_parser.get_one_string(sheet_name=ProjectWorkbook.ExperimentSheet.name,
                                                          data_key=ProjectWorkbook.ExperimentSheet.Key.lead_data_generator)
        data_gen_email = self.project_parser.get_one_string(sheet_name=ProjectWorkbook.ExperimentSheet.name,
                                                            data_key=ProjectWorkbook.ExperimentSheet.Key.lead_data_generator_email)
        data_gen_obj = Investigator(id=data_gen_str, email=data_gen_email)

        yield [data_gen_obj, ProjectInvestigatorRelationship(start_node=expt_obj, end_node=data_gen_obj, roles=[Role.DataGenerator])]

        informatician_str = self.project_parser.get_one_string(sheet_name=ProjectWorkbook.ExperimentSheet.name, data_key=ProjectWorkbook.ExperimentSheet.Key.lead_informatician)
        informatician_email = self.project_parser.get_one_string(sheet_name=ProjectWorkbook.ExperimentSheet.name, data_key=ProjectWorkbook.ExperimentSheet.Key.lead_informatician_email)
        informatician_obj = Investigator(id=informatician_str, email=informatician_email)

        yield [informatician_obj, ProjectInvestigatorRelationship(start_node=expt_obj, end_node=informatician_obj, roles=[Role.Informatician])]

        yield from self.get_sample_nodes_and_edges(expt_obj)

    def get_sample_nodes_and_edges(self, expt_obj):
        biospec_map = self.project_parser.get_parameter_map(ProjectWorkbook.BiospecimenSheet.name)

        biospecimen_id_column = biospec_map[ProjectWorkbook.BiospecimenSheet.Key.biospecimen_id]
        biospecimen_name_column = biospec_map[ProjectWorkbook.BiospecimenSheet.Key.biospecimen_name]
        organism_column = biospec_map[ProjectWorkbook.BiospecimenSheet.Key.biospecimen_organism]
        biospecimen_type_column = biospec_map[ProjectWorkbook.BiospecimenSheet.Key.biospecimen_type]
        exposure_type_column = biospec_map[ProjectWorkbook.BiospecimenSheet.Key.exposure_type]
        biospecimen_group_label_column = biospec_map[ProjectWorkbook.BiospecimenSheet.Key.biospecimen_group_label]
        cell_line_column = biospec_map.get(ProjectWorkbook.BiospecimenSheet.Key.cell_line)

        biospecimen_df = self.project_parser.sheet_dfs[ProjectWorkbook.BiospecimenDataSheet.name]

        biospecimen_map = {}

        for index, row in biospecimen_df.iterrows():
            biospecimen_id = row[biospecimen_id_column]
            biospecimen_name = row[biospecimen_name_column]
            organism: str = row[organism_column]
            biospecimen_type = row[biospecimen_type_column]
            exposure_type = row[exposure_type_column]
            biospecimen_group_label = row[biospecimen_group_label_column]
            biospecimen_obj = Biospecimen(
                id=f"{expt_obj.id}_{biospecimen_id}",
                name=str(biospecimen_name),
                type=str(biospecimen_type),
                organism=[organism],
                category=biospecimen_group_label,
                comment=exposure_type
            )
            biospecimen_map[biospecimen_id] = biospecimen_obj
            if cell_line_column is not None:
                biospecimen_obj.cell_line = row[cell_line_column]

            extra_columns = self.project_parser.get_other_properties(
                sheet_name=ProjectWorkbook.BiospecimenSheet.name,
                skip_keys=[
                    ProjectWorkbook.BiospecimenSheet.Key.biospecimen_name,
                    ProjectWorkbook.BiospecimenSheet.Key.biospecimen_id,
                    ProjectWorkbook.BiospecimenSheet.Key.biospecimen_name,
                    ProjectWorkbook.BiospecimenSheet.Key.biospecimen_organism,
                    ProjectWorkbook.BiospecimenSheet.Key.biospecimen_type,
                    ProjectWorkbook.BiospecimenSheet.Key.exposure_type,
                    ProjectWorkbook.BiospecimenSheet.Key.biospecimen_group_label,
                    ProjectWorkbook.BiospecimenSheet.Key.cell_line
                ])

            for key, val in extra_columns.items():
                if val is None or val == '':
                    continue
                setattr(biospecimen_obj, key.replace(' ', '_'), row[val])


        yield biospecimen_map.values()

        print(f"reading: {self.experiment_file}")
        sample_map = self.experiment_parser.get_parameter_map(ExperimentWorkbook.SampleSheet.name)

        sample_id_column = sample_map[ExperimentWorkbook.SampleSheet.Key.sample_id]
        biospecimen_id_link_column = sample_map[ExperimentWorkbook.SampleSheet.Key.biospecimen_id]
        comparison_label_column = sample_map[ExperimentWorkbook.SampleSheet.Key.biospecimen_comparison_label]

        replicate_column = sample_map.get(ExperimentWorkbook.SampleSheet.Key.biological_replicate_number)
        type_column = sample_map.get(ExperimentWorkbook.SampleSheet.Key.sample_type)

        sample_df = self.experiment_parser.sheet_dfs[ExperimentWorkbook.SampleDataSheet.name]

        sample_map = {}
        exp_samp_edges = []
        samp_bio_edges = []

        for index, row in sample_df.iterrows():
            sample_id = row[sample_id_column]
            biospecimen_id = row[biospecimen_id_link_column]
            comparison_label = row[comparison_label_column]
            replicate = None
            type = None

            if replicate_column is not None and replicate_column != '':
                replicate = int(row[replicate_column])
            if type_column is not None:
                type = row[type_column]

            sample_obj_id = f"{expt_obj.id}_{sample_id}"
            sample_obj = Sample(
                id=sample_obj_id,
                name=sample_id,
                description=comparison_label,
                type=type,
                replicate=replicate
            )
            sample_map[sample_id] = sample_obj

            samp_bio_edges.append(
                SampleBiospecimenRelationship(
                    start_node=sample_obj,
                    end_node=biospecimen_map[biospecimen_id]
                )
            )

            exp_samp_edges.append(
                ExperimentSampleRelationship(
                    start_node=expt_obj,
                    end_node=sample_obj
            ))
        yield [*sample_map.values(), *exp_samp_edges, *samp_bio_edges]

        yield from self.get_analytes_and_measurements(sample_map)

    def get_analytes_and_measurements(self, sample_map):
        if ExperimentWorkbook.GeneSheet.name in self.experiment_parser.sheet_dfs and ExperimentWorkbook.GeneDataSheet.name in self.experiment_parser.sheet_dfs:
            yield from self.get_genes_and_measurements(sample_map)
        if ExperimentWorkbook.MetabSheet.name in self.experiment_parser.sheet_dfs and ExperimentWorkbook.MetabDataSheet.name in self.experiment_parser.sheet_dfs:
            yield from self.get_metabolites_and_measurements(sample_map)
        if ExperimentWorkbook.ProteinSheet.name in self.experiment_parser.sheet_dfs and ExperimentWorkbook.ProteinDataSheet.name in self.experiment_parser.sheet_dfs:
            yield from self.get_proteins_and_measurements(sample_map)

    def get_proteins_and_measurements(self, sample_map):
        analyte_map = self.experiment_parser.get_parameter_map(ExperimentWorkbook.ProteinSheet.name)
        analyte_id_column = analyte_map[ExperimentWorkbook.ProteinSheet.Key.protein_id]
        analyte_df = self.experiment_parser.sheet_dfs[ExperimentWorkbook.ProteinDataSheet.name]
        analyte_map = {}
        extra_columns = self.experiment_parser.get_other_properties(
            sheet_name=ExperimentWorkbook.ProteinSheet.name,
            skip_keys=[
                ExperimentWorkbook.ProteinSheet.Key.protein_id
            ])
        for index, row in analyte_df.iterrows():
            analyte_id = row[analyte_id_column]
            obj_id = EquivalentId(id=analyte_id, type=Prefix.UniProtKB)
            analyte_obj = Protein(id=obj_id.id_str())
            for key, val in extra_columns.items():
                if val is None or val == '':
                    continue
                setattr(analyte_obj, key.replace(' ', '_'), row[val])
            analyte_map[analyte_id] = analyte_obj

        yield analyte_map.values()
        yield from self.get_measurements(sample_map, analyte_map, analyte_id_column)

    def get_metabolites_and_measurements(self, sample_map):
        analyte_map = self.experiment_parser.get_parameter_map(ExperimentWorkbook.MetabSheet.name)
        analyte_id_column = analyte_map[ExperimentWorkbook.MetabSheet.Key.metab_id]
        analyte_name_column = analyte_map[ExperimentWorkbook.MetabSheet.Key.metab_name]
        analyte_biotype_column = analyte_map[ExperimentWorkbook.MetabSheet.Key.metab_biotype]
        analyte_identification_level_column = analyte_map[ExperimentWorkbook.MetabSheet.Key.identification_level]

        analyte_df = self.experiment_parser.sheet_dfs[ExperimentWorkbook.MetabDataSheet.name]
        analyte_map = {}
        extra_columns = self.experiment_parser.get_other_properties(
            sheet_name=ExperimentWorkbook.MetabSheet.name,
            skip_keys=[
                ExperimentWorkbook.MetabSheet.Key.metab_id,
                ExperimentWorkbook.MetabSheet.Key.metab_name,
                ExperimentWorkbook.MetabSheet.Key.metab_biotype,
                ExperimentWorkbook.MetabSheet.Key.identification_level
            ])

        for index, row in analyte_df.iterrows():
            analyte_id = row[analyte_id_column]
            analyte_name = row[analyte_name_column]
            analyte_biotype = row.get(analyte_biotype_column)
            analyte_identification_level = row.get(analyte_identification_level_column)
            analyte_obj = Metabolite(
                id=str(analyte_id),
                name=str(analyte_name),
                type=analyte_biotype,
                identification_level=analyte_identification_level
            )
            for key, val in extra_columns.items():
                if val is None or val == '':
                    continue
                setattr(analyte_obj, key.replace(' ', '_'), row[val])
            analyte_map[analyte_id] = analyte_obj

        yield analyte_map.values()
        yield from self.get_measurements(sample_map, analyte_map, analyte_id_column)

    def get_genes_and_measurements(self, sample_map):

        analyte_map = self.experiment_parser.get_parameter_map(ExperimentWorkbook.GeneSheet.name)
        analyte_id_column = analyte_map[ExperimentWorkbook.GeneSheet.Key.ensembl_gene_id]
        symbol_column = analyte_map[ExperimentWorkbook.GeneSheet.Key.hgnc_gene_symbol]

        analyte_df = self.experiment_parser.sheet_dfs[ExperimentWorkbook.GeneDataSheet.name]
        analyte_map = {}

        extra_columns = self.experiment_parser.get_other_properties(
            sheet_name=ExperimentWorkbook.GeneSheet.name,
            skip_keys=[
                ExperimentWorkbook.GeneSheet.Key.ensembl_gene_id,
                ExperimentWorkbook.GeneSheet.Key.hgnc_gene_symbol
            ])

        for index, row in analyte_df.iterrows():
            analyte_id = row[analyte_id_column]
            equiv_id = EquivalentId(id = analyte_id, type = Prefix.ENSEMBL)
            symbol = row[symbol_column]
            analyte_obj = Gene(
                id = equiv_id.id_str(),
                symbol = symbol
            )
            for key, val in extra_columns.items():
                if val is None or val == '':
                    continue
                setattr(analyte_obj, key.replace(' ', '_'), row[val])
            analyte_map[analyte_id] = analyte_obj
        yield analyte_map.values()

        yield from self.get_measurements(sample_map, analyte_map, analyte_id_column)

    def get_measurements(self, sample_map, analyte_map, analyte_id_column):
        raw_data_df = self.experiment_parser.sheet_dfs[ExperimentWorkbook.RawDataSheet.name]
        stats_ready_df = self.experiment_parser.sheet_dfs[ExperimentWorkbook.StatsReadyDataSheet.name]

        measurement_map = {}
        for df, field in zip([raw_data_df, stats_ready_df], ['raw_data', 'stats_ready_data']):
            for index, row in df.iterrows():
                analyte_id = row[analyte_id_column]
                for column in df.columns:
                    if column == analyte_id_column:
                        continue
                    key = f"{analyte_id}|X|{column}"
                    if key in measurement_map:
                        edge_obj = measurement_map[key]
                    else:
                        edge_obj = SampleAnalyteRelationship(
                            start_node=sample_map[column],
                            end_node=analyte_map[analyte_id]
                        )
                        measurement_map[key] = edge_obj
                    edge_obj.__setattr__(field, row[column])
        yield measurement_map.values()



    def get_other_project_nodes_and_edges(self, proj_obj):
        types = self.get_project_type()
        proj_types, proj_type_rels = [], []

        for type in types:
            proj_type = ProjectType(
                id = type, name = type
            )
            proj_type_rel = ProjectTypeRelationship(
                start_node=proj_obj, end_node=proj_type
            )
            proj_types.append(proj_type)
            proj_type_rels.append(proj_type_rel)
        yield [proj_obj, *proj_types, *proj_type_rels]

        owner_names = self.get_project_owner_names()
        owner_emails = self.get_project_owner_emails()
        if len(owner_names) != len(owner_names):
            raise LookupError(f"Owner names and emails must have the same length", owner_names, owner_names)

        owner_objs, owner_rels = [], []
        for index, name in enumerate(owner_names):
            email = owner_emails[index]
            owner_obj = Investigator(id=name, email=email)
            owner_objs.append(owner_obj)
            owner_rels.append(
                ProjectInvestigatorRelationship(
                    start_node=proj_obj,
                    end_node=owner_obj,
                    roles=[Role.Owner]
                )
            )

        yield [*owner_objs, *owner_rels]

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.NCATSPounce

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version_date=self.get_project_date(),
            download_date=datetime.fromtimestamp(os.path.getmtime(self.project_file)).date()
        )

    def get_project_date(self) -> date:
        return self.project_parser.get_one_date(sheet_name=ProjectWorkbook.ProjectSheet.name, data_key=ProjectWorkbook.ProjectSheet.Key.date).date()

    def get_project_name(self) -> str:
        return self.project_parser.get_one_string(sheet_name=ProjectWorkbook.ProjectSheet.name, data_key=ProjectWorkbook.ProjectSheet.Key.project_name)

    def get_project_description(self) -> str:
        return self.project_parser.get_one_string(sheet_name=ProjectWorkbook.ProjectSheet.name, data_key=ProjectWorkbook.ProjectSheet.Key.description)

    def get_project_lab_groups(self) -> List[str]:
        return self.project_parser.get_one_string_list(sheet_name=ProjectWorkbook.ProjectSheet.name, data_key=ProjectWorkbook.ProjectSheet.Key.labgroups)

    def get_project_privacy_level(self) -> ProjectPrivacy:
        return ProjectPrivacy.parse(self.project_parser.get_one_string(sheet_name=ProjectWorkbook.ProjectSheet.name, data_key=ProjectWorkbook.ProjectSheet.Key.privacy_type))

    def get_project_keywords(self) -> List[str]:
        return self.project_parser.get_one_string_list(sheet_name=ProjectWorkbook.ProjectSheet.name, data_key=ProjectWorkbook.ProjectSheet.Key.custom_keywords)

    def get_project_type(self) -> List[str]:
        return self.project_parser.get_one_string_list(sheet_name=ProjectWorkbook.ProjectSheet.name, data_key=ProjectWorkbook.ProjectSheet.Key.project_type)

    def get_project_owner_names(self) -> List[str]:
        return self.project_parser.get_one_string_list(sheet_name=ProjectWorkbook.ProjectSheet.name, data_key=ProjectWorkbook.ProjectSheet.Key.owner_name)

    def get_project_owner_emails(self) -> List[str]:
        return self.project_parser.get_one_string_list(sheet_name=ProjectWorkbook.ProjectSheet.name, data_key=ProjectWorkbook.ProjectSheet.Key.owner_email)

    def get_experiment_name(self) -> str:
        return self.project_parser.get_one_string(sheet_name=ProjectWorkbook.ExperimentSheet.name, data_key=ProjectWorkbook.ExperimentSheet.Key.experiment_name)

    def get_experiment_type(self) -> str:
        return self.project_parser.get_one_string(sheet_name=ProjectWorkbook.ExperimentSheet.name, data_key=ProjectWorkbook.ExperimentSheet.Key.platform_type)

    def get_experiment_category(self) -> str:
        return self.project_parser.get_one_string(sheet_name=ProjectWorkbook.ExperimentSheet.name, data_key=ProjectWorkbook.ExperimentSheet.Key.experiment_category)

    def get_experiment_description(self) -> str:
        return self.project_parser.get_one_string(sheet_name=ProjectWorkbook.ExperimentSheet.name, data_key=ProjectWorkbook.ExperimentSheet.Key.description)

    def get_experiment_design(self) -> str:
        return self.project_parser.get_one_string(sheet_name=ProjectWorkbook.ExperimentSheet.name, data_key=ProjectWorkbook.ExperimentSheet.Key.experiment_design)

    def get_experiment_date(self) -> date:
        return self.project_parser.get_one_date(sheet_name=ProjectWorkbook.ExperimentSheet.name, data_key=ProjectWorkbook.ExperimentSheet.Key.date)