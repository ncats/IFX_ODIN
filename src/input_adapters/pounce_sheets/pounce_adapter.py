from typing import List

from src.input_adapters.excel_sheet_adapter import ExcelsheetParser
from src.input_adapters.pounce_sheets.constants import KEY_PROJECT_KEYWORDS, KEY_PROJECT_TYPE, \
    KEY_COLLAB_EMAIL, KEY_COLLAB_NAME, KEY_PROJECT_OWNER_EMAIL, KEY_PROJECT_OWNER_NAME, KEY_PROJECT_LABS, \
    KEY_PROJECT_DATE, KEY_PROJECT_NAME, KEY_DESCRIPTION, KEY_PROJECT_PRIVACY, KEY_PROJECT_SHEET, KEY_EXPERIMENT_SHEET, \
    KEY_EXPERIMENT_NAME, KEY_EXPERIMENT_DESIGN, KEY_EXPERIMENT_DATE, KEY_EXPERIMENT_CATEGORY, KEY_CONTACT_NAME, \
    KEY_CONTACT_EMAIL, KEY_DATAGEN_EMAIL, KEY_DATAGEN_NAME, KEY_INFORMATICIAN_NAME, KEY_INFORMATICIAN_EMAIL
from src.interfaces.input_adapter import NodeInputAdapter, RelationshipInputAdapter
from src.models.node import Node
from src.models.pounce.experiment import Experiment
from src.models.pounce.investigator import ProjectInvestigatorRelationship, Role
from src.models.pounce.project import Project, ProjectPrivacy, ProjectType, ProjectTypeRelationship
from src.models.pounce.project_experiment_relationship import ProjectExperimentRelationship


class PounceAdapter(NodeInputAdapter, RelationshipInputAdapter):
    name = "Pounce NCATS Input Sheet Adapter"
    project_sheet_parser: ExcelsheetParser
    experiment_sheet_parser: ExcelsheetParser

    def __init__(self, project_sheet: str, experiment_sheet: str):
        self.project_sheet_parser = ExcelsheetParser(path_to_sheet=project_sheet)
        self.experiment_sheet_parser = ExcelsheetParser(path_to_sheet=experiment_sheet)

    def get_biospecimen_paramter_map(self):
        map_df = self.project_sheet_parser.read_sheet("BiospecimenMap")
        param_map = self.project_sheet_parser.get_parameter_map(map_df, 'NCATSDPI_Variable_Name', 'Submitter_Variable_Name')

        cell_type_column = param_map['biospecimen_name']
        for k, v in param_map.items():
            if v.lower().startswith('cell'):
                cell_type_column = v
        param_map['cell_type']=cell_type_column
        return param_map

    def get_audit_trail_entries(self, obj) -> List[str]:
        return []

    def create_investigator_relationship(self, **kwargs):
        return ProjectInvestigatorRelationship(**kwargs)


    def get_experiment(self) -> List[Node]:
        experiment_df = self.project_sheet_parser.read_sheet(KEY_EXPERIMENT_SHEET)

        exp_name = self.project_sheet_parser.get_one_string(experiment_df, KEY_EXPERIMENT_NAME)
        exp_id = self.project_sheet_parser.get_id_from_name(exp_name)

        exp_obj = Experiment(
            id=exp_id,
            name=exp_name,
            description=self.project_sheet_parser.get_one_string(experiment_df, KEY_DESCRIPTION),
            design=self.project_sheet_parser.get_one_string(experiment_df, KEY_EXPERIMENT_DESIGN),
            run_date=self.project_sheet_parser.get_one_date(experiment_df, KEY_EXPERIMENT_DATE),
            category=self.project_sheet_parser.get_one_string(experiment_df, KEY_EXPERIMENT_CATEGORY)
        )

        investigator_objs = []
        investigator_rels = []

        objs, rels = self.project_sheet_parser.get_investigators(experiment_df, exp_obj, KEY_CONTACT_NAME, KEY_CONTACT_EMAIL, Role.Contact)
        investigator_objs.extend(objs)
        investigator_rels.extend(rels)

        objs, rels = self.project_sheet_parser.get_investigators(experiment_df, exp_obj, KEY_DATAGEN_NAME, KEY_DATAGEN_EMAIL, Role.DataGenerator)
        investigator_objs.extend(objs)
        investigator_rels.extend(rels)

        objs, rels = self.project_sheet_parser.get_investigators(experiment_df, exp_obj, KEY_INFORMATICIAN_NAME,
                                            KEY_INFORMATICIAN_EMAIL, Role.Informatician)
        investigator_objs.extend(objs)
        investigator_rels.extend(rels)

        return [exp_obj, *investigator_objs, *investigator_rels]

    def get_proj_exp_rel(self):

        project_df = self.project_sheet_parser.read_sheet(KEY_PROJECT_SHEET)
        project_name = self.project_sheet_parser.get_one_string(project_df, KEY_PROJECT_NAME)
        project_id = self.project_sheet_parser.get_id_from_name(project_name)

        experiment_df = self.project_sheet_parser.read_sheet(KEY_EXPERIMENT_SHEET)
        experiment_name = self.project_sheet_parser.get_one_string(experiment_df, KEY_EXPERIMENT_NAME)
        experiment_id = self.project_sheet_parser.get_id_from_name(experiment_name)

        return ProjectExperimentRelationship(
            start_node=Project(id=project_id),
            end_node=Experiment(id=experiment_id)
        )

    def get_all(self) -> List[Node]:
        project_df = self.project_sheet_parser.read_sheet(KEY_PROJECT_SHEET)

        project_name = self.project_sheet_parser.get_one_string(project_df, KEY_PROJECT_NAME)
        project_id = self.project_sheet_parser.get_id_from_name(project_name)


        proj_obj = Project(
            id=project_id,
            name=project_name,
            description=self.project_sheet_parser.get_one_string(project_df, KEY_DESCRIPTION),
            lab_groups=self.project_sheet_parser.get_one_string_list(project_df, KEY_PROJECT_LABS),
            start_date=self.project_sheet_parser.get_one_date(project_df, KEY_PROJECT_DATE),
            privacy_level=ProjectPrivacy.parse(self.project_sheet_parser.get_one_string(project_df, KEY_PROJECT_PRIVACY)),
            keywords=self.project_sheet_parser.get_one_string_list(project_df, KEY_PROJECT_KEYWORDS)
        )
        owner_objs, owner_rels = self.project_sheet_parser.get_investigators(project_df, proj_obj, KEY_PROJECT_OWNER_NAME, KEY_PROJECT_OWNER_EMAIL, Role.Owner)
        collab_objs, collab_rels = self.project_sheet_parser.get_investigators(project_df, proj_obj, KEY_COLLAB_NAME,
                                                          KEY_COLLAB_EMAIL, Role.Collaborator)

        types = self.project_sheet_parser.get_one_string_list(project_df, KEY_PROJECT_TYPE)
        type_objs = [
            ProjectType(id=type)
            for type in types
        ]
        type_rels = [
            ProjectTypeRelationship(start_node=proj_obj, end_node=type_obj) for type_obj in type_objs
        ]

        experiment_nodes_and_rels = self.get_experiment()
        proj_exp_rel = self.get_proj_exp_rel()

        bio_param_map = self.get_biospecimen_paramter_map()
        metadata_df = self.project_sheet_parser.read_sheet("BiospecimenMeta")

        for _, row in metadata_df.iterrows():
            print(row[bio_param_map['biospecimen_organism']])
            print(row[bio_param_map['biospecimen_type']])
            print(row[bio_param_map['cell_type']])

        return [proj_obj, *owner_objs, *collab_objs, *owner_rels, *collab_rels, *type_objs, *type_rels, *experiment_nodes_and_rels, proj_exp_rel]
