from typing import List

from src.input_adapters.excel_sheet_adapter import ExcelsheetParser
from src.interfaces.input_adapter import InputAdapter
from src.models.node import Node
from src.models.pounce.data import Protocol, Biospecimen, Factor, Sample, ExperimentSampleRelationship
from src.models.pounce.experiment import Experiment, ExperimentInvestigatorRelationship
from src.models.pounce.investigator import ProjectInvestigatorRelationship, Role, Investigator
from src.models.pounce.platform import Platform
from src.models.pounce.project import Project, ProjectPrivacy, ProjectType, ProjectTypeRelationship
from src.models.pounce.project_experiment_relationship import ProjectExperimentRelationship

def compile_sample_name(pattern, row, replicate: int) -> str:
    out_name = pattern
    out_name = out_name.replace("{replicate}", str(replicate))
    for column, value in row.items():
        out_name = out_name.replace("{" + column + "}", str(value))
    return out_name

class PounceAdapter(InputAdapter):
    excel_parser: ExcelsheetParser

    def get_audit_trail_entries(self, obj) -> List[str]:
        return []

    def __init__(self, input_sheet: str):
        self.excel_parser = ExcelsheetParser(path_to_sheet=input_sheet)

    def get_all(self) -> List[Node]:
        exp_level_stuff = self.get_all_experiment_level_stuff()
        exp_obj = exp_level_stuff[1] # yuck: TODO fix this

        static_factors = self.get_static_factors()

        samples, dynamic_factors, sample_rels = self.get_samples(static_factors)
        exp_sample_rels = [
            ExperimentSampleRelationship(
                start_node=exp_obj, end_node=samp_obj
            )
            for samp_obj in samples
        ]
        return [*exp_level_stuff, *static_factors, *samples, *exp_sample_rels, *dynamic_factors, *sample_rels]

    def get_samples(self, static_factors):

        samples = []
        dynamic_factors = []
        sample_rels = []

        sample_df = self.excel_parser.read_sheet('Samples')

        for index, row in sample_df.iterrows():
            for i in range(1, row['n_replicates'] + 1):
                sample_obj = Sample(
                    id=compile_sample_name(row['sample_name_pattern'], row, i)
                )
                samples.append(sample_obj)
                for factor in static_factors:

                    column_name = factor.id
                    val = row[column_name]
                    pass

        return samples, dynamic_factors, sample_rels

    def get_static_factors(self) -> List[Node]:
        factor_dict = self.excel_parser.get_config_dictionaries('Factors')
        factor_objects = []

        for plat_dict in factor_dict['Platform']:
            factor_objects.append(Platform(
                id=plat_dict['id'],
                type=plat_dict['type'],
                name=plat_dict['name']
            ))

        for protocol_dict in factor_dict['Protocols']:
            protocol_obj = Protocol(id=protocol_dict['id'])
            for k,v in protocol_dict.items():
                protocol_obj.__setattr__(k, v)
            factor_objects.append(protocol_obj)

        for biospecimen_dict in factor_dict['Biospecimen']:
            biospecimen_obj = Biospecimen(id=biospecimen_dict['id'])
            for k,v in biospecimen_dict.items():
                biospecimen_obj.__setattr__(k, v)
            factor_objects.append(biospecimen_obj)

        dynamic_factors = ['agent', 'vehicle']
        for fact_dict in factor_dict['Factor']:
            if fact_dict['type'] not in dynamic_factors:
                factor_obj = Factor(
                    id=fact_dict['column_label'],
                    name=fact_dict['name'],
                    type=fact_dict['type']
                )
                factor_objects.append(factor_obj)


        return factor_objects


    def get_all_experiment_level_stuff(self) -> List[Node]:
        def ensure_list(val, allow_tuple = False):
            if isinstance(val, list):
                return val
            if not allow_tuple and isinstance(val, tuple):
                return list(val)
            return [val]

        pe_dict = self.excel_parser.get_config_dictionaries('Project and Experiment')

        proj_dict = pe_dict['Project'][0]
        proj_obj = Project(
            id=proj_dict['name'],
            name=proj_dict['name'],
            description=proj_dict['description'],
            start_date=proj_dict['date'],
            lab_groups=ensure_list(proj_dict['labgroups']),
            privacy_level=ProjectPrivacy.parse(proj_dict['privacy_type']),
            keywords=ensure_list(proj_dict['keywords'])
        )

        owners = ensure_list(proj_dict['owners'], allow_tuple=True)
        invest_objs = [Investigator(id=owner[0], name=owner[0], email=owner[1]) for owner in owners]
        proj_inv_rels = [
            ProjectInvestigatorRelationship(start_node=proj_obj, end_node=owner_obj, roles=[Role.Owner])
            for owner_obj in invest_objs
        ]

        if 'collaborators' in proj_dict:
            collaborators = ensure_list(proj_dict['collaborators'], allow_tuple=True)

            collab_objs = [Investigator(id=collab[0], name=collab[0], email=collab[1]) for collab in collaborators]

            invest_objs.extend(collab_objs)
            proj_inv_rels.extend([
                ProjectInvestigatorRelationship(start_node=proj_obj, end_node=collab_obj, roles=[Role.Collaborator])
                for collab_obj in collab_objs
            ])


        exp_dict = pe_dict['Experiment'][0]
        exp_obj = Experiment(
            id=exp_dict['name'],
            type=exp_dict['output_type'],
            description=exp_dict['description'],
            design=exp_dict['design'],
            category=exp_dict['category']
        )
        exp_inv_rels = []
        if 'contact' in exp_dict:
            contact_dict = exp_dict['contact']
            invest_obj = Investigator(id=contact_dict[0], name=contact_dict[0], email=contact_dict[1])
            invest_objs.append(invest_obj)
            exp_inv_rels.append(ExperimentInvestigatorRelationship(
                start_node=exp_obj, end_node=invest_obj, roles=[Role.Contact]
            ))
        if 'lead_data_generator' in exp_dict:
            contact_dict = exp_dict['lead_data_generator']
            invest_obj = Investigator(id=contact_dict[0], name=contact_dict[0], email=contact_dict[1])
            invest_objs.append(invest_obj)
            exp_inv_rels.append(ExperimentInvestigatorRelationship(
                start_node=exp_obj, end_node=invest_obj, roles=[Role.DataGenerator]
            ))
        if 'lead_informatician' in exp_dict:
            contact_dict = exp_dict['lead_informatician']
            invest_obj = Investigator(id=contact_dict[0], name=contact_dict[0], email=contact_dict[1])
            invest_objs.append(invest_obj)
            exp_inv_rels.append(ExperimentInvestigatorRelationship(
                start_node=exp_obj, end_node=invest_obj, roles=[Role.Informatician]
            ))

        pe_rel = ProjectExperimentRelationship(start_node=proj_obj, end_node=exp_obj)

        return [proj_obj, exp_obj, pe_rel, *invest_objs, *proj_inv_rels, *exp_inv_rels]