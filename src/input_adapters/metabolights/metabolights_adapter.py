import csv
import uuid
from datetime import datetime
from typing import List, Union
import re

from src.interfaces.input_adapter import InputAdapter
from src.models.node import Node
from src.models.pounce.data import ExperimentSampleRelationship, Sample, Biospecimen, Factor, SampleFactorRelationship, \
    Compound, Treatment, SampleMeasurementRelationship, MeasurementAnalyteRelationship, Measurement
from src.models.pounce.experiment import Experiment
from src.models.pounce.project import Project, ProjectPrivacy
from src.models.pounce.project_experiment_relationship import ProjectExperimentRelationship

ch_sample_type = 'Characteristics[Sample type]'

ch_organism_part = 'Characteristics[Organism part]'

ch_organism = 'Characteristics[Organism]'


def strip_tags(html: str):
    return re.sub(r'<[^>]+>', '', html)

class ParseMetabolights:
    investigation_file: str
    sample_file: str
    assay_files: List[str]
    matrix_files: List[str]

    def __init__(self, investigation_file: str, sample_file: str, assay_files: List[str], matrix_files: List[str]):
        self.investigation_file = investigation_file
        self.sample_file = sample_file
        self.assay_files = assay_files
        self.matrix_files = matrix_files

    def parse_proj_and_exp(self):
        with open(self.investigation_file, 'r') as i_file:
            lines = i_file.readlines()
            study_id = self.get_one_val(lines, 'Study Identifier')
            title = self.get_one_val(lines, 'Study Title')
            description = self.get_one_val(lines, 'Study Description')
            description = strip_tags(description)

            proj = Project(
                id=f"MTBLS:{study_id}",
                name=title,
                start_date=datetime.strptime(self.get_one_val(lines, 'Study Public Release Date'), "%Y-%m-%d"),
                privacy_level=ProjectPrivacy.Public,
                keywords=self.get_list_of_values(lines, 'Study Design Type')
            )

            exp = Experiment(
                id=f"MTBLS:{study_id}",
                name=title,
                description=description
            )
        return proj, exp

    biospecimen_dict = {}
    factor_dict = {}
    compound_dict = {}

    def get_or_create_factors(self, sample_file_row):
        factors = {k:v for k, v in sample_file_row.items() if k.startswith('Factor Value[')}
        factor_units = {k:v for k, v in sample_file_row.items() if k.startswith('Unit[')}

        for factor in factors:
            val = sample_file_row[factor]
            if val is None or val == '':
                continue
            factor_name = self.get_bracketed_name(factor)
            unit_key = f"Unit[{factor_name}]"

            properties = {
                "value": val
            }
            if unit_key in factor_units:
                unit_val = sample_file_row[unit_key]
                properties["unit"]=unit_val

            if factor_name not in self.factor_dict:
                self.factor_dict[factor_name] = Treatment(
                    id=factor_name,
                    name=factor_name
                )

            yield self.factor_dict[factor_name], properties




    def get_or_create_biospecimen(self, sample_file_row):
        species = sample_file_row[ch_organism]
        biospecimen = sample_file_row[ch_organism_part]
        other_characteristics = {k:v for k, v in sample_file_row.items() if k.startswith('Characteristics[') and k not in [ch_organism, ch_organism_part, ch_sample_type]}

        key = '|'.join([species, biospecimen, *[f"{self.get_bracketed_name(k)}|{v}" for k, v in other_characteristics.items() if v is not None and v != ""]])
        if key not in self.biospecimen_dict:
            biospecimen_obj = Biospecimen(
                id=key,
                name=biospecimen,
                organism=[species]
            )
            for k, v in other_characteristics.items():
                if v is not None and v != "":
                    biospecimen_obj.__setattr__(self.get_bracketed_name(k), v)

            self.biospecimen_dict[key] = biospecimen_obj
        return self.biospecimen_dict[key]

    def is_experimental_sample(self, sample_file_row):
        return sample_file_row[ch_sample_type] == 'experimental sample'

    def get_bracketed_name(self, field_name):
        pattern = r'\[(.*?)\]'
        match = re.search(pattern, field_name)
        return match.group(1) if match else None

    def get_sfile_field_names(self, csvfile):
        reader = csv.reader(csvfile, delimiter='\t')
        headers = next(reader)
        # headers = [header.lower() for header in headers]
        output_headers = []
        for index, field in enumerate(headers):
            if field == 'Unit':
                previous_field = headers[index-1]
                if not previous_field.startswith('Factor Value['):
                    raise Exception("Unit fields are supposed to follow Factor Value fields.")
                previous_field_name = self.get_bracketed_name(previous_field)
                output_headers.append(f"Unit[{previous_field_name}]")
            else:
                output_headers.append(field)
        csvfile.seek(0)
        return output_headers

    def parse_samples(self):
        samples: List[Sample] = []
        relationships: List[SampleFactorRelationship] = []
        with open(self.sample_file) as csvfile:
            field_names = self.get_sfile_field_names(csvfile)
            reader: csv.DictReader = csv.DictReader(csvfile, delimiter='\t', fieldnames=field_names)

            for row in reader:
                if not self.is_experimental_sample(row):
                    continue
                sample_obj = Sample(
                    id=row['Sample Name'],
                    description=row['Source Name'])

                samples.append(sample_obj)
                biospecimen = self.get_or_create_biospecimen(row)
                relationships.append(
                    SampleFactorRelationship(
                        start_node=sample_obj,
                        end_node=biospecimen
                    )
                )

                for factor, properties in self.get_or_create_factors(row):
                    factor_rel = SampleFactorRelationship(
                        start_node=sample_obj,
                        end_node=factor
                    )
                    for k, v in properties.items():
                        factor_rel.__setattr__(k, v)
                    relationships.append(factor_rel)

        return samples, [*self.biospecimen_dict.values(), *self.factor_dict.values()], relationships


    def get_or_create_compound(self, matrix_file_row):
        db_id = matrix_file_row['database_identifier']
        if db_id not in self.compound_dict:
            self.compound_dict[db_id] = Compound(
                id=db_id,
                name=matrix_file_row['metabolite_identification'],
                chemical_formula=matrix_file_row['chemical_formula'],
                smiles=matrix_file_row['smiles'],
                inchi=matrix_file_row['inchi']
            )
        return self.compound_dict[db_id]


    def parse_data(self, samples: List[Sample]):
        relationships: List[Union[SampleMeasurementRelationship, MeasurementAnalyteRelationship]] = []
        measurements: List[Measurement] = []
        for m_file in self.matrix_files:
            with open(m_file) as csvfile:
                reader: csv.DictReader = csv.DictReader(csvfile, delimiter='\t')
                for row in reader:
                    compound_obj = self.get_or_create_compound(row)
                    for sample in samples:
                        if sample.id in row:
                            value = row[sample.id]
                            if value is not None and value != 0:
                                measurement_obj = Measurement(
                                    id = str(uuid.uuid4()), value=value)
                                measurements.append(measurement_obj)
                                relationships.append(
                                    SampleMeasurementRelationship(
                                        start_node=sample,
                                        end_node=measurement_obj
                                    )
                                )
                                relationships.append(
                                    MeasurementAnalyteRelationship(
                                        start_node=measurement_obj,
                                        end_node=compound_obj
                                    )
                                )


        return measurements, self.compound_dict.values(), relationships



    def get_list_of_values(self, lines: List[str], label: str):
        matching_line = [line for line in lines if line.startswith(label)]
        pieces = matching_line[0].split('\t')
        return [p.strip() for p in  pieces[1:]]


    def get_one_val(self, lines: List[str], label: str):
        matching_line = [line for line in lines if line.startswith(label)]
        pieces = matching_line[0].split('\t')
        return pieces[1].strip()


class MetabolightsAdapter(InputAdapter, ParseMetabolights):

    def get_all(self) -> List[Node]:
        proj_obj, exp_obj = self.parse_proj_and_exp()
        proj_exp_rel = ProjectExperimentRelationship(
            start_node=proj_obj, end_node=exp_obj
        )

        samples, factors, sample_factor_rels = self.parse_samples()
        exp_sample_rels = [
            ExperimentSampleRelationship(
                start_node=exp_obj,
                end_node=sample
            )
            for sample in samples
        ]

        measurements, compounds, data_relationships = self.parse_data(samples)

        return [proj_obj, exp_obj, proj_exp_rel, *samples, *exp_sample_rels, *factors,
                *sample_factor_rels, *measurements, *compounds, *data_relationships]
