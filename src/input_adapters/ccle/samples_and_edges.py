import csv
import os
import pickle
import gzip
from abc import ABC
from contextlib import contextmanager
from typing import List, Union, Dict, Any

from src.constants import Prefix
from src.input_adapters.ccle.experiment_and_project import CCLEFileInputAdapter
from src.models.gene import Gene
from src.models.node import Node, Relationship, EquivalentId
from src.models.pounce.data import Sample, Measurement, SampleMeasurementRelationship, MeasurementAnalyteRelationship, \
    SampleFactorRelationship, Biospecimen, ExperimentSampleRelationship
from src.models.pounce.experiment import Experiment


@contextmanager
def get_reader(file_path: str):
    file = gzip.open(file_path, "rt", encoding="utf-8")
    if file_path.endswith('.gct.gz'):  # Skip first two rows of gct files, since they are not part of the table
        for _ in range(2):
            next(file)
    yield csv.DictReader(file, delimiter='\t')

def get_sample_map(file_path: str) -> Dict[str, Sample]:
    sample_dict: Dict[str, Sample] = {}
    with get_reader(file_path) as reader:
        headers = reader.fieldnames
        for column in headers[2:]:
            ccle_id = column
            id = f"CCLE 2019 - {ccle_id}"
            sample = Sample(
                id = id,
                name = column
            )
            sample_dict[ccle_id] = sample
    return sample_dict

class SampleAdapter(CCLEFileInputAdapter):
    def get_all(self) -> List[Union[Node, Relationship]]:
        sample_dict = get_sample_map(self.file_path)
        return list(sample_dict.values())

class MeasurementBaseAdapter(CCLEFileInputAdapter, ABC):
    def get_cached_lists(self, field_name, limit):
        folder = os.path.dirname(self.file_path)
        try:
            with open(os.path.join(folder, f'measurement_objects_{field_name}_{limit}.pkl'), 'rb') as f:
                measurement_objects = pickle.load(f)
            with open(os.path.join(folder, f'samp_meas_edges_{field_name}_{limit}.pkl'), 'rb') as f:
                samp_meas_edges = pickle.load(f)
            with open(os.path.join(folder, f'meas_gene_edges_{field_name}_{limit}.pkl'), 'rb') as f:
                meas_gene_edges = pickle.load(f)
            with open(os.path.join(folder, f'exp_samp_edges_{field_name}_{limit}.pkl'), 'rb') as f:
                exp_samp_edges = pickle.load(f)
        except FileNotFoundError:
            measurement_objects = []
            samp_meas_edges = []
            meas_gene_edges = []
            exp_samp_edges = []
        return measurement_objects, samp_meas_edges, meas_gene_edges, exp_samp_edges

    def save_cached_lists(self, measurement_objects, samp_meas_edges, meas_gene_edges, exp_samp_edges, field_name, limit):
        folder = os.path.dirname(self.file_path)
        with open(os.path.join(folder, f'measurement_objects_{field_name}_{limit}.pkl'), 'wb') as f:
            pickle.dump(measurement_objects, f)
        with open(os.path.join(folder, f'samp_meas_edges_{field_name}_{limit}.pkl'), 'wb') as f:
            pickle.dump(samp_meas_edges, f)
        with open(os.path.join(folder, f'meas_gene_edges_{field_name}_{limit}.pkl'), 'wb') as f:
            pickle.dump(meas_gene_edges, f)
        with open(os.path.join(folder, f'exp_samp_edges_{field_name}_{limit}.pkl'), 'wb') as f:
            pickle.dump(exp_samp_edges, f)


    def get_all_components(self, field_name = 'count') -> tuple[list[Any], list[Any], list[Any], list[Any]]:
        limit = 1000
        measurement_objects, samp_meas_edges, meas_gene_edges, exp_samp_edges = self.get_cached_lists(field_name, limit)
        if len(measurement_objects) > 0:
            return measurement_objects, samp_meas_edges, meas_gene_edges, exp_samp_edges

        sample_dict = get_sample_map(self.file_path)

        index = 0
        exp_obj = Experiment(id=self.get_experiment_name())
        with get_reader(self.file_path) as reader:
            for row in reader:
                index += 1
                if limit is not None and index > limit:
                    continue

                if 'Name' in row:
                    gene_id = row['Name'].split('.')[0]
                else:
                    gene_id = row['gene_id'].split('.')[0]

                for sample, sample_obj in sample_dict.items():
                    measurement_value = float(row[sample])
                    if measurement_value == 0:
                        continue

                    measurement_obj = Measurement(
                        id=f"{sample_obj.id}-{gene_id}"
                    )
                    measurement_obj.__setattr__(field_name, measurement_value)
                    measurement_objects.append(measurement_obj)

                    samp_meas_edge = SampleMeasurementRelationship(
                        start_node=sample_obj,
                        end_node=measurement_obj
                    )
                    samp_meas_edges.append(samp_meas_edge)

                    exp_samp_edge = ExperimentSampleRelationship(
                        start_node=exp_obj,
                        end_node=sample_obj
                    )
                    exp_samp_edges.append(exp_samp_edge)

                    gene_meas_edge = MeasurementAnalyteRelationship(
                        start_node=measurement_obj,
                        end_node=Gene(
                            id = EquivalentId(id=gene_id, type=Prefix.ENSEMBL).id_str()
                        )
                    )
                    meas_gene_edges.append(gene_meas_edge)

        self.save_cached_lists(measurement_objects, samp_meas_edges, meas_gene_edges, exp_samp_edges, field_name, limit)

        return measurement_objects, samp_meas_edges, meas_gene_edges, exp_samp_edges

class MeasurementAdapter(MeasurementBaseAdapter):
    field_name: str


    def __init__(self, field_name: str = 'count', **kwargs):
        MeasurementBaseAdapter.__init__(self, **kwargs)
        self.field_name = field_name

    def get_all(self) -> List[Union[Node, Relationship]]:
        measurements, _, _, _ = self.get_all_components(self.field_name)
        return measurements

class SampleMeasurementAdapter(MeasurementBaseAdapter):
    def get_all(self) -> List[Union[Node, Relationship]]:
        _, samp_meas_edges, _, _ = self.get_all_components()
        return samp_meas_edges

class SampleExperimentAdapter(MeasurementBaseAdapter):
    def get_all(self) -> List[Union[Node, Relationship]]:
        _, _, _, exp_samp_edges = self.get_all_components()
        return exp_samp_edges

class MeasurementGeneAdapter(MeasurementBaseAdapter):
    def get_all(self) -> List[Union[Node, Relationship]]:
        _, _, meas_gene_edges, _ = self.get_all_components()
        return meas_gene_edges

class SampleBiospecimenAdapter(MeasurementBaseAdapter):
    context_file_path: str
    depmap_map: Dict[str, str]

    def get_depmap_map(self):
        depmap_map = {}
        with open(self.context_file_path, 'r') as file:
            reader = csv.DictReader(file, delimiter='\t')
            for line in reader:
                depmap_id = line.get('depMapID')
                ccle_id = line.get('CCLE_ID')
                if depmap_id is not None and depmap_id != 'NA':
                    if ccle_id in depmap_map:
                        raise Exception(f"Duplicate ccle_id: {ccle_id}")
                    depmap_map[ccle_id] = depmap_id
        return depmap_map

    def __init__(self, context_file_path: str, **kwargs):
        self.context_file_path = context_file_path
        CCLEFileInputAdapter.__init__(self, **kwargs)
        self.depmap_map = self.get_depmap_map()

    def get_all(self) -> List[Union[Node, Relationship]]:
        sample_dict = get_sample_map(self.file_path)
        samp_bio_edges: List[SampleFactorRelationship] = []
        for sample in sample_dict.values():
            depmap_id = self.depmap_map.get(sample.name)
            bio_id = EquivalentId(id = depmap_id, type = Prefix.DepMap).id_str()
            bio_obj = Biospecimen(id = bio_id)
            samp_bio_edges.append(SampleFactorRelationship(
                start_node=sample,
                end_node=bio_obj
            ))
        return samp_bio_edges
