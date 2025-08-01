import csv
import gzip
import os
from abc import ABC
from contextlib import contextmanager
from datetime import date, datetime
from typing import List, Union, Dict, Generator

from src.constants import DataSourceName, Prefix
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.gene import Gene
from src.models.metabolite import Metabolite
from src.models.node import Node, Relationship, EquivalentId
from src.models.pounce.data import Sample, ExperimentSampleRelationship, Biospecimen, SampleBiospecimenRelationship, \
    SampleAnalyteRelationship
from src.models.pounce.experiment import Experiment
from src.models.pounce.project import Project, ProjectType, ProjectTypeRelationship
from src.models.pounce.project_experiment_relationship import ProjectExperimentRelationship


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


class CCLEInputAdapter(InputAdapter, ABC):
    cell_line_annotation_file: str
    rnaseq_data_files: List[str]
    rnaseq_field_names: List[str]
    lcms_data_file: str

    download_date: date

    def __init__(self,
                 cell_line_annotation_file: str,
                 rnaseq_data_files: List[str],
                 rnaseq_field_names: List[str],
                 lcms_data_file: str
                 ):
        InputAdapter.__init__(self)
        self.cell_line_annotation_file = cell_line_annotation_file
        self.rnaseq_data_files = rnaseq_data_files
        self.rnaseq_field_names = rnaseq_field_names
        self.download_date = datetime.fromtimestamp(os.path.getmtime(self.rnaseq_data_files[0])).date()
        self.lcms_data_file = lcms_data_file

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.CCLE

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version="CCLE 2019",
            download_date=self.download_date
        )

    def get_experiment_name(self):
        return f"{self.get_datasource_name().value} - {self.get_version().version}"

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        print("parsing ccle data")
        start_time = datetime.now()

        proj_obj = Project(
            id="CCLE",
            name="Cancer Cell Line Encyclopedia",
            description='The Cancer Cell Line Encyclopedia (CCLE) project started in 2008 as a collaboration between the Broad Institute, and the Novartis Institutes for Biomedical Research and its Genomics Institute of the Novartis Research Foundation. The goal is to conduct a detailed genetic and pharmacologic characterization of a large panel of human cancer models, to develop integrated computational analyses that link distinct pharmacologic vulnerabilities to genomic patterns and to translate cell line integrative genomics into cancer patient stratification. Later the MD Anderson and Harvard Medical school joined the project. As of summer of 2018 CCLE continues its efforts as part of the Broad Cancer Dependency Map Project.',
        )

        experiment_obj = Experiment(
            id="CCLE",
            name=self.get_experiment_name(),
            type='bulk_rnaseq',
            description='The Cancer Cell Line Encyclopedia (CCLE) project started in 2008 as a collaboration between the Broad Institute, and the Novartis Institutes for Biomedical Research and its Genomics Institute of the Novartis Research Foundation. The goal is to conduct a detailed genetic and pharmacologic characterization of a large panel of human cancer models, to develop integrated computational analyses that link distinct pharmacologic vulnerabilities to genomic patterns and to translate cell line integrative genomics into cancer patient stratification. Later the MD Anderson and Harvard Medical school joined the project. As of summer of 2018 CCLE continues its efforts as part of the Broad Cancer Dependency Map Project.',
            category="in vitro"
        )

        proj_exp_edge = ProjectExperimentRelationship(
            start_node=proj_obj, end_node=experiment_obj
        )

        proj_type = ProjectType(
            id="Disease Characterization", name="Disease Characterization"
        )

        proj_proj_type_edge = ProjectTypeRelationship(
            start_node=proj_obj,
            end_node=proj_type
        )

        yield [proj_obj, proj_type, proj_proj_type_edge, experiment_obj, proj_exp_edge]

        sample_dict = get_sample_map(self.rnaseq_data_files[0])

        yield list(sample_dict.values())

        exp_samp_edges = []
        for sample_obj in sample_dict.values():
            exp_samp_edge = ExperimentSampleRelationship(
                start_node=experiment_obj,
                end_node=sample_obj
            )
            exp_samp_edges.append(exp_samp_edge)

        yield exp_samp_edges

        biospecimens = []
        samp_bio_edges: List[SampleBiospecimenRelationship] = []

        with open(self.cell_line_annotation_file, 'r') as file:
            reader = csv.DictReader(file, delimiter='\t')
            for line in reader:
                depMapID = line.get('depMapID')
                ccle_id = line.get('CCLE_ID')
                if depMapID is not None and depMapID != 'NA':
                    id = EquivalentId(id = depMapID, type = Prefix.DepMap).id_str()
                else:
                    id = EquivalentId(id = ccle_id, type = Prefix.CCLE_ID).id_str()

                biospecimen = Biospecimen(
                    id=id,
                    name=line.get('Name'),
                    type="Cancer Cell Line",
                    organism=['Homo sapiens (Human)'],
                    part=line.get('Site_Primary'),
                    cell_line=line.get('CCLE_ID'),
                    sex=line.get('Gender').lower(),
                    age=line.get('Age')
                )
                biospecimens.append(biospecimen)

                sample_obj = sample_dict.get(ccle_id)
                if sample_obj is not None:
                    samp_bio_edges.append(SampleBiospecimenRelationship(
                        start_node=sample_obj,
                        end_node=biospecimen
                    ))

        yield biospecimens
        yield samp_bio_edges

        limit = None
        samp_gene_edges = []
        genes = {}
        yielded_genes = set()

        for index, file_path in enumerate(self.rnaseq_data_files):
            field_name = self.rnaseq_field_names[index]
            count = 0
            with get_reader(file_path) as reader:
                for row in reader:
                    count += 1
                    if limit is not None and count > limit:
                        continue

                    if 'Name' in row:
                        gene_id = row['Name'].split('.')[0]
                    else:
                        gene_id = row['gene_id'].split('.')[0]

                    if gene_id in genes:
                        gene_obj = genes[gene_id]
                    else:
                        gene_obj = Gene(
                            id = EquivalentId(id=gene_id, type=Prefix.ENSEMBL).id_str()
                        )
                        genes[gene_id] = gene_obj

                    for sample, sample_obj in sample_dict.items():
                        measurement_value = float(row[sample])
                        # if measurement_value == 0:
                        #     continue

                        samp_gene_edge = SampleAnalyteRelationship(
                            start_node=sample_obj,
                            end_node=gene_obj
                        )
                        samp_gene_edges.append(samp_gene_edge)
                        samp_gene_edge.__setattr__(field_name, measurement_value)

                    if len(samp_gene_edges) > self.batch_size:

                        unyielded_genes = [gene for gene in genes.values() if gene.id not in yielded_genes]
                        yield unyielded_genes
                        yielded_genes.update([gene.id for gene in unyielded_genes])

                        yield samp_gene_edges
                        samp_gene_edges = []


        unyielded_genes = [gene for gene in genes.values() if gene.id not in yielded_genes]
        yield unyielded_genes
        yielded_genes.update([gene.id for gene in unyielded_genes])

        yield samp_gene_edges

        yield from self.get_lcms_data(sample_dict)

        end_time = datetime.now()
        print(f"parsing ccle data took:{end_time - start_time}s")

    def get_lcms_data(self, sample_dict: Dict[str, Sample]) -> Generator[List[Union[Node, Relationship]], None, None]:
        with open(self.lcms_data_file, 'r') as file:
            reader = csv.DictReader(file)
            metabolite_names = reader.fieldnames[2:]  # skip the first column (assumed to be sample or ID)

            metabolites = {name: Metabolite(id=name, name=name)
                for name in metabolite_names
            }

            yield list(metabolites.values())

            metabolite_edges = []

            for line in reader:
                ccle_id = line.get('CCLE_ID')
                sample_obj = sample_dict.get(ccle_id)
                if sample_obj is None:
                    continue

                for metabolite_name in reader.fieldnames[2:]:
                    metabolite_obj = metabolites.get(metabolite_name)

                    if metabolite_obj is None:
                        raise Exception(f"Metabolite {metabolite_name} not found in metabolites dictionary")

                    samp_met_edge = SampleAnalyteRelationship(
                        start_node=sample_obj,
                        end_node=metabolite_obj,
                        stats_ready_data=line[metabolite_name]
                    )
                    metabolite_edges.append(samp_met_edge)
                    if len(metabolite_edges) >= self.batch_size:
                        yield metabolite_edges
                        metabolite_edges = []

            yield metabolite_edges

