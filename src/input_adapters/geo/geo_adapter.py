import math
import uuid
from datetime import datetime
from typing import List
import GEOparse
from src.interfaces.input_adapter import NodeInputAdapter
from src.models.node import Node
from src.models.pounce.data import Biospecimen, \
    Sample, SampleFactorRelationship, ExperimentSampleRelationship, Factor, Treatment
from src.models.pounce.experiment import Experiment, ExperimentPlatformRelationship
from src.models.pounce.output import RNAProbe, SampleRNAProbeRelationship, Gene, GeneRNAProbeRelationship
from src.models.pounce.platform import Platform
from src.models.pounce.project import Project, ProjectPrivacy
from src.models.pounce.project_experiment_relationship import ProjectExperimentRelationship

biospecimen_types = ['individual', 'cell line']
treatment_types = ['agent']

class ParseGeo:
    accession: str

    def __init__(self, accession: str):
        self.accession = accession

    def parse_proj_and_exp(self, gse: GEOparse.GEOparse):
        name = gse.metadata['title'][0]
        description = gse.metadata.get('description', [None])[0]

        proj = Project(
            id=f"GEO:{self.accession}",
            name=name,
            start_date=datetime.strptime(gse.metadata['update_date'][0], "%b %d %Y"),
            privacy_level=ProjectPrivacy.Public
        )

        exp = Experiment(
            id=f"GEO:{self.accession}",
            name=name,
            description=description,
            category=gse.metadata['type'][0]
        )

        return [proj, exp]

    def parse_platform(self, gse: GEOparse.GEOparse):
        plat_id = gse.metadata['platform'][0]
        platform = Platform(
            id=plat_id,
            name=plat_id,
            type=gse.metadata['platform_technology_type'][0],
            url=f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={plat_id}"
        )
        return platform


    def get_or_create_sample(self, sample_map, sample_id, description):
        if sample_id not in sample_map:
            sample_map[sample_id] = Sample(
                                        id=sample_id,
                                        name=sample_id,
                                        description=description
                                    )
        return sample_map[sample_id]

    def get_or_create_biospecimen(self, biospecimen_map, type, name, species):
        if name not in biospecimen_map:
            if type == 'individual':
                id = '|'.join([species, type])
                name = type
            else:
                id = '|'.join([species, name])
            biospecimen_map[name] = Biospecimen(
                id=id,
                name=name,
                type=type,
                organism=[species]
            )
        return biospecimen_map[name]

    def get_or_create_treatment(self, treatment_map, type):
        if type not in treatment_map:
            treatment_map[type] = Treatment(
                id = str(uuid.uuid4()),
                name=type
            )
        return treatment_map[type]

    def parse_samples(self, gse: GEOparse.GEOparse):

        species = gse.metadata['sample_organism'][0]

        sample_dict = {}
        biospecimen_dict = {}
        treatment_dict = {}
        relationships = []

        for entry in gse.subsets.values():
            type = entry.metadata['type'][0]
            name = entry.metadata['description'][0]
            sample_ids = entry.metadata['sample_id'][0].split(',')
            if type in biospecimen_types:
                factor_obj = self.get_or_create_biospecimen(biospecimen_dict, type, name, species)
            elif type in treatment_types:
                factor_obj = self.get_or_create_treatment(treatment_dict, type)
            else:
                raise Exception(f"unknown factor type: {type}")
            for sample_id in sample_ids:
                sample_obj = self.get_or_create_sample(sample_dict, sample_id, gse.columns.description[sample_id])
                factor_rel = SampleFactorRelationship(
                    start_node=sample_obj,
                    end_node=factor_obj
                )
                factor_rel.value = name
                relationships.append(factor_rel)

        return sample_dict.values(), [*biospecimen_dict.values(), *treatment_dict.values()], relationships

    def parse_data(self, gse: GEOparse.GEOparse, samples: List[Sample]):
        nodes = []
        gene_map = {}
        relationships = []
        for _, row in gse.table.iterrows():
            platform_id_obj = RNAProbe(id=row['ID_REF'])
            nodes.append(platform_id_obj)

            gene_symbol = row['IDENTIFIER']
            if gene_symbol in gene_map:
                gene_obj = gene_map[gene_symbol]
            else:
                gene_obj = Gene(id=gene_symbol)
                gene_map[gene_symbol] = gene_obj
                nodes.append(gene_obj)
            relationships.append(
                GeneRNAProbeRelationship(
                    start_node=gene_obj,
                    end_node=platform_id_obj
                )
            )

            for sample in samples:
                val = row[sample.id]
                if not math.isnan(val):
                    rel = SampleRNAProbeRelationship(
                        start_node=sample,
                        end_node=platform_id_obj,
                        value=val
                    )
                    relationships.append(rel)

        return nodes, relationships


class GeoAdapter(NodeInputAdapter, ParseGeo):
    name = "Pounce GEO Adapter"

    def get_audit_trail_entries(self, obj) -> List[str]:
        return []

    def get_all(self) -> List[Node]:
        print(self.accession)
        gse = GEOparse.get_GEO(geo=self.accession, destdir="./input_files/GEO")
        [project_obj, exp_obj] = self.parse_proj_and_exp(gse)
        proj_exp_rel = ProjectExperimentRelationship(start_node=project_obj, end_node=exp_obj)
        platform_obj = self.parse_platform(gse)
        exp_plat_rel = ExperimentPlatformRelationship(start_node=exp_obj, end_node=platform_obj)

        samples, factors, sample_factor_relationships = self.parse_samples(gse)

        experiment_sample_relationships = []
        for sample_obj in samples:
            experiment_sample_relationships.append(
                ExperimentSampleRelationship(
                    start_node=exp_obj,
                    end_node=sample_obj
                )
            )

        data, data_relationships = self.parse_data(gse, samples)

        return [project_obj, exp_obj, proj_exp_rel,
                platform_obj, exp_plat_rel, *samples, *factors, *sample_factor_relationships,
                *data, *data_relationships, *experiment_sample_relationships]
