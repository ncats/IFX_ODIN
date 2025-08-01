from typing import Optional, Union, List, Dict

import strawberry
from strawberry import Info

from src.api_adapters.arango_api_adapter import ArangoAPIAdapter
from src.api_adapters.strawberry_models.class_generators import make_linked_list_result_type, \
    make_networked_list_result_type
from src.api_adapters.strawberry_models.input_types import LinkedListFilterSettings
from src.api_adapters.strawberry_models.shared_query_models import Provenance, generate_resolvers
from src.interfaces.result_types import LinkedListQueryContext, NetworkedListQueryContext
from src.models.node import EquivalentId
from src.models.pounce.data import (Biospecimen as BiospecimenBase, Sample as SampleBase,
                                    SampleBiospecimenRelationship as SampleBiospecimenRelationshipBase,
                                    SampleAnalyteRelationship as SampleAnalyteRelationshipBase,
                                    ExperimentSampleRelationship as ExperimentSampleRelationshipBase)
from src.models.pounce.investigator import Investigator as InvestigatorBase, ProjectInvestigatorRelationship as ProjectInvestigatorRelationshipBase
from src.models.pounce.project_experiment_relationship import ProjectExperimentRelationship as ProjectExperimentRelationshipBase
from src.models.protein import Protein as ProteinBase
from src.models.pounce.project import Project as ProjectBase, ProjectType as ProjectTypeBase, ProjectTypeRelationship as ProjectTypeRelationshipBase
from src.models.pounce.experiment import Experiment as ExperimentBase, ExperimentInvestigatorRelationship as ExperimentInvestigatorRelationshipBase
from src.models.gene import Gene as GeneBase, GeneticLocation
from src.models.metabolite import Metabolite as MetaboliteBase

from src.interfaces.simple_enum import NodeLabel, RelationshipLabel
from src.models.analyte import Synonym

# base classes
NodeLabel = strawberry.type(NodeLabel)
RelationshipLabel = strawberry.type(RelationshipLabel)
EquivalentId = strawberry.type(EquivalentId)
Synonym = strawberry.type(Synonym)
GeneticLocation = strawberry.type(GeneticLocation)

# node classes

@strawberry.type
class Biospecimen(BiospecimenBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    @strawberry.field()
    def samples(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "SampleBiospecimenQueryResult":
        api: ArangoAPIAdapter = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Sample",
            source_id=None,
            dest_data_model="Biospecimen",
            edge_model="SampleBiospecimenRelationship",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def experiments(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "BiospecimenExperimentQueryResult":
        api = info.context["api"]
        context = NetworkedListQueryContext(
            source_data_model="Experiment",
            source_id=None,
            dest_data_model="Biospecimen",
            intermediate_data_models=["Sample"],
            edge_models=["ExperimentSampleRelationship", "SampleBiospecimenRelationship" ],
            dest_id=root.id
            # filter=filter
        )
        result = api.get_networked_list(context)
        return result


@strawberry.type
class Sample(SampleBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    @strawberry.field()
    def experiment(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "SampleExperimentQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Experiment",
            source_id=None,
            dest_data_model="Sample",
            edge_model="ExperimentSampleRelationship",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def biospecimen(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "BiospecimenSampleQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Sample",
            source_id=root.id,
            dest_data_model="Biospecimen",
            edge_model="SampleBiospecimenRelationship",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def genes(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "SampleGeneQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Sample",
            source_id=root.id,
            dest_data_model="Gene",
            edge_model="SampleAnalyteRelationship",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def proteins(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "SampleProteinQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Sample",
            source_id=root.id,
            dest_data_model="Protein",
            edge_model="SampleAnalyteRelationship",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def metabolites(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "SampleMetaboliteQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Sample",
            source_id=root.id,
            dest_data_model="Metabolite",
            edge_model="SampleAnalyteRelationship",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

@strawberry.type
class Investigator(InvestigatorBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    @strawberry.field()
    def projects(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "InvestigatorProjectQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Project",
            source_id=None,
            dest_data_model="Investigator",
            edge_model="ProjectInvestigatorRelationship",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def experiments(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "InvestigatorExperimentQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Experiment",
            source_id=None,
            dest_data_model="Investigator",
            edge_model="ProjectInvestigatorRelationship", # should be ExperimentInvestigatorRelationship, but the database is wrong right now
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

@strawberry.type
class Protein(ProteinBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    @strawberry.field()
    def samples(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "ProteinSampleQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Sample",
            source_id=None,
            dest_data_model="Protein",
            edge_model="SampleAnalyteRelationship",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

@strawberry.type
class Project(ProjectBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    @strawberry.field()
    def project_type(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "ProjectProjectTypeQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Project",
            source_id=root.id,
            dest_data_model="ProjectType",
            edge_model="ProjectTypeRelationship",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def experiments(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "ProjectExperimentQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Project",
            source_id=root.id,
            dest_data_model="Experiment",
            edge_model="ProjectExperimentRelationship",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def investigators(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "ProjectInvestigatorQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Project",
            source_id=root.id,
            dest_data_model="Investigator",
            edge_model="ProjectInvestigatorRelationship",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

@strawberry.type
class ProjectType(ProjectTypeBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    @strawberry.field()
    def projects(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "ProjectTypeProjectQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Project",
            source_id=None,
            dest_data_model="ProjectType",
            edge_model="ProjectTypeRelationship",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result


@strawberry.type
class Experiment(ExperimentBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    @staticmethod
    def data_query(root, type_str: str, biospecimen_id: Optional[str] = None, analyte_id: Optional[str] = None, top: Optional[int] = 10, skip: Optional[int] = 0) -> str:
        return f"""FOR exp IN Experiment
          FILTER exp.id == "{root.id}"
          
          FOR sample IN OUTBOUND exp ExperimentSampleRelationship
            FOR biospecimen IN OUTBOUND sample SampleBiospecimenRelationship
              {"FILTER biospecimen.id == '" + biospecimen_id + "'" if biospecimen_id else ""} 
              
              FOR analyte, edge IN OUTBOUND sample SampleAnalyteRelationship
                LIMIT {skip}, {top}
                FILTER IS_SAME_COLLECTION("{type_str}", analyte)
                {"FILTER '" + analyte_id + "' in analyte.xref" if analyte_id else ""}
                RETURN {{
                  sample: sample,
                  biospecimen: biospecimen,
                  analyte: analyte,
                  data_edge: edge
                }}"""


    @strawberry.field()
    def gene_data(root,
                  info: Info, biospecimen_id: Optional[str] = None,
                  gene_id: Optional[str] = None,
                  top: Optional[int] = 10,
                  skip: Optional[int] = 0
                  ) -> Optional[List["GeneDataResults"]]:
        api: ArangoAPIAdapter = info.context["api"]
        query = Experiment.data_query(root, "Gene", biospecimen_id=biospecimen_id, analyte_id=gene_id, top=top, skip=skip)
        results = api.runQuery(query)
        obj_results = [
            GeneDataResults(
                sample=api.convert_to_class("Sample", row['sample']),
                biospecimen=api.convert_to_class("Biospecimen", row['biospecimen']),
                gene=api.convert_to_class("Gene", row['analyte']),
                data_edge=api.convert_to_class("SampleAnalyteRelationship", row['data_edge'])
            )
            for row in results
        ]
        return obj_results

    @strawberry.field()
    def metabolite_data(root, info: Info, biospecimen_id: Optional[str] = None, metabolite_id: Optional[str] = None, top: Optional[int] = 10, skip: Optional[int] = 0) -> Optional[List["MetaboliteDataResults"]]:
        api: ArangoAPIAdapter = info.context["api"]
        query = Experiment.data_query(root, "Metabolite", biospecimen_id=biospecimen_id, analyte_id=metabolite_id, top=top, skip=skip)
        results = api.runQuery(query)
        obj_results = [
            MetaboliteDataResults(
                sample=api.convert_to_class("Sample", row['sample']),
                biospecimen=api.convert_to_class("Biospecimen", row['biospecimen']),
                metabolite=api.convert_to_class("Metabolite", row['analyte']),
                data_edge=api.convert_to_class("SampleAnalyteRelationship", row['data_edge'])
            )
            for row in results
        ]
        return obj_results

    @strawberry.field()
    def protein_data(root, info: Info, biospecimen_id: Optional[str] = None, protein_id: Optional[str] = None, top: Optional[int] = 10, skip: Optional[int] = 0) -> Optional[List["ProteinDataResults"]]:
        api: ArangoAPIAdapter = info.context["api"]
        query = Experiment.data_query(root, "Protein", biospecimen_id=biospecimen_id, analyte_id=protein_id, top=top, skip=skip)
        results = api.runQuery(query)
        obj_results = [
            ProteinDataResults(
                sample=api.convert_to_class("Sample", row['sample']),
                biospecimen=api.convert_to_class("Biospecimen", row['biospecimen']),
                protein=api.convert_to_class("Protein", row['analyte']),
                data_edge=api.convert_to_class("SampleAnalyteRelationship", row['data_edge'])
            )
            for row in results
        ]
        return obj_results

    @strawberry.field()
    def projects(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "ExperimentProjectQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Project",
            source_id=None,
            dest_data_model="Experiment",
            edge_model="ProjectExperimentRelationship",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result


    @strawberry.field()
    def investigators(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "ExperimentInvestigatorQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Experiment",
            source_id=root.id,
            dest_data_model="Investigator",
            edge_model="ProjectInvestigatorRelationship", # should be ExperimentInvestigatorRelationship, but the database is wrong right now
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def samples(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "ExperimentSampleQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Experiment",
            source_id=root.id,
            dest_data_model="Sample",
            edge_model="ExperimentSampleRelationship",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def biospecimens(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "ExperimentBiospecimenQueryResult":
        api = info.context["api"]
        context = NetworkedListQueryContext(
            source_data_model="Experiment",
            source_id=root.id,
            dest_data_model="Biospecimen",
            intermediate_data_models=["Sample"],
            edge_models=["ExperimentSampleRelationship", "SampleBiospecimenRelationship" ],
            dest_id=None
        #     filter=filter
        )
        result = api.get_networked_list(context)
        return result

@strawberry.type
class Gene(GeneBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    @strawberry.field()
    def samples(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "GeneSampleQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Sample",
            source_id=None,
            dest_data_model="Gene",
            edge_model="SampleAnalyteRelationship",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

@strawberry.type
class Metabolite(MetaboliteBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    @strawberry.field()
    def samples(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "MetaboliteSampleQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Sample",
            source_id=None,
            dest_data_model="Metabolite",
            edge_model="SampleAnalyteRelationship",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

# edge classes
@strawberry.type
class ExperimentSampleRelationship(ExperimentSampleRelationshipBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)
    start_node: Experiment
    end_node: Sample

@strawberry.type
class SampleBiospecimenRelationship(SampleBiospecimenRelationshipBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)
    start_node: Sample
    end_node: Biospecimen

@strawberry.type
class ProjectExperimentRelationship(ProjectExperimentRelationshipBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)
    start_node: Project
    end_node: Experiment

@strawberry.type
class ProjectInvestigatorRelationship(ProjectInvestigatorRelationshipBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)
    start_node: Project
    end_node: Investigator

@strawberry.type
class ExperimentInvestigatorRelationship(ExperimentInvestigatorRelationshipBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)
    start_node: Experiment
    end_node: Investigator

@strawberry.type
class ProjectTypeRelationship(ProjectTypeRelationshipBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)
    start_node: Project
    end_node: ProjectType

@strawberry.type
class SampleAnalyteRelationship(SampleAnalyteRelationshipBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)
    start_node: Sample
    end_node: Union[Gene, Metabolite, Protein]


@strawberry.type
class GeneDataResults:
    sample: Sample
    biospecimen: Optional[Biospecimen]
    gene: Gene
    data_edge: SampleAnalyteRelationship

@strawberry.type
class ProteinDataResults:
    sample: Sample
    biospecimen: Optional[Biospecimen]
    protein: Protein
    data_edge: SampleAnalyteRelationship

@strawberry.type
class MetaboliteDataResults:
    sample: Sample
    biospecimen: Optional[Biospecimen]
    metabolite: Metabolite
    data_edge: SampleAnalyteRelationship

ExperimentSampleQueryResult = make_linked_list_result_type("ExperimentSampleQueryResult", "ExperimentSampleDetails", ExperimentSampleRelationship, Sample)
SampleExperimentQueryResult = make_linked_list_result_type("SampleExperimentQueryResult", "SampleExperimentDetails", ExperimentSampleRelationship, Experiment)

ExperimentBiospecimenQueryResult = make_networked_list_result_type("ExperimentBiospecimenQueryResult", "ExperimentBiospecimenDetails", Biospecimen)
BiospecimenExperimentQueryResult = make_networked_list_result_type("BiospecimenExperimentQueryResult", "BiospecimenExperimentDetails", Experiment)

SampleGeneQueryResult = make_linked_list_result_type("SampleGeneQueryResult", "SampleGeneDetails", SampleAnalyteRelationship, Gene)
SampleMetaboliteQueryResult = make_linked_list_result_type("SampleMetaboliteQueryResult", "SampleMetaboliteDetails", SampleAnalyteRelationship, Metabolite)
SampleProteinQueryResult = make_linked_list_result_type("SampleProteinQueryResult", "SampleProteinDetails", SampleAnalyteRelationship, Protein)
GeneSampleQueryResult = make_linked_list_result_type("GeneSampleQueryResult", "GeneSampleDetails", SampleAnalyteRelationship, Sample)
MetaboliteSampleQueryResult = make_linked_list_result_type("MetaboliteSampleQueryResult", "MetaboliteSampleDetails", SampleAnalyteRelationship, Sample)
ProteinSampleQueryResult = make_linked_list_result_type("ProteinSampleQueryResult", "ProteinSampleDetails", SampleAnalyteRelationship, Sample)

ProjectProjectTypeQueryResult = make_linked_list_result_type("ProjectProjectTypeQueryResult", "ProjectProjectTypeDetails", ProjectTypeRelationship, ProjectType)
ProjectTypeProjectQueryResult = make_linked_list_result_type("ProjectTypeProjectQueryResult", "ProjectTypeProjectDetails", ProjectTypeRelationship, Project)

SampleBiospecimenQueryResult = make_linked_list_result_type("SampleBiospecimenQueryResult", "SampleBiospecimenDetails", SampleBiospecimenRelationship, Sample)
BiospecimenSampleQueryResult = make_linked_list_result_type("BiospecimenSampleQueryResult", "BiospecimenSampleDetails", SampleBiospecimenRelationship, Biospecimen)

ProjectExperimentQueryResult = make_linked_list_result_type("ProjectExperimentQueryResult", "ProjectExperimentDetails", ProjectExperimentRelationship, Experiment)
ExperimentProjectQueryResult = make_linked_list_result_type("ExperimentProjectQueryResult", "ExperimentProjectDetails", ProjectExperimentRelationship, Project)

ProjectInvestigatorQueryResult = make_linked_list_result_type("ProjectInvestigatorQueryResult", "ProjectInvestigatorDetails", ProjectInvestigatorRelationship, Investigator)
InvestigatorProjectQueryResult = make_linked_list_result_type("InvestigatorProjectQueryResult", "InvestigatorProjectDetails", ProjectInvestigatorRelationship, Project)

ExperimentInvestigatorQueryResult = make_linked_list_result_type("ExperimentInvestigatorQueryResult", "ExperimentInvestigatorDetails", ExperimentInvestigatorRelationship, Investigator)
InvestigatorExperimentQueryResult = make_linked_list_result_type("InvestigatorExperimentQueryResult", "InvestigatorExperimentDetails", ExperimentInvestigatorRelationship, Experiment)

ENDPOINTS: Dict[type, Dict[str, str]] = {
    Project: {
        "list": "projects",
        "details": "resolve_project"
    },
    Experiment: {
        "list": "experiments",
        "details": "resolve_experiment"
    },
    Investigator: {
        "list": "investigators",
        "details": "resolve_investigator"
    },
    Biospecimen: {
        "list": "biospecimens",
        "details": "resolve_biospecimen"
    },
    Sample: {
        "list": "samples",
        "details": "resolve_sample"
    },
    Protein: {
        "list": "proteins",
        "details": "resolve_protein"
    },
    ProjectType: {
        "list": "project_types",
        "details": "resolve_project_type"
    },
    Metabolite: {
        "list": "metabolites",
        "details": "resolve_metabolite"
    },
    Gene: {
        "list": "genes",
        "details": "resolve_gene"
    }
}

EDGES : Dict[type, str] = {
    SampleAnalyteRelationship: "sample_analyte_edges",
    ProjectTypeRelationship: "project_type_edges",
    ExperimentInvestigatorRelationship: "experiment_investigator_edges",
    ExperimentSampleRelationship: "experiment_sample_edges",
    SampleBiospecimenRelationship: "sample_biospecimen_edges",
    ProjectExperimentRelationship: "project_experiment_edges",
    ProjectInvestigatorRelationship: "project_investigator_edges"
}


def Query(url):
    resolvers = generate_resolvers(ENDPOINTS, EDGES, url)
    return strawberry.type(type("Query", (), resolvers))
