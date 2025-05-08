from src.api_adapters.strawberry_models.query_models import ResolveProteinResult, ResolveGeneResult, \
    ResolveTranscriptResult, ResolveLigandResult, GoTermResult, Gene
import strawberry
from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter

from src.use_cases.build_from_yaml import HostDashboardFromYaml

yaml_file = "./src/use_cases/api/pharos_prod_dashboard.yaml"
dashboard = HostDashboardFromYaml(yaml_file=yaml_file)
api = dashboard.api_adapter

@strawberry.type
class Query:
    @strawberry.field
    def resolve_protein(self, id: str) -> ResolveProteinResult:
        result = api.resolve_id("Protein", id=id, sortby={"uniprot_reviewed": "desc", "uniprot_canonical": "desc", "mapping_ratio": "desc"})
        return result

    @strawberry.field
    def resolve_gene(self, id: str) -> ResolveGeneResult:
        result = api.resolve_id("Gene", id=id, sortby={"mapping_ratio": "desc"})
        return result

    @strawberry.field
    def resolve_transcript(self, id: str) -> ResolveTranscriptResult:
        result = api.resolve_id("Transcript", id=id, sortby={"mapping_ratio": "desc"})
        return result

    @strawberry.field
    def resolve_ligand(self, id: str) -> ResolveLigandResult:
        result = api.resolve_id("Ligand", id=id)
        return result

    @strawberry.field
    def go_term(self, id: str) -> GoTermResult:
        result = api.get_details("GoTerm", id=id)
        return result


schema = strawberry.Schema(query=Query)

graphql_app = GraphQLRouter(schema)

app = FastAPI()
app.include_router(graphql_app, prefix="/graphql")
