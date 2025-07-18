from strawberry.schema.config import StrawberryConfig

import strawberry
from fastapi import FastAPI, Request, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from strawberry.fastapi import GraphQLRouter

from src.api_adapters.strawberry_models.pharos_query_models import Query as pharosQuery
from src.api_adapters.strawberry_models.pounce_query_models import Query as pounceQuery
from src.api_adapters.strawberry_models.gramp_query_models import Query as grampQuery

from src.use_cases.build_from_yaml import HostDashboardFromYaml

api_configs = [
    {
        "prefix": "pharos",
        "yaml_file": "./src/use_cases/api/pharos_dashboard.yaml",
        "query_function": pharosQuery
    },
    {
        "prefix": "pounce",
        "yaml_file": "./src/use_cases/api/pounce_dashboard.yaml",
        "query_function": pounceQuery
    },
    {
        "prefix": "gramp",
        "yaml_file": "./src/use_cases/api/gramp_dashboard.yaml",
        "query_function": grampQuery
    }
]

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def create_graphql_router(prefix, yaml_file, query_function):
    dashboard = HostDashboardFromYaml(yaml_file=yaml_file)
    api = dashboard.api_adapter
    url = dashboard.configuration.config_dict['api_adapter'][0]['credentials']['url']

    QueryClass = query_function(url)  # ✅ New class
    schema = strawberry.Schema(query=QueryClass, config=StrawberryConfig(auto_camel_case=False))

    def get_context(request: Request):
        return {"request": request, "api": api}

    graphql_router = GraphQLRouter(schema, context_getter=get_context)

    rest_router = APIRouter()
    for path, handler in api.get_rest_endpoints().items():
        rest_router.add_api_route(f"/rest/{prefix}/{path}", handler, methods=["GET"])

    return graphql_router, rest_router

# Now create and include each one
for api_config in api_configs:
    prefix = api_config["prefix"]
    yaml_file = api_config["yaml_file"]
    query_function = api_config["query_function"]

    graphql_router, rest_router = create_graphql_router(prefix, yaml_file, query_function)

    app.include_router(graphql_router, prefix=f"/graphql/{prefix}")
    app.include_router(rest_router)
