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

for api_config in api_configs:
    yaml_file = api_config["yaml_file"]
    prefix = api_config["prefix"]
    query_function = api_config["query_function"]

    dashboard = HostDashboardFromYaml(yaml_file=yaml_file)
    api = dashboard.api_adapter
    url = dashboard.configuration.config_dict['api_adapter'][0]['credentials']['url']
    Query = query_function(url)

    schema = strawberry.Schema(query=Query, config=StrawberryConfig(auto_camel_case=False))

    def get_context(request: Request):
        return {"request": request, "api": api}

    graphql_app = GraphQLRouter(schema, context_getter=get_context)

    app.include_router(graphql_app, prefix=f"/graphql/{prefix}")

    rest_endpoints = api.get_rest_endpoints()
    router = APIRouter()

    for path, handler in rest_endpoints.items():
        router.add_api_route(f"/rest/{prefix}/{path}", handler, methods=["GET"])
    app.include_router(router)
