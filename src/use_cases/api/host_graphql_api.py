import os

from strawberry.schema.config import StrawberryConfig

import strawberry
from fastapi import FastAPI, Request, APIRouter
from fastapi.middleware.cors import CORSMiddleware
from strawberry.fastapi import GraphQLRouter

from src.api_adapters.strawberry_models.pharos_query_models import Query as pharosQuery
from src.api_adapters.strawberry_models.pounce_query_models import Query as pounceQuery

from src.use_cases.build_from_yaml import HostDashboardFromYaml

def create_app(yaml_file: str) -> FastAPI:
    dashboard = HostDashboardFromYaml(yaml_file=yaml_file)
    api = dashboard.api_adapter
    url = dashboard.configuration.config_dict['api_adapter'][0]['credentials']['url']
    if yaml_file.find('pounce') >= 0:
        Query = pounceQuery(url)
    else:
        Query = pharosQuery(url)

    schema = strawberry.Schema(query=Query,
                               config=StrawberryConfig(
                                   auto_camel_case=False
                               ))

    def get_context(request: Request):
        return {"request": request, "api": api}

    graphql_app = GraphQLRouter(schema, context_getter=get_context)

    app = FastAPI()

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(graphql_app, prefix="/graphql")

    rest_endpoints = api.get_rest_endpoints()
    router = APIRouter()

    for path, handler in rest_endpoints.items():
        router.add_api_route(f"/{path}", handler, methods=["GET"])
    app.include_router(router)

    return app

yaml_path = os.environ.get("YAML_FILE", "./src/use_cases/api/pharos_prod_dashboard.yaml")
app = create_app(yaml_file=yaml_path)
