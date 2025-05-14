from strawberry.schema.config import StrawberryConfig

import strawberry
from fastapi import FastAPI, Request
from strawberry.fastapi import GraphQLRouter

from src.api_adapters.strawberry_models.pounce_query_models import Query
from src.use_cases.build_from_yaml import HostDashboardFromYaml

yaml_file = "./src/use_cases/api/pounce_dev_dashboard.yaml"

dashboard = HostDashboardFromYaml(yaml_file=yaml_file)
api = dashboard.api_adapter

schema = strawberry.Schema(query=Query, config=StrawberryConfig(auto_camel_case=False))

def get_context(request: Request):
    return {"request": request, "api": api}

graphql_app = GraphQLRouter(schema, context_getter=get_context)

app = FastAPI()
app.include_router(graphql_app, prefix="/graphql")
