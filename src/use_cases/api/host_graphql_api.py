from src.api_adapters.graphene_utils import GrapheneConverter
from src.use_cases.build_from_yaml import HostDashboardFromYaml

yaml_file = "./src/use_cases/api/pharos_local_dashboard.yaml"
dashboard = HostDashboardFromYaml(yaml_file=yaml_file)
api = dashboard.api_adapter

import graphene
from flask import Flask
from graphql_server.flask import GraphQLView


class_map = api.get_class_map()
graphene_class_map = {}


for class_name, cls in class_map.items():
    print(f"Creating Graphene class for {class_name}")
    graphene_class_map[class_name] = GrapheneConverter.from_dataclass(cls)


def generic_resolver(data_model):
    def resolve(root, info):
        result = api.get_list(data_model, {}, 10, 0)
        return result.list
    return resolve

def create_query():
    fields = {
        f"{class_name.lower()}s":
        graphene.Field(
            graphene.List(graphene_class_map[class_name]),
            resolver=generic_resolver(class_name)
        )
    for class_name in class_map.keys()}
    return type('Query', (graphene.ObjectType,), fields)

# Dynamically create the Query class
Query = create_query()

# Create the schema
schema = graphene.Schema(query=Query)


app = Flask(__name__)
app.add_url_rule(
    "/graphql",
    view_func=GraphQLView.as_view("graphql", schema=schema, graphiql=True)
)
app.run()
