import os
from src.models.gene import Gene, GeneticLocation
import strawberry
from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter

from src.models.node import EquivalentId
from src.use_cases.build_from_yaml import HostDashboardFromYaml

print(os.getcwd())

yaml_file = "./src/use_cases/api/pharos_prod_dashboard.yaml"
dashboard = HostDashboardFromYaml(yaml_file=yaml_file)
api = dashboard.api_adapter

EquivalentIdType = strawberry.type(EquivalentId)
GeneticLocationType = strawberry.type(GeneticLocation)
GeneType = strawberry.type(Gene)

@strawberry.type
class Query:
    @strawberry.field
    def gene(self, id: str) -> GeneType:

        res = api.get_details("Gene", id="IFXGene:05TD54T")

        return Gene.from_dict(res.details)

schema = strawberry.Schema(query=Query)

graphql_app = GraphQLRouter(schema)

app = FastAPI()
app.include_router(graphql_app, prefix="/graphql")


#
# import graphene
# from flask import Flask
# from graphql_server.flask import GraphQLView
#
# from src.api_adapters.graphene_utils import GrapheneConverter
# from src.models.protein import Protein
# from tests.test_classes import SimpleFieldClass, ListFieldClass, ChildClass, ParentClass, ChildClass2, ClassWithEnum, \
#     Category, ClassWithNestedEnumClass, ClassWithUnion
#
#
# gSimpleClass = GrapheneConverter.from_dataclass(SimpleFieldClass)
# gListClass = GrapheneConverter.from_dataclass(ListFieldClass)
# gChildClass = GrapheneConverter.from_dataclass(ChildClass)
# gChildClass2 = GrapheneConverter.from_dataclass(ChildClass2)
# gParentClass = GrapheneConverter.from_dataclass(ParentClass)
# gEnumClass = GrapheneConverter.from_dataclass(ClassWithEnum)
# gNestedEnumClass = GrapheneConverter.from_dataclass(ClassWithNestedEnumClass)
# gUnionClass = GrapheneConverter.from_dataclass(ClassWithUnion)
# gProtein = GrapheneConverter.from_dataclass(Protein)
#
# def resolve_parent(root, info):
#     if root:
#         if isinstance(root, ChildClass):
#             return ParentClass(id=f"parent_of_{root.id}", data="some_data")
#         elif isinstance(root, ChildClass2):
#             return [ParentClass(id=f"parent_of_{root.id}", data="some_data")]
#         return None
#     else:
#         parent = ParentClass(id="new one", data="new_value")
#         return parent
#
# gChildClass.resolve_parent = resolve_parent
# gChildClass2.resolve_parents = resolve_parent
#
# class Query(graphene.ObjectType):
#     simple_field = graphene.Field(gSimpleClass, description="A simple field")
#     list_field = graphene.Field(gListClass, description="A list field")
#     child_field = graphene.Field(gChildClass, description="A child field")
#     child_field2 = graphene.Field(gChildClass2, description="A child field 2")
#     parent_field = graphene.Field(gParentClass, description="A parent field")
#     class_with_enum = graphene.Field(gEnumClass, description="A class with enum")
#     class_with_nested_enum_class = graphene.Field(gNestedEnumClass, description="A class with nested enum class")
#     class_with_union = graphene.Field(gUnionClass, description="A class with union")
#     protein = graphene.Field(gProtein, description="A protein")
#
#
#     def resolve_simple_field(root, info):
#         return SimpleFieldClass(id="id",num=2,yes=False,dec=2)
#
#     def resolve_list_field(root, info):
#         return ListFieldClass(id=["id1", "id2"], num=[1, 2], yes=[True, False], dec=[1.0, 2.0])
#
#     def resolve_child_field(root, info):
#         child = ChildClass(id="child_id", data="child_data")
#         return child
#
#     def resolve_child_field2(root, info):
#         child = ChildClass2(id="child_id")
#         return child
#
#     def resolve_parent_field(root, info):
#         return resolve_parent(root, info)
#
#     def resolve_class_with_enum(root, info):
#         return ClassWithEnum(id="id", cat=Category.B, cats=[Category.A, Category.C])
#
#     def resolve_class_with_nested_enum_class(root, info):
#         return ClassWithNestedEnumClass(id="id", cwe=ClassWithEnum(id="id2", cat=Category.C, cats=[Category.B, Category.B]))
#
#     def resolve_class_with_union(root, info):
#         return ClassWithUnion(id="id", optional="optional_value", mixed=3,
#                               two_dc=SimpleFieldClass(id="id", num=1, yes=True, dec=1.0),
#                               two_mixed=SimpleFieldClass(id="id", num=1, yes=True, dec=1.0))
#
#     def resolve_protein(root, info):
#         return Protein(id="id", name="protein_name", sequence="ACGT")
#
# # Create the schema
# schema = graphene.Schema(query=Query)
#
# app = Flask(__name__)
# app.add_url_rule(
#     "/graphql",
#     view_func=GraphQLView.as_view("graphql", schema=schema, graphiql=True)
# )
# app.run()
