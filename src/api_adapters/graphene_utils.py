import dataclasses
import datetime
import enum
import typing

import graphene

_type_mapping = {
    int: graphene.Int,
    str: graphene.String,
    bool: graphene.Boolean,
    float: graphene.Float,
    datetime: graphene.DateTime,
    datetime.date: graphene.Date,
}

def get_graphene_field(field_type):

    if typing.get_origin(field_type) is typing.Union:
        args = typing.get_args(field_type)
        # Find the non-None type, assuming Optional is Union[T, None]
        non_null_types = [arg for arg in args if arg is not type(None)]
        if len(non_null_types) == 1:
            actual_type = non_null_types[0]
            return get_graphene_field(actual_type)
        return graphene.Field(graphene.String)

    elif typing.get_origin(field_type) is list:
        args = typing.get_args(field_type)
        list_item_type = args[0]
        graphene_list_type = GrapheneConverter._python_type_to_graphene_type(list_item_type)
        if graphene_list_type is None:
            if isinstance(list_item_type, type) and issubclass(list_item_type, enum.Enum):
                graphene_list_type = graphene.Enum.from_enum(list_item_type)
            elif typing.get_origin(list_item_type) is typing.Union:
                graphene_list_type = graphene.String
            else:
                graphene_list_type = GrapheneConverter.from_dataclass(list_item_type)
        graphene_type = graphene.List(graphene_list_type)
        return graphene.Field(graphene_type)
    elif dataclasses.is_dataclass(field_type):
        return graphene.Field(GrapheneConverter.from_dataclass(field_type))
    elif isinstance(field_type, type) and issubclass(field_type, enum.Enum):
        return graphene.Field(graphene.Enum.from_enum(field_type))
    else:
        graphene_type = GrapheneConverter._python_type_to_graphene_type(field_type)
        return graphene.Field(graphene_type)

class GrapheneConverter:

    @staticmethod
    def from_dataclass(dataclass_: dataclasses.dataclass):
        print(f"Converting {dataclass_.__name__} to Graphene ObjectType")
        if not dataclasses.is_dataclass(dataclass_):
            raise TypeError("Input must be a dataclass.")

        name = dataclass_.__name__
        fields = {}

        for field in dataclasses.fields(dataclass_):
            print(f"Processing field: {field.name} of type {field.type}")
            field_type = field.type
            graphene_field = get_graphene_field(field_type)

            if graphene_field is not None:
                print(f"Converted field: {field.name} of type {graphene_field.type}")
                fields[field.name] = graphene_field


        return type(name, (graphene.ObjectType,), fields)


    @staticmethod
    def _python_type_to_graphene_type(python_type):
        return _type_mapping.get(python_type)