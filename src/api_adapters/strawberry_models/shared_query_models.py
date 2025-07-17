from dataclasses import field
from typing import Optional, List, Type, Dict

import strawberry
from strawberry import Info
from strawberry.scalars import JSON

from src.interfaces.metadata import CollectionMetadata as CollectionMetadataBase, DatabaseMetadata as DatabaseMetadataBase
from src.api_adapters.strawberry_models.class_generators import make_list_result_type, make_resolve_result_type
from src.api_adapters.strawberry_models.input_types import ListFilterSettings, ListQueryContext
from src.models.datasource_version_info import parse_to_date, DataSourceDetails

from src.shared.record_merger import FieldConflictBehavior

DataSourceDetails = strawberry.type(DataSourceDetails)

@strawberry.type
class KeyValuePair:
    key: str
    value: int


@strawberry.type
class CollectionMetadata(CollectionMetadataBase):
    sources: List[DataSourceDetails] = field(default_factory=list)
    marginal_source_counts: List[KeyValuePair] = field(default_factory=list)
    joint_source_counts: List[KeyValuePair] = field(default_factory=list)

@strawberry.type
class DatabaseMetadata(DatabaseMetadataBase):
    collections: List[CollectionMetadata]
    url: str


@strawberry.type
class FieldUpdateDetails(DataSourceDetails):
    field: str
    old_value: Optional[str]
    new_value: str
    conflict_behavior: Optional[FieldConflictBehavior]
    @staticmethod
    def parse_tsv(tsv_str: str) -> "FieldUpdateDetails":
        field, old_value, new_value, name, version, version_date, download_date, *conflict_behavior = tsv_str.split('\t')
        conflict_behavior = conflict_behavior[0] if conflict_behavior else None
        dsv = FieldUpdateDetails(
            field=field,
            old_value=None if old_value == 'NULL' else old_value,
            new_value=new_value,
            name=name,
            version=version if version and version != 'None' else None,
            version_date=parse_to_date(version_date),
            download_date=parse_to_date(download_date),
            conflict_behavior=FieldConflictBehavior.parse(conflict_behavior)
        )
        return dsv


@strawberry.type
class Provenance:
    creation: DataSourceDetails
    updates: Optional[List[FieldUpdateDetails]]

    @staticmethod
    def parse_provenance_fields(root) -> "Provenance":
        ds_str = getattr(root, "creation")
        creation_details = DataSourceDetails.parse_tsv(ds_str)
        update_strs = getattr(root, "updates")
        return Provenance(
            creation=creation_details,
            updates=[FieldUpdateDetails.parse_tsv(line) for line in update_strs if not line.startswith('labels')] if update_strs else None)

def generate_details_resolver(source_data_model: Type, sortby = {}):
    class_name = source_data_model.__name__
    return_type = make_resolve_result_type(f"{class_name}ResolveResult", source_data_model)
    @strawberry.field()
    def resolver(self, info: Info, id: str) -> return_type:
        api = info.context["api"]
        return api.resolve_id(class_name, id=id, sortby=sortby)
    return resolver


def generate_list_resolver(source_data_model: Type):
    class_name = source_data_model.__name__
    return_type = make_list_result_type(f"{class_name}ListResult", source_data_model)
    @strawberry.field()
    def resolver(self, info: Info, filter: Optional[ListFilterSettings] = None) -> return_type:
        api = info.context["api"]
        context = ListQueryContext(source_data_model=class_name, filter=filter)
        return api.get_list_obj(context)
    return resolver



def create_edge_collections_class(EDGES: Dict[type, str]):
    namespace = {}
    for model, name in EDGES.items():
        namespace[name] = generate_list_resolver(model)
    return strawberry.type(type("EdgeCollections", (), namespace))

def create_edge_collection(EDGES: Dict[type, str]):
    EdgeCollections = create_edge_collections_class(EDGES)

    # Resolver for the edges root field
    @strawberry.field()
    def edges() -> EdgeCollections:
        return EdgeCollections()

    return edges


@strawberry.field()
def etl_metadata(info: Info) -> Optional[JSON]:
    api = info.context['api']
    result = api.get_etl_metadata()
    if result is None:
        return {}
    return result

def generate_resolvers(ENDPOINTS: Dict[type, Dict[str, str]], EDGES: Dict[type, str], url):

    @strawberry.field()
    def database_metadata(info: Info) -> DatabaseMetadata:
        api = info.context["api"]
        result: DatabaseMetadata = api.get_metadata()
        for coll in result.collections:
            coll.marginal_source_counts = [KeyValuePair(key=k, value=v) for k,v in coll.marginal_source_counts.items()]
            coll.joint_source_counts = [KeyValuePair(key=k, value=v) for k,v in coll.joint_source_counts.items()]
        result.url = url
        return result


    global resolvers
    list_resolvers = {
        info["list"]: generate_list_resolver(model_cls)
        for model_cls, info in ENDPOINTS.items()
        if "list" in info
    }
    details_resolvers = {
        info["details"]: generate_details_resolver(model_cls)
        for model_cls, info in ENDPOINTS.items()
        if "details" in info
    }
    edge_resolvers = {'edges': create_edge_collection(EDGES)} if EDGES else {}

    resolvers = {
        **list_resolvers,
        **details_resolvers,
        **edge_resolvers,
        'database_metadata': database_metadata,
        'etl_metadata': etl_metadata
    }
    return resolvers