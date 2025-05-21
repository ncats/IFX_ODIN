from datetime import date, datetime
from typing import Optional, List, Type, Dict

import strawberry
from strawberry import Info

from src.api_adapters.strawberry_models.class_generators import make_list_result_type, make_resolve_result_type
from src.api_adapters.strawberry_models.input_types import ListFilterSettings, ListQueryContext

from src.shared.record_merger import FieldConflictBehavior


@strawberry.type
class DataSourceDetails:
    name: str
    version: Optional[str]
    version_date: Optional[date]
    download_date: Optional[date]

    @staticmethod
    def parse_tsv(tsv_str: str) -> "DataSourceDetails":
        name, version, version_date, download_date = tsv_str.split('\t')
        dsv = DataSourceDetails(
            name=name,
            version=version if version else None,
            version_date=parse_to_date(version_date),
            download_date=parse_to_date(download_date)
        )
        return dsv


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


def parse_to_date(iso_format_str: str) -> Optional[date]:
    if iso_format_str is None:
        return None
    if iso_format_str == 'None':
        return None
    if len(iso_format_str) > 10:
        dt = datetime.fromisoformat(iso_format_str)
        return date(dt.year, dt.month, dt.day)
    return date.fromisoformat(iso_format_str)


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


def generate_resolvers(ENDPOINTS: Dict[type, Dict[str, str]]):
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
    resolvers = {
        **list_resolvers,
        **details_resolvers
    }
    return resolvers