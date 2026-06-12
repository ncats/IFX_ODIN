# from src.use_cases.build_from_yaml import BuildGraphFromYaml
#
# etl_builder = BuildGraphFromYaml(yaml_file="./src/use_cases/working.yaml")
# etl_builder.prepare_datastore()
# etl_builder.do_etl()
from src.core.data_registry import DataRegistry
from src.registry.manifest import read_manifest

registry = DataRegistry.from_minio_credentials("src/use_cases/secrets/ifxdev_minio.yaml")

print(registry.list_derived_artifacts())
print(registry.check_derived_artifacts())
print(registry.sync_derived_artifacts(dest="/tmp/ifx-registry-cache", dry_run=True))
# print(registry.sync_derived_artifacts(dest="/tmp/ifx-registry-cache", dry_run=False))


# results = registry.sync_latest_snapshots(
#     dest="/private/tmp/ifx-registry-cache",
#     min_days_since_last_update=7,
#     dry_run=False,
# )
#
# results = registry.sync_ex
#
# if len(results) == 0:
#     print("everything's sync'd yo")
#
# for row in results:
#     print(row)
