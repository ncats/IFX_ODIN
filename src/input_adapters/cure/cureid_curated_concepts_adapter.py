import csv
from typing import Generator, List

from src.constants import DataSourceName
from src.input_adapters.cure.cureid_tsv_lookup import normalize_cureid_label
from src.input_adapters.flat_file_adapter import FlatFileAdapter
from src.models.cure.pasc.condition import Condition
from src.models.cure.rasopathies.drug import Drug
from src.models.cure.rasopathies.phenotype import Phenotype
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import Node


class CureIdCuratedConceptsAdapter(FlatFileAdapter):
    def __init__(self, data_source):
        file_path = str(data_source.file())
        super().__init__(file_path=file_path)
        self.version_info = data_source.version_info()

    def get_all(self) -> Generator[List[Node], None, None]:
        batch: List[Node] = []
        emitted_keys: set[tuple[str, str]] = set()

        with open(self.file_path, newline="") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            for row in reader:
                for side in ("subject", "object"):
                    source_type = normalize_cureid_label(row.get(f"{side}_type"))
                    final_label = normalize_cureid_label(row.get(f"{side}_final_label"))
                    curie = normalize_cureid_label(row.get(f"{side}_final_curie"))
                    if source_type is None or curie is None:
                        continue

                    node = None
                    if source_type == "Disease":
                        node = Condition(id=curie, name=final_label)
                    elif source_type == "Drug":
                        node = Drug(id=curie, name=final_label)
                    elif source_type in {"PhenotypicFeature", "AdverseEvent"}:
                        node = Phenotype(id=curie, name=final_label)

                    if node is None:
                        continue

                    emitted_key = (node.__class__.__name__, node.id)
                    if emitted_key in emitted_keys:
                        continue
                    emitted_keys.add(emitted_key)
                    batch.append(node)

                    if len(batch) >= self.batch_size:
                        yield batch
                        batch = []

        if batch:
            yield batch

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.CURE

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info
