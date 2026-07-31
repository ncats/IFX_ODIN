from pathlib import Path
import re
from typing import Dict, Generator, List, Optional, Set, Union

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.metabolite_harmonization import RheaReactionClass, RheaReactionClassParentEdge


ENZCLASS_PATTERN = re.compile(r"^\s*(\d+)\.\s*([0-9-]+)\.\s*([0-9-]+)\.\s*([0-9-]+)\s+(.+?)\s*$")
EC_ID_PATTERN = re.compile(r"^\d+\.(?:\d+|-)\.(?:\d+|-)\.(?:\d+|-|n\d+)$")


class ExpasyEnzymeClassAdapter(InputAdapter):
    def __init__(
        self,
        data_source=None,
        enzclass_file: Optional[str] = None,
        enzyme_dat_file: Optional[str] = None,
        max_records: Optional[int] = None,
    ):
        if data_source is not None:
            enzclass_file = str(data_source.file("enzclass.txt"))
            enzyme_dat_file = str(data_source.file("enzyme.dat"))
            self.version_info = data_source.version_info()
        else:
            self.version_info = DatasourceVersionInfo(version=None, version_date=None, download_date=None)
        if enzclass_file is None or enzyme_dat_file is None:
            raise ValueError("ExpasyEnzymeClassAdapter requires data_source or enzclass_file and enzyme_dat_file")
        self.enzclass_file = Path(enzclass_file)
        self.enzyme_dat_file = Path(enzyme_dat_file)
        self.max_records = max_records

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.ExPASy

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def get_all(self) -> Generator[List[Union[RheaReactionClass, RheaReactionClassParentEdge]], None, None]:
        class_names = self._class_names()
        class_ids = self._expanded_class_ids(class_names)

        batch: List[Union[RheaReactionClass, RheaReactionClassParentEdge]] = []
        for class_id in sorted(class_ids, key=_ec_sort_key):
            batch.append(
                RheaReactionClass(
                    id=f"EC:{class_id}",
                    ec_level=_ec_level(class_id),
                    name=class_names.get(class_id),
                )
            )
            if len(batch) >= self.batch_size:
                yield batch
                batch = []

        for class_id in sorted(class_ids, key=_ec_sort_key):
            parent_id = _parent_ec(class_id)
            if parent_id is None:
                continue
            batch.append(
                RheaReactionClassParentEdge(
                    start_node=RheaReactionClass(id=f"EC:{class_id}"),
                    end_node=RheaReactionClass(id=f"EC:{parent_id}"),
                )
            )
            if len(batch) >= self.batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

    def _class_names(self) -> Dict[str, str]:
        names = {}
        names.update(self._parse_enzclass_names())
        names.update(self._parse_enzyme_dat_names())
        if self.max_records is not None:
            return dict(list(names.items())[: self.max_records])
        return names

    def _parse_enzclass_names(self) -> Dict[str, str]:
        names = {}
        with self.enzclass_file.open(encoding="utf-8") as handle:
            for raw_line in handle:
                match = ENZCLASS_PATTERN.match(raw_line)
                if not match:
                    continue
                ec_id = ".".join(match.groups()[:4])
                names[ec_id] = match.group(5).strip()
        return names

    def _parse_enzyme_dat_names(self) -> Dict[str, str]:
        names = {}
        current_id = None
        current_name = None
        with self.enzyme_dat_file.open(encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.rstrip("\n")
                if line.startswith("ID"):
                    current_id = line[5:].strip()
                    if not _is_standard_ec_id(current_id):
                        current_id = None
                    current_name = None
                elif line.startswith("DE") and current_id is not None and current_name is None:
                    current_name = line[5:].strip()
                elif line == "//":
                    if current_id and current_name:
                        names[current_id] = current_name
                    current_id = None
                    current_name = None
        return names

    @staticmethod
    def _expanded_class_ids(class_names: Dict[str, str]) -> Set[str]:
        class_ids = set(class_names)
        for class_id in list(class_names):
            current = class_id
            while True:
                parent = _parent_ec(current)
                if parent is None:
                    break
                class_ids.add(parent)
                current = parent
        return class_ids


def _parent_ec(ec_id: str) -> Optional[str]:
    if not _is_standard_ec_id(ec_id):
        return None
    parts = ec_id.split(".")
    if len(parts) != 4:
        return None
    if parts[1:] == ["-", "-", "-"]:
        return None
    for index in range(3, 0, -1):
        if parts[index] != "-":
            parent = list(parts)
            for parent_index in range(index, 4):
                parent[parent_index] = "-"
            return ".".join(parent)
    return None


def _ec_level(ec_id: str) -> int:
    return 4 - ec_id.count("-")


def _ec_sort_key(ec_id: str):
    key = []
    for part in ec_id.split("."):
        if part == "-":
            key.append((-1, 0))
        elif part.startswith("n") and part[1:].isdigit():
            key.append((1, int(part[1:])))
        else:
            key.append((0, int(part)))
    return key


def _is_standard_ec_id(ec_id: Optional[str]) -> bool:
    return bool(ec_id and EC_ID_PATTERN.match(ec_id))
