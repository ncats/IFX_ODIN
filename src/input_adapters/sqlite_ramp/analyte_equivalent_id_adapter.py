from abc import abstractmethod, ABC

from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import NodeInputAdapter
from src.input_adapters.sqlite_ramp.tables import Source as SqliteSource
from src.interfaces.merge_object import MergeObject
from src.models.analyte import Analyte, EquivalentId


class AnalyteEquivalentIDAdapter(NodeInputAdapter, SqliteAdapter, ABC):
    name = "RaMP Analyte Equivalent ID Adapter"
    id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        NodeInputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    @abstractmethod
    def get_id_prefix(self) -> str:
        pass

    def get_all(self):
        results = self.get_session().query(
            SqliteSource.rampId,
            SqliteSource.sourceId,
            SqliteSource.IDtype,
            SqliteSource.priorityHMDBStatus,
            SqliteSource.dataSource
        ).filter(SqliteSource.rampId.startswith(self.get_id_prefix())).distinct()

        analyte_dict = {}
        for row in results:
            ramp_id = row[0]
            if ramp_id in analyte_dict:
                analyte_dict[ramp_id].append(row)
            else:
                analyte_dict[ramp_id] = [row]

        objs: [MergeObject] = [
            MergeObject(
                obj=Analyte(id=key,
                            equivalent_ids=[EquivalentId(
                                id=row[1],
                                type=row[2],
                                status=row[3],
                                source=row[4]
                            ) for row in equivalent_ids]), field="equivalent_ids"
            )
            for key, equivalent_ids in analyte_dict.items()
        ]
        return objs


class MetaboliteEquivalentIDAdapter(AnalyteEquivalentIDAdapter):
    name = "RaMP Metabolite Equivalent ID Adapter"
    start_id_normalizer = PassthroughNormalizer()

    def get_id_prefix(self) -> str:
        return "RAMP_C"


class ProteinEquivalentIDAdapter(AnalyteEquivalentIDAdapter):
    name = "RaMP Protein Equivalent ID Adapter"
    start_id_normalizer = PassthroughNormalizer()  # this should be node-norm

    def get_id_prefix(self) -> str:
        return "RAMP_G"
