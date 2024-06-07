from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.util.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import InputAdapter
from src.input_adapters.sqlite_ramp.tables import Source as SqliteSource
from src.interfaces.merge_object import MergeObject
from src.models.analyte import Analyte, EquivalentId


class AnalyteEquivalentIDAdapter(InputAdapter, SqliteAdapter):
    name = "RaMP Analyte Equivalent ID Adapter"
    id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        InputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self):
        results = self.get_session().query(
            SqliteSource.rampId,
            SqliteSource.sourceId,
            SqliteSource.IDtype,
            SqliteSource.priorityHMDBStatus,
            SqliteSource.dataSource
        ).distinct()

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

    def next(self):
        nodes = self.get_all()
        for node in nodes:
            yield node
