from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.util.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import InputAdapter
from src.input_adapters.sqlite_ramp.tables import AnalyteSynonym as SqliteAnalyteSynonym
from src.interfaces.merge_object import MergeObject
from src.models.analyte import Analyte, AnalyteSynonym


class AnalyteSynonymAdapter(InputAdapter, SqliteAdapter):
    name = "RaMP Analyte Synonym Adapter"
    id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        InputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    def get_all(self):
        results = self.get_session().query(
            SqliteAnalyteSynonym.rampId,
            SqliteAnalyteSynonym.Synonym,
            SqliteAnalyteSynonym.source
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
                            synonyms=[AnalyteSynonym(
                                term=row[1], source=row[2]
                            ) for row in synonym_list]), field="synonyms"
            )
            for key, synonym_list in analyte_dict.items()
        ]
        return objs

    def next(self):
        nodes = self.get_all()
        for node in nodes:
            yield node
