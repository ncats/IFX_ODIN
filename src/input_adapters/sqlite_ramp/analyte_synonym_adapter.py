from abc import ABC, abstractmethod

from src.id_normalizers.passthrough_normalizer import PassthroughNormalizer
from src.input_adapters.sqlite_adapter import SqliteAdapter
from src.interfaces.input_adapter import NodeInputAdapter
from src.input_adapters.sqlite_ramp.tables import AnalyteSynonym as SqliteAnalyteSynonym
from src.interfaces.merge_object import MergeObject
from src.models.analyte import Analyte, Synonym


class AnalyteSynonymAdapter(NodeInputAdapter, SqliteAdapter, ABC):
    name = "RaMP Analyte Synonym Adapter"
    id_normalizer = PassthroughNormalizer()

    def __init__(self, sqlite_file):
        NodeInputAdapter.__init__(self)
        SqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    @abstractmethod
    def get_id_prefix(self) -> str:
        pass

    def get_all(self):
        results = self.get_session().query(
            SqliteAnalyteSynonym.rampId,
            SqliteAnalyteSynonym.Synonym,
            SqliteAnalyteSynonym.source
        ).filter(SqliteAnalyteSynonym.rampId.startswith(self.get_id_prefix())).distinct()

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
                            synonyms=[Synonym(
                                term=row[1], source=row[2]
                            ) for row in synonym_list]), field="synonyms"
            )
            for key, synonym_list in analyte_dict.items()
        ]
        return objs


class MetaboliteSynonymAdapter(AnalyteSynonymAdapter):
    name = "RaMP Metabolite Synonym Adapter"
    id_normalizer = PassthroughNormalizer()

    def get_id_prefix(self) -> str:
        return "RAMP_C"


class ProteinSynonymAdapter(AnalyteSynonymAdapter):
    name = "RaMP Protein Synonym Adapter"
    id_normalizer = PassthroughNormalizer()

    def get_id_prefix(self) -> str:
        return "RAMP_G"
