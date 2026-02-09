from typing import List

from src.input_adapters.neo4j_adapter import Neo4jAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.node import Node


class PounceEffectSizeCalculator(InputAdapter, Neo4jAdapter):
    accession: str

    def __init__(self, accession: str, **kwargs):
        Neo4jAdapter.__init__(self, **kwargs)
        self.accession = accession

    def get_all(self) -> List[Node]:

        query = "match (p:Project {id:'GEO:" + self.accession + "'})-[r*1..3]-(exp:ExperimentalConditionSet) return distinct exp.biospecimen, exp.treatment, exp.platform"
        res = self.runQuery(query)
        for row in res:
            print(row)
        return []