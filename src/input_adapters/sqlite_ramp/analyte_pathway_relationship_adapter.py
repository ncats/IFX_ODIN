from abc import ABC, abstractmethod
from typing import List, Generator, Union

from src.constants import Prefix
from src.input_adapters.sqlite_ramp.ramp_sqlite_adapter import RaMPSqliteAdapter
from src.input_adapters.sqlite_ramp.tables import AnalytePathwayRelationship as SqliteAnalytePathwayRelationship, Pathway as SqlitePathway, Analyte as SqliteAnalyte, Source as SqliteSource
from src.models.gene import Gene
from src.models.metabolite import Metabolite
from src.models.node import EquivalentId
from src.models.pathway import GenePathwayRelationship, MetabolitePathwayRelationship, ProteinPathwayRelationship, Pathway
from src.models.protein import Protein


class AnalytePathwayRelationshipAdapter(RaMPSqliteAdapter, ABC):

    def __init__(self, sqlite_file):
        RaMPSqliteAdapter.__init__(self, sqlite_file=sqlite_file)

    @abstractmethod
    def get_id_prefix(self) -> str:
        pass

    @abstractmethod
    def get_all(self):
        pass


class MetabolitePathwayRelationshipAdapter(AnalytePathwayRelationshipAdapter):
    def get_id_prefix(self) -> str:
        return "RAMP_C"

    def get_all(self) -> Generator[List[MetabolitePathwayRelationship], None, None]:
        results = self.get_session().query(
            SqliteAnalytePathwayRelationship.rampId,
            SqliteAnalytePathwayRelationship.pathwayRampId,
            SqliteAnalytePathwayRelationship.pathwaySource
        ).filter(SqliteAnalytePathwayRelationship.rampId.startswith(self.get_id_prefix())).all()

        yield [
            MetabolitePathwayRelationship(
                start_node=Metabolite(id=row[0]),
                end_node=Pathway(id=row[1]),
                source=row[2]
            ) for row in results
        ]


class ProteinPathwayRelationshipAdapter(AnalytePathwayRelationshipAdapter):
    def get_id_prefix(self) -> str:
        return "RAMP_G"

    def get_all(self) -> Generator[List[ProteinPathwayRelationship], None, None]:
        results = self.get_session().query(
            SqliteAnalytePathwayRelationship.rampId,
            SqliteAnalytePathwayRelationship.pathwayRampId,
            SqliteAnalytePathwayRelationship.pathwaySource
        ).filter(SqliteAnalytePathwayRelationship.rampId.startswith(self.get_id_prefix())).all()

        yield [
            ProteinPathwayRelationship(
                start_node=Protein(id=row[0]),
                end_node=Pathway(id=row[1]),
                source=row[2]
            ) for row in results
        ]


class EnsemblGenePathwayRelationshipAdapter(AnalytePathwayRelationshipAdapter):
    def get_id_prefix(self) -> str:
        return "RAMP_G"

    def get_all(self) -> Generator[List[GenePathwayRelationship], None, None]:
        results = (
            self.get_session()
            .query(
                SqliteAnalytePathwayRelationship.pathwayRampId,
                SqliteSource.sourceId,
                SqliteAnalytePathwayRelationship.pathwaySource
            )
            .join(
                SqliteAnalyte,
                SqliteAnalyte.rampId == SqliteAnalytePathwayRelationship.rampId
            )
            .join(
                SqliteSource,
                SqliteSource.rampId == SqliteAnalyte.rampId
            )
            .filter(
                SqliteAnalyte.type == "gene",
                SqliteSource.IDtype == "ensembl"
            )
            .order_by(SqliteAnalytePathwayRelationship.pathwayRampId)
            .all()
        )

        relationships: List[GenePathwayRelationship] = []
        for row in results:
            gene_id = row[1].split(":")[1]
            ensembl_gene_id = EquivalentId(id=gene_id, type=Prefix.ENSEMBL).id_str()
            relationships.append(
                GenePathwayRelationship(
                    start_node=Gene(id=ensembl_gene_id),
                    end_node=Pathway(id=row[0]),
                    source=row[2]
                )
            )
        yield relationships