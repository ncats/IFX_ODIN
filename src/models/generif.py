from dataclasses import dataclass, field
from datetime import datetime

from src.models.gene import Gene
from src.models.node import Node, Relationship


@dataclass
class GeneRif(Node):
    text: str = None


@dataclass
class GeneGeneRifRelationship(Relationship):
    start_node = Gene
    end_node = GeneRif
    gene_id: int = None
    date: datetime = None
    pmids: list[str] = field(default_factory=list)

    def _identity_tuple(self):
        start_id = getattr(self.start_node, "id", None)
        end_id = getattr(self.end_node, "id", None)
        pmids_key = tuple(sorted(self.pmids)) if self.pmids else tuple()
        return (start_id, end_id, self.gene_id, self.date, pmids_key)

    def __eq__(self, other):
        if not isinstance(other, GeneGeneRifRelationship):
            return NotImplemented
        return self._identity_tuple() == other._identity_tuple()

    def __hash__(self):
        return hash(self._identity_tuple())