from dataclasses import dataclass, field
from typing import Optional, List, TYPE_CHECKING

from src.core.decorators import facets
from src.models.node import Node, Relationship

if TYPE_CHECKING:
    from src.models.pounce.experiment import Experiment
    from src.models.gene import Gene
    from src.models.metabolite import Metabolite
    import pandas as pd


@dataclass
@facets(category_fields=['data_type'])
class StatsResult(Node):
    data_type: Optional[str] = None
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    analyte_id_column: Optional[str] = None
    file_reference: Optional[str] = None
    comparison_columns: List[str] = field(default_factory=list)
    _data_frame: Optional["pd.DataFrame"] = field(default=None, repr=False)


@dataclass
class ExperimentStatsResultEdge(Relationship):
    start_node: "Experiment" = None
    end_node: "StatsResult" = None


@dataclass
class StatsResultGeneEdge(Relationship):
    start_node: "StatsResult" = None
    end_node: "Gene" = None


@dataclass
class StatsResultMetaboliteEdge(Relationship):
    start_node: "StatsResult" = None
    end_node: "Metabolite" = None