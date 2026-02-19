import re
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Tuple, TYPE_CHECKING, Dict

from src.core.decorators import facets
from src.models.node import Node, Relationship

if TYPE_CHECKING:
    from src.models.pounce.experiment import Experiment
    from src.models.pounce.project import Person
    from src.models.gene import Gene
    from src.models.metabolite import Metabolite
    import pandas as pd

# Ordered longest-first to avoid prefix collisions (e.g. "ES" matching "ESPval")
_STAT_TYPE_PREFIXES = [
    ("ESadjPval", "adjusted_pvalue"),
    ("ESPval", "pvalue"),
    ("ES2", "effect_size_2"),
    ("ES", "effect_size"),
]


@dataclass
class ComparisonColumn:
    column_name: str
    stat_type: Optional[str] = None
    group1: Optional[str] = None
    group2: Optional[str] = None
    comparison: Optional[str] = None
    properties: List[str] = field(default_factory=list)
    notes: Optional[str] = None

    def to_dict(self) -> Dict[str, str]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)

    @staticmethod
    def _normalize(s: str) -> str:
        """Collapse consecutive underscores to a single underscore."""
        return re.sub(r'_+', '_', s)

    @staticmethod
    def parse_stat_type(column_name: str) -> Optional[str]:
        for prefix, stat_type in _STAT_TYPE_PREFIXES:
            if column_name.startswith(prefix):
                return stat_type
        return None

    @staticmethod
    def parse_groups(column_name: str) -> Tuple[Optional[str], Optional[str]]:
        """Strip the stat-type prefix then split on _vs_ (case-insensitive).

        ES_DENV3_vs_Mock  → group1="DENV3", group2="Mock"
        ES_Age            → group1="Age",   group2=None
        """
        remainder = column_name
        for prefix, _ in _STAT_TYPE_PREFIXES:
            if column_name.startswith(prefix):
                remainder = column_name[len(prefix):].lstrip('_')
                break
        if not remainder:
            return None, None
        remainder = re.sub(r'_+', '_', remainder)
        parts = re.split(r'_[Vv][Ss]_', remainder, maxsplit=1)
        if len(parts) == 2:
            return parts[0], parts[1]
        return remainder, None

    @classmethod
    def from_column_name(cls, column_name: str) -> "ComparisonColumn":
        """Build a ComparisonColumn from just the column name (no map metadata)."""
        stat_type = cls.parse_stat_type(column_name)
        group1, group2 = cls.parse_groups(column_name)
        if group1 and group2:
            comparison = f"{group1}_VS_{group2}"
        else:
            comparison = group1
        return cls(column_name=column_name, stat_type=stat_type, group1=group1, group2=group2, comparison=comparison)


@dataclass
@facets(category_fields=['data_type'])
class StatsResult(Node):
    data_type: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    pre_processing_description: Optional[str] = None
    peri_processing_description: Optional[str] = None
    effect_size_description: Optional[str] = None
    effect_size_pval_description: Optional[str] = None
    effect_size_adj_pval_description: Optional[str] = None
    effect_size_2_description: Optional[str] = None
    data_analysis_code_link: Optional[str] = None
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    analyte_id_column: Optional[str] = None
    file_reference: Optional[str] = None
    comparison_columns: List[ComparisonColumn] = field(default_factory=list)
    _data_frame: Optional["pd.DataFrame"] = field(default=None, repr=False)


@dataclass
class ExperimentStatsResultEdge(Relationship):
    start_node: "Experiment" = None
    end_node: "StatsResult" = None


@dataclass
class StatsResultPersonEdge(Relationship):
    start_node: "StatsResult" = None
    end_node: "Person" = None
    role: str = None


@dataclass
class StatsResultGeneEdge(Relationship):
    start_node: "StatsResult" = None
    end_node: "Gene" = None


@dataclass
class StatsResultMetaboliteEdge(Relationship):
    start_node: "StatsResult" = None
    end_node: "Metabolite" = None