"""Container for all parsed metadata from a POUNCE submission.

Data matrices (RawData, PeakData, StatsReadyData) are intentionally excluded —
validators only need the metadata sheets, which keeps validation fast regardless
of data volume.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from src.input_adapters.pounce_sheets.parsed_classes import (
    ParsedProject,
    ParsedPerson,
    ParsedBiosample,
    ParsedBiospecimen,
    ParsedDemographics,
    ParsedExposure,
    ParsedExperiment,
    ParsedRunBiosample,
    ParsedStatsResultsMeta,
)


@dataclass
class ParsedPounceData:
    project: Optional[ParsedProject] = None
    people: List[ParsedPerson] = field(default_factory=list)
    param_maps: Dict[str, Dict[str, str]] = field(default_factory=dict)
    biosamples: List[ParsedBiosample] = field(default_factory=list)
    biospecimens: List[ParsedBiospecimen] = field(default_factory=list)
    demographics: List[ParsedDemographics] = field(default_factory=list)
    exposures: List[ParsedExposure] = field(default_factory=list)
    experiments: List[ParsedExperiment] = field(default_factory=list)
    run_biosamples: List[ParsedRunBiosample] = field(default_factory=list)
    stats_results: List[ParsedStatsResultsMeta] = field(default_factory=list)
