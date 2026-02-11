from dataclasses import dataclass, field
from datetime import date
from typing import Optional, List, TYPE_CHECKING

from src.core.decorators import facets
from src.models.node import Node, Relationship

if TYPE_CHECKING:
    from src.models.pounce.project import Project, Person
    from src.models.pounce.biosample import Biosample


@dataclass
@facets(category_fields=['experiment_type', 'platform_type'])
class Experiment(Node):
    name: Optional[str] = None
    description: Optional[str] = None
    design: Optional[str] = None
    experiment_type: Optional[str] = None
    date: Optional[date] = None
    platform_type: Optional[str] = None
    platform_name: Optional[str] = None
    platform_provider: Optional[str] = None
    platform_output_type: Optional[str] = None
    public_repo_id: Optional[str] = None
    repo_url: Optional[str] = None
    raw_file_archive_dir: List[str] = field(default_factory=list)
    extraction_protocol: Optional[str] = None
    acquisition_method: Optional[str] = None
    metabolite_identification_description: Optional[str] = None
    experiment_data_file: Optional[str] = None
    attached_files: List[str] = field(default_factory=list)


@dataclass
class ProjectExperimentEdge(Relationship):
    start_node: "Project" = None
    end_node: "Experiment" = None


@dataclass
class ExperimentPersonEdge(Relationship):
    start_node: "Experiment" = None
    end_node: "Person" = None
    role: str = None


@dataclass
class RunBiosample(Node):
    biological_replicate_number: Optional[int] = None
    technical_replicate_number: Optional[int] = None
    run_order: Optional[int] = None


@dataclass
class BiosampleRunBiosampleEdge(Relationship):
    start_node: "Biosample" = None
    end_node: "RunBiosample" = None
