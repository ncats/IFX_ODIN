from dataclasses import dataclass
from typing import Optional

from src.models.node import Node


@dataclass
class DRGCResource(Node):
    uniprot_id: Optional[str] = None
    rssid: Optional[str] = None
    resource_type: Optional[str] = None
    json: Optional[str] = None
    legacy_target_id: Optional[int] = None
