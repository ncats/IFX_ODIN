from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, List, Optional


@dataclass
class GeneRifAnnotation:
    text: str
    updated_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, object]:
        data = asdict(self)
        if self.updated_at is not None:
            data["updated_at"] = self.updated_at.isoformat()
        return data

    @classmethod
    def from_dict(cls, data: dict):
        values = {k: v for k, v in data.items() if k in cls.__dataclass_fields__}
        updated_at = values.get("updated_at")
        if isinstance(updated_at, str) and updated_at.strip():
            try:
                values["updated_at"] = datetime.fromisoformat(updated_at)
            except ValueError:
                pass
        return cls(**values)


@dataclass
class PublicationReference:
    pmid: str
    source: str
    gene_id: Optional[int] = None
    gene_rifs: Optional[List[GeneRifAnnotation]] = None

    def to_dict(self) -> Dict[str, object]:
        data = asdict(self)
        if self.gene_rifs is not None:
            data["gene_rifs"] = [gene_rif.to_dict() for gene_rif in self.gene_rifs]
        return data

    @classmethod
    def from_dict(cls, data: dict):
        values = {k: v for k, v in data.items() if k in cls.__dataclass_fields__}
        if isinstance(values.get("gene_rifs"), list):
            values["gene_rifs"] = [GeneRifAnnotation.from_dict(item) for item in values["gene_rifs"]]
        return cls(**values)
