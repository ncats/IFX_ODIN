from dataclasses import asdict, dataclass
from typing import Dict, Optional


@dataclass
class YearScore:
    year: Optional[int] = None
    score: Optional[float] = None

    def to_dict(self) -> Dict[str, float]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})
