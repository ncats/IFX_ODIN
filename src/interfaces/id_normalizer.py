from abc import ABC, abstractmethod
from typing import Union, List, Dict
from dataclasses import dataclass, asdict


@dataclass
class NormalizationMatch:
    match: str
    context: List[str] = None

    def to_dict(self) -> Dict[str, str]:
        return asdict(self)


@dataclass
class IdNormalizerResult:
    best_matches: List[NormalizationMatch] = None
    other_matches: List[NormalizationMatch] = None

    def to_dict(self) -> Dict[str, str]:
        return asdict(self)


class IdNormalizer(ABC):
    name: str

    @abstractmethod
    def normalize(self, input_ids: Union[str, List[str]]) -> Dict[str, IdNormalizerResult]:
        pass
