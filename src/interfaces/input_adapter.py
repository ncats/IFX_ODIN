from abc import ABC, abstractmethod
from src.interfaces.id_normalizer import IdNormalizer


class InputAdapter(ABC):
    name: str
    id_normalizer: IdNormalizer


    @abstractmethod
    def next(self):
        pass
