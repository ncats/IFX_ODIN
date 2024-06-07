from abc import ABC, abstractmethod


class OutputAdapter(ABC):
    name: str

    @abstractmethod
    def store(self, objects) -> bool:
        pass

    @abstractmethod
    def create_or_truncate_datastore(self) -> bool:
        pass
