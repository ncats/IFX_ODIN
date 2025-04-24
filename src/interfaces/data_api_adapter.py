from abc import ABC, abstractmethod

from src.interfaces.labeler import Labeler


class APIAdapter(ABC):
    labeler: Labeler
    label: str

    def __init__(self, label: str):
        self.label = label

    @abstractmethod
    def list_nodes(self):
        """List all data models in the database."""
        raise NotImplementedError("Derived classes must implement list_data_models")

    @abstractmethod
    def list_edges(self):
        """List all edges in the database."""
        raise NotImplementedError("Derived classes must implement list_edges")

    @abstractmethod
    def get_facet_values(self, data_model: str, field: str, filter: dict = None, top: int = 20):
        """Get the top N facet values for a given data model and field."""
        raise NotImplementedError("Derived classes must implement get_facet_values")

    @abstractmethod
    def get_count(self, data_model: str, filter: dict = None):
        """Get the data model."""
        raise NotImplementedError("Derived classes must implement get_data_model")

    @abstractmethod
    def get_list(self, data_model: str, filter: dict = None, top: int = 20, skip: int = 0):
        """Get the data model."""
        raise NotImplementedError("Derived classes must implement get_data_model")