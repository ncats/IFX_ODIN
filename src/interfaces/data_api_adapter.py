from abc import ABC, abstractmethod

import networkx as nx

from src.interfaces.labeler import Labeler
from src.interfaces.result_types import FacetQueryResult, CountQueryResult, ListQueryResult, DetailsQueryResult


class APIAdapter(ABC):
    labeler: Labeler
    label: str

    def __init__(self, label: str):
        self.label = label

    @abstractmethod
    def get_graph_representation(self, unlabel: bool = False) -> nx.Graph:
        """Get the graph representation of the data."""
        raise NotImplementedError("Derived classes must implement get_graph")


    @abstractmethod
    def list_nodes(self):
        """List all data models in the database."""
        raise NotImplementedError("Derived classes must implement list_data_models")

    @abstractmethod
    def list_edges(self):
        """List all edges in the database."""
        raise NotImplementedError("Derived classes must implement list_edges")

    @abstractmethod
    def get_facet_values(self, data_model: str, field: str, filter: dict = None, top: int = 20) -> FacetQueryResult:
        """Get the top N facet values for a given data model and field."""
        raise NotImplementedError("Derived classes must implement get_facet_values")

    @abstractmethod
    def get_count(self, data_model: str, filter: dict = None) -> CountQueryResult:
        """Get the data model."""
        raise NotImplementedError("Derived classes must implement get_data_model")

    @abstractmethod
    def get_list(self, data_model: str, filter: dict = None, top: int = 20, skip: int = 0) -> ListQueryResult:
        """Get the data model."""
        raise NotImplementedError("Derived classes must implement get_data_model")

    @abstractmethod
    def get_details(self, data_model: str, id: str) -> DetailsQueryResult:
        """Get the data model."""
        raise NotImplementedError("Derived classes must implement get_data_model")

    @abstractmethod
    def get_edge_types(self, data_model: str):
        """Get the edge types for a given data model."""
        raise NotImplementedError("Derived classes must implement get_edge_types")

    @abstractmethod
    def get_edge_list(self, data_model: str,  edge_data_model: str, start_id: str = None, end_id: str = None, top: int = 10, skip: int = 0):
        """Get the edge list for a given edge data model."""
        raise NotImplementedError("Derived classes must implement get_edge_list")