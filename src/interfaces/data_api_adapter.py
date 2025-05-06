import os.path
from abc import ABC, abstractmethod
from typing import List, Union
import importlib.util
import networkx as nx

from src.interfaces.labeler import Labeler
from src.interfaces.result_types import FacetQueryResult, CountQueryResult, ListQueryResult, DetailsQueryResult
from src.models.node import Node, Relationship


class APIAdapter(ABC):
    labeler: Labeler
    label: str
    modules: List
    class_map = {}

    def __init__(self, label: str, imports: List[str]):
        self.label = label
        self.modules = []
        for imp in imports:
            module_name = os.path.splitext(os.path.basename(imp))[0]
            spec = importlib.util.spec_from_file_location(module_name, imp)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            self.modules.append(module)

    def convert_to_class(self, class_name: str, data: dict) -> Union[Node, Relationship]:
        cls = self.get_class(class_name)
        if cls is None:
            raise Exception(f"Class {class_name} not found in class map")
        instance = cls.from_dict(data)
        return instance

    def get_class_map(self):
        if not self.class_map:
            self.initialize_class_map()
        return self.class_map

    def get_class(self, class_name: str):
        if not self.class_map:
            self.initialize_class_map()
        return self.class_map[class_name]

    def initialize_class_map(self):
        nodes = self.list_nodes()
        edges = self.list_edges()
        self.class_map = {}
        for class_name in [*nodes, *edges]:
            found = False
            for module in self.modules:
                cls = getattr(module, class_name, None)
                if cls:
                    self.class_map[class_name] = cls
                    found = True
                    break
            if not found:
                print(f"Class {class_name} not found.  are you missing an import?")


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