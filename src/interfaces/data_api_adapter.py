import os.path
from abc import ABC, abstractmethod
from typing import List, Union, Optional, Dict, Callable
import importlib.util
import networkx as nx

from src.core.decorators import collect_facets
from src.interfaces.labeler import Labeler
from src.interfaces.metadata import DatabaseMetadata
from src.interfaces.result_types import FacetQueryResult, ListQueryResult, DetailsQueryResult, \
    ResolveResult, LinkedListQueryResult, LinkedListQueryContext, LinkDetails, ListQueryContext, \
    NetworkedListQueryContext, UpsetQueryContext, UpsetResult
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
        setattr(instance, 'creation', data.get('creation'))
        setattr(instance, 'updates', data.get('updates'))
        return instance

    def get_class(self, class_name: str):
        if not self.class_map:
            self.initialize_class_map()
        return self.class_map[class_name]

    def get_default_facets(self, class_name: str) -> List[str]:
        cls = self.get_class(class_name)
        categories, numerics = collect_facets(cls)
        return [cat for cat in categories if cat not in ['id', 'xref']]

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
    def get_metadata(self) -> DatabaseMetadata:
        raise NotImplementedError("Derived classes must implement get_metadata")

    @abstractmethod
    def get_etl_metadata(self) -> any:
        raise NotImplementedError("Derived classes must implement get_etl_metadata")


    @abstractmethod
    def get_graph_representation(self, unlabel: bool = False) -> nx.Graph:
        """Get the graph representation of the data."""
        raise NotImplementedError("Derived classes must implement get_graph_representation")

    @abstractmethod
    def list_nodes(self):
        """List all data models in the database."""
        raise NotImplementedError("Derived classes must implement list_nodes")

    @abstractmethod
    def list_edges(self):
        """List all edges in the database."""
        raise NotImplementedError("Derived classes must implement list_edges")


    # FETCH TOP LEVEL LISTS
    def get_list_obj(self, context: ListQueryContext = None) -> ListQueryResult:
        result = ListQueryResult()
        result._query_context = context
        result._query_service = self
        return result

    @abstractmethod
    def get_query(self, context: ListQueryContext, top: int, skip: int) -> str:
        raise NotImplementedError("Derived classes must implement get_query")

    @abstractmethod
    def get_list(self, context: ListQueryContext, top: int, skip: int) -> List[Node]:
        """Get the data model."""
        raise NotImplementedError("Derived classes must implement get_list")

    @abstractmethod
    def get_count(self, context: ListQueryContext = None) -> int:
        """Get the data model."""
        raise NotImplementedError("Derived classes must implement get_count")

    @abstractmethod
    def get_facets(self, context: ListQueryContext, facets: List[str], top: int) -> List[FacetQueryResult]:
        raise NotImplementedError("Derived classes must implement get_facets")

    @abstractmethod
    def get_upset(self, context: ListQueryContext, facet_context: UpsetQueryContext) -> List[UpsetResult]:
        raise NotImplementedError("Derived classes must implement get_upset")

    # FETCH LISTS LINKED TO A NODE
    def get_linked_list(self, context: LinkedListQueryContext) -> LinkedListQueryResult:
        result = LinkedListQueryResult()
        result._query_context = context
        result._query_service = self
        return result

    @abstractmethod
    def get_linked_list_query(self, context: LinkedListQueryContext, top: int, skip: int) -> str:
        raise NotImplementedError("Derived classes must implement get_linked_list_query")

    @abstractmethod
    def get_linked_list_count(self, context: LinkedListQueryContext) -> int:
        raise NotImplementedError("Derived classes must implement get_linked_list_count")

    @abstractmethod
    def get_linked_list_details(self, context: LinkedListQueryContext, top: int, skip: int) -> List[LinkDetails]:
        raise NotImplementedError("Derived classes must implement get_linked_list_details")

    @abstractmethod
    def get_linked_list_facets(self, context: LinkedListQueryContext, node_facets: Optional[List[str]], edge_facets: Optional[List[str]]) -> List[FacetQueryResult]:
        raise NotImplementedError("Derived classes must implement get_linked_list_facets")


    def get_networked_list(self, context: NetworkedListQueryContext) -> LinkedListQueryResult:
        result = LinkedListQueryResult()
        result._query_context = context
        result._query_service = self
        return result

    @abstractmethod
    def get_networked_list_query(self, context: NetworkedListQueryContext, top: int, skip: int) -> str:
        raise NotImplementedError("Derived classes must implement get_networked_list_query")

    @abstractmethod
    def get_networked_list_count(self, context: NetworkedListQueryContext) -> int:
        raise NotImplementedError("Derived classes must implement get_networked_list_count")

    @abstractmethod
    def get_networked_list_details(self, context: NetworkedListQueryContext, top: int, skip: int) -> List[LinkDetails]:
        raise NotImplementedError("Derived classes must implement get_networked_list_details")

    @abstractmethod
    def get_networked_list_facets(self, context: NetworkedListQueryContext, node_facets: Optional[List[str]]) -> List[FacetQueryResult]:
        raise NotImplementedError("Derived classes must implement get_networked_list_facets")

    # FETCH DETAILS
    @abstractmethod
    def resolve_id(self, data_model: str, id: str, sortby: dict = {}) -> ResolveResult:
        """Get the data model."""
        raise NotImplementedError("Derived classes must implement resolve_id")

    @abstractmethod
    def get_details(self, data_model: str, id: str) -> DetailsQueryResult:
        """Get the data model."""
        raise NotImplementedError("Derived classes must implement get_details")

    @abstractmethod
    def get_edge_types(self, data_model: str):
        """Get the edge types for a given data model."""
        raise NotImplementedError("Derived classes must implement get_edge_types")

    @abstractmethod
    def get_edge_list(self, data_model: str,  edge_data_model: str, start_id: str = None, end_id: str = None, top: int = 10, skip: int = 0):
        """Get the edge list for a given edge data model."""
        raise NotImplementedError("Derived classes must implement get_edge_list")

    def get_rest_endpoints(self) -> Dict[str, Callable]:
        """
        Return a dict mapping from endpoint paths to callables that implement the logic.
        Each callable should take (request: Request) as an argument (or use FastAPI params).
        """
        return {}