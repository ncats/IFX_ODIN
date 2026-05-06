from dataclasses import dataclass
from typing import List, Optional

from src.core.decorators import facets, search
from src.models.gene import Gene
from src.models.node import Node, Relationship
from src.models.protein import Protein


@dataclass
@facets(category_fields=["species"])
@search(text_fields=["symbol", "name", "source_primary_id"])
class OrthologGene(Node):
    species: Optional[str] = None
    symbol: Optional[str] = None
    name: Optional[str] = None
    source_primary_id: Optional[str] = None
    source_db_id: Optional[str] = None
    entrez_gene_id: Optional[str] = None
    ensembl_gene_id: Optional[str] = None


@dataclass
class GeneOrthologGeneEdge(Relationship):
    start_node: Gene = None
    end_node: OrthologGene = None
    species: Optional[str] = None
    support_sources: Optional[List[str]] = None
    source_db_ids: Optional[List[str]] = None
    ortholog_symbols: Optional[List[str]] = None
    ortholog_names: Optional[List[str]] = None


@dataclass
class ProteinOrthologGeneEdge(Relationship):
    start_node: Protein = None
    end_node: OrthologGene = None
    species: Optional[str] = None
    support_sources: Optional[List[str]] = None
    source_db_ids: Optional[List[str]] = None
    ortholog_symbols: Optional[List[str]] = None
    ortholog_names: Optional[List[str]] = None
