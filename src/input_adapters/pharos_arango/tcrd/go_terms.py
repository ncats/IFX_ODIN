from typing import Generator, List, Union

from src.input_adapters.pharos_arango.tcrd.protein import PharosArangoAdapter
from src.models.datasource_version_info import DataSourceDetails
from src.models.go_term import GoTerm, GoTermHasParent, ProteinGoTermRelationship, GoEvidence
from src.models.node import Node, Relationship
from src.models.protein import Protein

def go_term_query() -> str:
    return f"""FOR go IN `GoTerm`
        RETURN go
    """

def go_assoc_query(reviewed_only: bool) -> str:
    if not reviewed_only:
        return f"""
        FOR rel IN `ProteinGoTermRelationship`
            RETURN rel
        """
    return f"""
    FOR rel IN `ProteinGoTermRelationship`
        LET pro = DOCUMENT(rel._from)
        FILTER pro.uniprot_reviewed == {reviewed_only}
        RETURN rel
    """

def go_parent_query() -> str:
    return f"""FOR rel in `GoTermHasParent`
    RETURN {{
        "start": rel.start_id,
        "end": rel.end_id
    }}"""

def go_term_version_query():
    return f"""FOR go IN `GoTerm`
    limit 1
    RETURN go.creation
    """

class GoTermAdapter(PharosArangoAdapter):

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        go_terms = self.runQuery(go_term_query())
        go_objects = [GoTerm.from_dict(g) for g in go_terms]
        go_map = {g.id: g for g in go_objects}

        yield go_objects

        go_rels = self.runQuery(go_parent_query())

        yield [
            GoTermHasParent(
                start_node=go_map[rel['start']],
                end_node=go_map[rel['end']]
            ) for rel in go_rels
        ]

        go_associations = self.runQuery(go_assoc_query(self.reviewed_only))
        pro_go_rels = [
            ProteinGoTermRelationship(
                start_node=Protein(id = assoc['start_id']),
                end_node=go_map[assoc['end_id']],
                evidence=[GoEvidence.from_dict(e) for e in assoc.get('evidence', [])]
            ) for assoc in go_associations
        ]
        yield pro_go_rels

    def get_version_info_query(self) -> DataSourceDetails:
        raw_version_info = self.runQuery(go_term_version_query())[0]
        return DataSourceDetails.parse_tsv(raw_version_info)

