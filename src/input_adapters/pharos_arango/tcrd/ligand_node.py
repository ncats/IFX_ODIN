import time
from abc import ABC
from typing import Generator, List
from src.input_adapters.pharos_arango.tcrd.protein import PharosArangoAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DataSourceDetails
from src.models.ligand import Ligand, ProteinLigandRelationship

def ligand_edge_query(reviewed_only: bool, meets_idg_cutoff: bool, last_key: str = None, limit: int = 10000) -> str:
    pagination_filter = f'FILTER rel._key > "{last_key}"' if last_key else ""
    pro_clause = f'FILTER pro.uniprot_reviewed == {reviewed_only}' if reviewed_only else ""
    cutoff_clause = f'FILTER rel.meets_idg_cutoff == {meets_idg_cutoff}' if meets_idg_cutoff else ""
    if not reviewed_only and not meets_idg_cutoff:
        return f"""
            FOR rel IN `biolink:interacts_with`
                {pagination_filter}
            SORT rel._key
            LIMIT {limit}
            RETURN rel
        """
    return f"""
    FOR rel IN `biolink:interacts_with`
        {pagination_filter}
        {cutoff_clause}
        SORT rel._key
        LIMIT {limit}   
        LET pro = DOCUMENT(rel._from)
        {pro_clause}
        return rel
    """


def ligand_query(reviewed_only: bool, meets_idg_cutoff: bool, last_key: str = None, limit: int = 10000) -> str:
    pagination_filter = f'FILTER lig._key > "{last_key}"' if last_key else ""
    pro_clause = f'FILTER pro.uniprot_reviewed == {reviewed_only}' if reviewed_only else ""
    cutoff_clause = f'FILTER rel.meets_idg_cutoff == {meets_idg_cutoff}' if meets_idg_cutoff else ""

    if not reviewed_only and not meets_idg_cutoff:
        return f"""
            FOR lig IN `biolink:ChemicalEntity`
                {pagination_filter}
            SORT lig._key
            LIMIT {limit}
            RETURN lig
        """

    return f"""
    FOR lig IN `biolink:ChemicalEntity`
        {pagination_filter}
        SORT lig._key
        LIMIT {limit}
        
        LET proteins = (
            FOR rel IN `biolink:interacts_with`
                FILTER rel._to == lig._id
                {cutoff_clause}
                LET pro = DOCUMENT(rel._from)
                {pro_clause}
                RETURN pro
        )
        FILTER LENGTH(proteins) > 0
        RETURN lig
        """


def ligand_version_query():
    return f"""FOR pro IN `biolink:ChemicalEntity`
        limit 1
        RETURN pro.creation
        """

class LigandBaseAdapter(PharosArangoAdapter, InputAdapter, ABC):
    meets_idg_cutoff: bool

    def get_version_info_query(self):
        raw_version_info = self.runQuery(ligand_version_query())[0]
        return DataSourceDetails.parse_tsv(raw_version_info)

    def __init__(self, credentials, database_name: str, meets_idg_cutoff: bool = True, reviewed_only: bool = True):
        PharosArangoAdapter.__init__(self, credentials, database_name, reviewed_only)
        InputAdapter.__init__(self)
        self.meets_idg_cutoff = meets_idg_cutoff


class LigandEdgeAdapter(LigandBaseAdapter):
    def get_all(self) -> Generator[List[ProteinLigandRelationship], None, None]:
        last_key = None
        batch_size = self.batch_size

        while True:
            query = ligand_edge_query(self.reviewed_only, self.meets_idg_cutoff, last_key=last_key, limit=batch_size)
            start_time = time.time()
            rows = list(self.runQuery(query))

            elapsed = time.time() - start_time
            print(f"Fetched {len(rows)} rows after last_key={last_key} (batch_size={batch_size}) in {elapsed:.2f} sec")

            if not rows:
                break
            relationships = [
                ProteinLigandRelationship.from_dict(row)
                for row in rows
            ]

            yield relationships

            # Advance bookmark
            last_key = rows[-1]["_key"]



class LigandNodeAdapter(LigandBaseAdapter):
    def get_all(self) -> Generator[List[Ligand], None, None]:
        last_key = None
        batch_size = self.batch_size

        while True:
            query = ligand_query(self.reviewed_only, self.meets_idg_cutoff, last_key=last_key, limit=batch_size)
            start_time = time.time()
            rows = list(self.runQuery(query))

            elapsed = time.time() - start_time
            print(f"Fetched {len(rows)} rows after last_key={last_key} (batch_size={batch_size}) in {elapsed:.2f} sec")

            if not rows:
                break  # no more results

            ligands = [
                Ligand.from_dict(row)
                for row in rows
            ]

            yield ligands

            # Advance bookmark
            last_key = rows[-1]["_key"]
