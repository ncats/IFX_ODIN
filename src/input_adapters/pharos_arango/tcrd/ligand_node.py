import time
from abc import ABC
from typing import Generator, List
from src.constants import DataSourceName
from src.input_adapters.pharos_arango.tcrd.protein import PharosArangoAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DataSourceDetails
from src.models.ligand import Ligand, ProteinLigandEdge

def ligand_edge_query(meets_idg_cutoff: bool, last_key: str = None, limit: int = 10000) -> str:
    pagination_filter = f'FILTER rel._key > "{last_key}"' if last_key else ""
    cutoff_clause = f'FILTER rel.meets_idg_cutoff == {meets_idg_cutoff}' if meets_idg_cutoff else ""
    trimmed_sources = [DataSourceName.ChEMBL.value, DataSourceName.IUPHAR.value]
    if not meets_idg_cutoff:
        return f"""
        FOR rel IN `ProteinLigandEdge`
            {pagination_filter}
            {cutoff_clause}
            SORT rel._key
            LIMIT {limit}
            RETURN rel
        """

    return f"""
    FOR rel IN `ProteinLigandEdge`
        {pagination_filter}
        {cutoff_clause}
        LET pro = DOCUMENT(rel._from)
        LET cutoff = (
            pro.idg_family == "Kinase" ? 7.52288 :
            pro.idg_family == "Ion Channel" ? 5 :
            pro.idg_family == "GPCR" ? 7 :
            pro.idg_family == "Nuclear Receptor" ? 7 : 6
        )
        LET filtered_details = (
            FOR detail IN (rel.details || [])
                FILTER detail.activity_source NOT IN {trimmed_sources}
                    OR (detail.act_value != null AND detail.act_value >= cutoff)
                RETURN detail
        )
        FILTER LENGTH(filtered_details) > 0
        SORT rel._key
        LIMIT {limit}
        RETURN MERGE(rel, {{details: filtered_details}})
    """


def ligand_query(meets_idg_cutoff: bool, last_key: str = None, limit: int = 10000) -> str:
    pagination_filter = f'FILTER lig._key > "{last_key}"' if last_key else ""
    cutoff_clause = f'FILTER rel.meets_idg_cutoff == {meets_idg_cutoff}' if meets_idg_cutoff else ""
    return f"""
    FOR lig IN `Ligand`
        {pagination_filter}
        SORT lig._key
        LIMIT {limit}
        LET proteins = (
            FOR rel IN `ProteinLigandEdge`
                FILTER rel._to == lig._id
                {cutoff_clause}
                RETURN rel
        )
        FILTER LENGTH(proteins) > 0
        RETURN lig
    """


def ligand_version_query():
    return f"""FOR lig IN `Ligand`
        limit 1
        RETURN lig.creation
        """

class LigandBaseAdapter(PharosArangoAdapter, InputAdapter, ABC):
    meets_idg_cutoff: bool

    def get_version_info_query(self):
        raw_version_info = self.runQuery(ligand_version_query())[0]
        return DataSourceDetails.parse_tsv(raw_version_info)

    def __init__(self, credentials, database_name: str, meets_idg_cutoff: bool = True):
        PharosArangoAdapter.__init__(self, credentials, database_name)
        InputAdapter.__init__(self)
        self.meets_idg_cutoff = meets_idg_cutoff


class LigandEdgeAdapter(LigandBaseAdapter):
    def get_all(self) -> Generator[List[ProteinLigandEdge], None, None]:
        last_key = None
        batch_size = self.batch_size

        while True:
            query = ligand_edge_query(self.meets_idg_cutoff, last_key=last_key, limit=batch_size)
            start_time = time.time()
            rows = list(self.runQuery(query))

            elapsed = time.time() - start_time
            print(f"Fetched {len(rows)} rows after last_key={last_key} (batch_size={batch_size}) in {elapsed:.2f} sec")

            if not rows:
                break
            relationships = [
                ProteinLigandEdge.from_dict(row)
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
            query = ligand_query(self.meets_idg_cutoff, last_key=last_key, limit=batch_size)
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
