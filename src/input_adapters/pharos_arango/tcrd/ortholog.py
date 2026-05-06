from typing import Generator, List

from src.input_adapters.pharos_arango.tcrd.protein import PharosArangoAdapter
from src.models.datasource_version_info import DataSourceDetails
from src.models.mouse_phenotype import (
    ProteinMousePhenotypeEdge,
    MousePhenotype,
    MousePhenotypeDetail,
    OrthologGeneMousePhenotypeEdge,
)
from src.models.ortholog import OrthologGene, ProteinOrthologGeneEdge
from src.models.protein import Protein


def ortholog_gene_query(last_key: str = None, limit: int = 5000) -> str:
    filter_clause = f'FILTER d._key > "{last_key}"' if last_key else ""
    return f"""
    FOR d IN `OrthologGene`
        {filter_clause}
        SORT d._key
        LIMIT {limit}
        RETURN d
    """


def ortholog_phenotype_edge_query(last_key: str = None, limit: int = 5000) -> str:
    filter_clause = f'FILTER rel._key > "{last_key}"' if last_key else ""
    return f"""
    FOR rel IN `OrthologGeneMousePhenotypeEdge`
        {filter_clause}
        SORT rel._key
        LIMIT {limit}
        LET phenotype = FIRST(
            FOR mp IN `MousePhenotype`
                FILTER mp.id == rel.end_id
                LIMIT 1
                RETURN mp
        )
        RETURN MERGE(rel, {{
            end_node: phenotype
        }})
    """


def ortholog_protein_links_query() -> str:
    return """
    FOR po IN `ProteinOrthologGeneEdge`
        FILTER po.end_id IN @ortholog_ids
        COLLECT ortholog_id = po.end_id, protein_node_id = po.start_id INTO grouped
        RETURN {
            ortholog_id: ortholog_id,
            protein_node_id: protein_node_id,
            support_sources: FIRST(grouped[*].po.support_sources) || []
        }
    """


def ortholog_version_query() -> str:
    return """FOR d IN `OrthologGene` LIMIT 1 RETURN d.creation"""


def ortholog_phenotype_version_query() -> str:
    return """FOR rel IN `OrthologGeneMousePhenotypeEdge` LIMIT 1 RETURN rel.creation"""


def protein_phenotype_edge_query(last_key: str = None, limit: int = 5000) -> str:
    filter_clause = f'FILTER rel._key > "{last_key}"' if last_key else ""
    return f"""
    FOR rel IN `ProteinMousePhenotypeEdge`
        {filter_clause}
        SORT rel._key
        LIMIT {limit}
        LET phenotype = FIRST(
            FOR mp IN `MousePhenotype`
                FILTER mp.id == rel.end_id
                LIMIT 1
                RETURN mp
        )
        RETURN MERGE(rel, {{
            end_node: phenotype
        }})
    """


def protein_phenotype_version_query() -> str:
    return """FOR rel IN `ProteinMousePhenotypeEdge` LIMIT 1 RETURN rel.creation"""


def protein_ortholog_edge_query(last_key: str = None, limit: int = 5000) -> str:
    filter_clause = f'FILTER rel._key > "{last_key}"' if last_key else ""
    return f"""
    FOR rel IN `ProteinOrthologGeneEdge`
        {filter_clause}
        SORT rel._key
        LIMIT {limit}
        LET ortholog = FIRST(
            FOR og IN `OrthologGene`
                FILTER og.id == rel.end_id
                LIMIT 1
                RETURN og
        )
        RETURN MERGE(rel, {{
            end_node: ortholog
        }})
    """


class OrthologGeneAdapter(PharosArangoAdapter):
    batch_size = 5_000

    def get_all(self) -> Generator[List[OrthologGene], None, None]:
        last_key = None
        while True:
            rows = list(self.runQuery(ortholog_gene_query(last_key=last_key, limit=self.batch_size)))
            if not rows:
                break
            yield [OrthologGene.from_dict(row) for row in rows]
            last_key = rows[-1]["_key"]

    def get_version_info_query(self) -> DataSourceDetails:
        raw_version_info = self.runQuery(ortholog_version_query())[0]
        return DataSourceDetails.parse_tsv(raw_version_info)


class OrthologGeneMousePhenotypeAdapter(PharosArangoAdapter):
    batch_size = 5_000

    def _load_protein_links(self, ortholog_ids: List[str]) -> dict[str, list[dict]]:
        if not ortholog_ids:
            return {}
        rows = self.runQuery(
            ortholog_protein_links_query(),
            bind_vars={"ortholog_ids": ortholog_ids},
        )
        link_map: dict[str, list[dict]] = {}
        for row in rows:
            link_map.setdefault(row["ortholog_id"], []).append({
                "protein_node_id": row["protein_node_id"],
                "support_sources": row.get("support_sources") or [],
            })
        return link_map

    def get_all(self) -> Generator[List[OrthologGeneMousePhenotypeEdge], None, None]:
        last_key = None
        while True:
            rows = list(self.runQuery(ortholog_phenotype_edge_query(last_key=last_key, limit=self.batch_size)))
            if not rows:
                break
            protein_link_map = self._load_protein_links(list({row["start_id"] for row in rows}))

            batch = []
            for row in rows:
                edge = OrthologGeneMousePhenotypeEdge(
                    start_node=OrthologGene(id=row["start_id"]),
                    end_node=MousePhenotype(
                        id=row["end_id"],
                        name=(row.get("end_node") or {}).get("name"),
                    ),
                    details=[MousePhenotypeDetail.from_dict(d) for d in (row.get("details") or [])],
                )
                edge.protein_links = protein_link_map.get(row["start_id"], [])
                batch.append(edge)
            yield batch
            last_key = rows[-1]["_key"]

    def get_version_info_query(self) -> DataSourceDetails:
        raw_version_info = self.runQuery(ortholog_phenotype_version_query())[0]
        return DataSourceDetails.parse_tsv(raw_version_info)


class ProteinMousePhenotypeAdapter(PharosArangoAdapter):
    batch_size = 5_000

    def get_all(self) -> Generator[List[ProteinMousePhenotypeEdge], None, None]:
        last_key = None
        while True:
            rows = list(self.runQuery(protein_phenotype_edge_query(last_key=last_key, limit=self.batch_size)))
            if not rows:
                break

            batch = []
            for row in rows:
                batch.append(
                    ProteinMousePhenotypeEdge(
                        start_node=Protein(id=row["start_id"]),
                        end_node=MousePhenotype(
                            id=row["end_id"],
                            name=(row.get("end_node") or {}).get("name"),
                        ),
                        details=[MousePhenotypeDetail.from_dict(d) for d in (row.get("details") or [])],
                    )
                )
            yield batch
            last_key = rows[-1]["_key"]

    def get_version_info_query(self) -> DataSourceDetails:
        raw_version_info = self.runQuery(protein_phenotype_version_query())[0]
        return DataSourceDetails.parse_tsv(raw_version_info)


class ProteinOrthologGeneEdgeAdapter(PharosArangoAdapter):
    batch_size = 5_000

    def get_all(self) -> Generator[List[ProteinOrthologGeneEdge], None, None]:
        last_key = None
        while True:
            rows = list(self.runQuery(protein_ortholog_edge_query(last_key=last_key, limit=self.batch_size)))
            if not rows:
                break

            yield [
                ProteinOrthologGeneEdge(
                    start_node=Protein(id=row["start_id"]),
                    end_node=OrthologGene(
                        id=row["end_id"],
                        species=(row.get("end_node") or {}).get("species"),
                        symbol=(row.get("end_node") or {}).get("symbol"),
                        name=(row.get("end_node") or {}).get("name"),
                        source_primary_id=(row.get("end_node") or {}).get("source_primary_id"),
                        source_db_id=(row.get("end_node") or {}).get("source_db_id"),
                        entrez_gene_id=(row.get("end_node") or {}).get("entrez_gene_id"),
                        ensembl_gene_id=(row.get("end_node") or {}).get("ensembl_gene_id"),
                    ),
                    species=row.get("species"),
                    support_sources=row.get("support_sources") or [],
                    source_db_ids=row.get("source_db_ids") or [],
                    ortholog_symbols=row.get("ortholog_symbols") or [],
                    ortholog_names=row.get("ortholog_names") or [],
                )
                for row in rows
            ]
            last_key = rows[-1]["_key"]

    def get_version_info_query(self) -> DataSourceDetails:
        raw_version_info = self.runQuery(ortholog_version_query())[0]
        return DataSourceDetails.parse_tsv(raw_version_info)
