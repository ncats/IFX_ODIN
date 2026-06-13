from typing import Generator, List, Optional, Union

from src.constants import Prefix
from src.input_adapters.pharos_mysql.base import Pharos319Adapter
from src.models.node import EquivalentId, Node, Relationship
from src.models.protein import Protein
from src.models.virus import Virus, ViralPPIDetail, ViralPPIEdge, ViralProtein, VirusViralProteinEdge
from src.shared.sqlalchemy_tables.pharos_tables_old import (
    Protein as mysql_Protein,
    ViralPPI as mysql_ViralPPI,
    ViralProtein as mysql_ViralProtein,
    Virus as mysql_Virus,
)


def _virus_node_id(virus_taxid: str) -> str:
    return EquivalentId(id=str(virus_taxid), type=Prefix.NCBITaxon).id_str()


def _viral_protein_node_id(viral_protein_id: int | str) -> str:
    return f"PHIPSTER.ViralProtein:{viral_protein_id}"


def _parse_pdb_ids(raw_value: Optional[str]) -> List[str]:
    if raw_value is None:
        return []
    value = raw_value.strip()
    if not value:
        return []
    return [token.strip() for token in value.split("|") if token.strip()]


class PHIPSTERLegacyLiftAdapter(Pharos319Adapter):
    batch_size: int = 10000

    def __init__(self, credentials, max_rows: Optional[int] = None, data_source=None):
        super().__init__(credentials, data_source=data_source)
        self.max_rows = max_rows

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        viruses = self._load_viruses()
        if viruses:
            yield viruses

        viral_proteins = self._load_viral_proteins()
        if viral_proteins:
            yield viral_proteins

        virus_viral_protein_edges = self._load_virus_viral_protein_edges()
        if virus_viral_protein_edges:
            yield virus_viral_protein_edges

        yield from self._yield_viral_ppi_edges()

    def _load_viruses(self) -> List[Virus]:
        rows = (
            self.get_session().query(
                mysql_Virus.virusTaxid,
                mysql_Virus.nucleic1,
                mysql_Virus.nucleic2,
                mysql_Virus.order,
                mysql_Virus.family,
                mysql_Virus.subfamily,
                mysql_Virus.genus,
                mysql_Virus.species,
                mysql_Virus.name,
            )
            .order_by(mysql_Virus.virusTaxid)
        )

        return [
            Virus(
                id=_virus_node_id(row[0]),
                source_id=str(row[0]),
                nucleic1=row[1],
                nucleic2=row[2],
                order=row[3],
                family=row[4],
                subfamily=row[5],
                genus=row[6],
                species=row[7],
                name=row[8],
            )
            for row in rows
        ]

    def _load_viral_proteins(self) -> List[ViralProtein]:
        rows = (
            self.get_session().query(
                mysql_ViralProtein.id,
                mysql_ViralProtein.name,
                mysql_ViralProtein.ncbi,
                mysql_ViralProtein.virus_id,
            )
            .order_by(mysql_ViralProtein.id)
        )

        return [
            ViralProtein(
                id=_viral_protein_node_id(row[0]),
                name=row[1],
                ncbi=row[2],
            )
            for row in rows
        ]

    def _load_virus_viral_protein_edges(self) -> List[VirusViralProteinEdge]:
        rows = (
            self.get_session().query(
                mysql_ViralProtein.virus_id,
                mysql_ViralProtein.id,
            )
            .filter(mysql_ViralProtein.virus_id.is_not(None))
            .order_by(mysql_ViralProtein.id)
        )

        return [
            VirusViralProteinEdge(
                start_node=ViralProtein(id=_viral_protein_node_id(row[1])),
                end_node=Virus(id=_virus_node_id(row[0])),
            )
            for row in rows
        ]

    def _yield_viral_ppi_edges(self) -> Generator[List[ViralPPIEdge], None, None]:
        rows = (
            self.get_session().query(
                mysql_ViralPPI.viral_protein_id,
                mysql_Protein.uniprot,
                mysql_ViralPPI.dataSource,
                mysql_ViralPPI.finalLR,
                mysql_ViralPPI.pdbIDs,
                mysql_ViralPPI.highConfidence,
            )
            .join(mysql_Protein, mysql_Protein.id == mysql_ViralPPI.protein_id)
            .order_by(mysql_ViralPPI.id)
        )
        if self.max_rows is not None:
            rows = rows.limit(self.max_rows)

        batch: List[ViralPPIEdge] = []
        for row in rows:
            batch.append(
                ViralPPIEdge(
                    start_node=Protein(id=EquivalentId(id=row[1], type=Prefix.UniProtKB).id_str()),
                    end_node=ViralProtein(id=_viral_protein_node_id(row[0])),
                    details=[
                        ViralPPIDetail(
                            source=row[2],
                            source_protein_id=EquivalentId(id=row[1], type=Prefix.UniProtKB).id_str(),
                            final_lr=float(row[3]) if row[3] is not None else None,
                            pdb_ids=_parse_pdb_ids(row[4]),
                            high_confidence=bool(row[5]) if row[5] is not None else None,
                        )
                    ],
                )
            )
            if len(batch) >= self.batch_size:
                yield batch
                batch = []

        if batch:
            yield batch
