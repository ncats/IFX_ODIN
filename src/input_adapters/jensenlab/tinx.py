import csv
import os
import re
import sqlite3
import tempfile
from collections import defaultdict
from datetime import date, datetime
from typing import Dict, Generator, Iterable, List, Optional, Set, Tuple, Union

from src.constants import DataSourceName, Prefix
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.disease import Disease, DiseaseAssociationDetail, TINXImportanceEdge
from src.models.node import EquivalentId, Node, Relationship
from src.models.protein import Protein


class TINXAdapter(InputAdapter):
    progress_every = 1000
    batch_size = 1000

    def __init__(
        self,
        protein_mentions_file_path: str,
        disease_mentions_file_path: str,
        version_file_path: Optional[str] = None,
        max_proteins: Optional[int] = None,
        max_diseases: Optional[int] = None,
        max_pairs: Optional[int] = None,
    ):
        self.protein_mentions_file_path = protein_mentions_file_path
        self.disease_mentions_file_path = disease_mentions_file_path
        self.version_file_path = version_file_path
        self.max_proteins = max_proteins
        self.max_diseases = max_diseases
        self.max_pairs = max_pairs

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.TINX

    def get_version(self) -> DatasourceVersionInfo:
        version = None
        version_date = None
        if self.version_file_path and os.path.exists(self.version_file_path):
            with open(self.version_file_path, "r", encoding="utf-8") as handle:
                reader = csv.DictReader(handle, delimiter="\t")
                row = next(reader, None)
                if row:
                    version = row.get("version") or None
                    raw_version_date = row.get("version_date") or None
                    if raw_version_date:
                        try:
                            version_date = date.fromisoformat(raw_version_date)
                        except ValueError:
                            version_date = None

        return DatasourceVersionInfo(
            version=version,
            version_date=version_date,
            download_date=self._download_date(),
        )

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        print("TIN-X: building protein PMID counts")
        pmid_to_protein_count = self._build_protein_pmid_counts()
        print(
            f"TIN-X: built protein PMID counts for {len(pmid_to_protein_count)} pmids "
            f"across {sum(pmid_to_protein_count.values())} protein-pmid mentions"
        )

        print("TIN-X: building disease PMID counts")
        pmid_to_disease_count = self._build_disease_pmid_counts()
        print(
            f"TIN-X: built disease PMID counts for {len(pmid_to_disease_count)} pmids "
            f"across {sum(pmid_to_disease_count.values())} disease-pmid mentions"
        )

        print("TIN-X: emitting protein novelty")
        yield from self._yield_protein_novelty(pmid_to_protein_count)
        print("TIN-X: emitting disease novelty")
        yield from self._yield_disease_novelty(pmid_to_disease_count)

    def _build_protein_pmid_counts(self) -> Dict[str, int]:
        pmid_to_protein_count: Dict[str, int] = defaultdict(int)
        for loaded_proteins, (protein_id, pmids) in enumerate(self._iter_protein_mentions(), start=1):
            for pmid in pmids:
                pmid_to_protein_count[pmid] += 1
            if loaded_proteins % self.progress_every == 0:
                print(
                    f"TIN-X: indexed {loaded_proteins} proteins into "
                    f"{len(pmid_to_protein_count)} unique pmids"
                )
        return pmid_to_protein_count

    def _build_disease_pmid_counts(self) -> Dict[str, int]:
        pmid_to_disease_count: Dict[str, int] = defaultdict(int)
        for loaded_diseases, (_, pmids) in enumerate(self._iter_disease_mentions(), start=1):
            for pmid in pmids:
                pmid_to_disease_count[pmid] += 1
            if loaded_diseases % self.progress_every == 0:
                print(
                    f"TIN-X: indexed {loaded_diseases} diseases into "
                    f"{len(pmid_to_disease_count)} unique pmids"
                )
        return pmid_to_disease_count

    def _yield_protein_novelty(self, pmid_to_protein_count: Dict[str, int]) -> Generator[List[Protein], None, None]:
        batch: List[Protein] = []
        emitted = 0
        for protein_id, pmids in self._iter_protein_mentions():
            novelty = self._compute_novelty(pmids, pmid_to_protein_count)
            if novelty is None:
                continue
            batch.append(
                Protein(
                    id=EquivalentId(id=protein_id, type=Prefix.ENSEMBL).id_str(),
                    novelty=[novelty],
                )
            )
            emitted += 1
            if len(batch) >= self.batch_size:
                print(f"TIN-X: yielding {len(batch)} protein novelty nodes ({emitted} total)")
                yield batch
                batch = []
        if batch:
            print(f"TIN-X: final protein novelty batch {len(batch)} ({emitted} total)")
            yield batch

    def _yield_disease_novelty(self, pmid_to_disease_count: Dict[str, int]) -> Generator[List[Disease], None, None]:
        batch: List[Disease] = []
        emitted = 0
        for doid, pmids in self._iter_disease_mentions():
            novelty = self._compute_novelty(pmids, pmid_to_disease_count)
            if novelty is None:
                continue
            batch.append(
                Disease(
                    id=doid,
                    novelty=[novelty],
                )
            )
            emitted += 1
            if len(batch) >= self.batch_size:
                print(f"TIN-X: yielding {len(batch)} disease novelty nodes ({emitted} total)")
                yield batch
                batch = []
        if batch:
            print(f"TIN-X: final disease novelty batch {len(batch)} ({emitted} total)")
            yield batch

    def _iter_protein_mentions(self) -> Generator[Tuple[str, Set[str]], None, None]:
        seen_proteins: Set[str] = set()
        with open(self.protein_mentions_file_path, "r", encoding="utf-8", errors="replace") as handle:
            for raw_line in handle:
                row = raw_line.rstrip("\n").split("\t", 1)
                if len(row) < 2:
                    continue
                protein_id = row[0].strip()
                if not protein_id.startswith("ENSP") or protein_id in seen_proteins:
                    continue
                pmids = self._parse_pmid_field(row[1])
                if not pmids:
                    continue
                seen_proteins.add(protein_id)
                yield protein_id, pmids
                if self.max_proteins is not None and len(seen_proteins) >= self.max_proteins:
                    break

    def _iter_disease_mentions(self) -> Generator[Tuple[str, Set[str]], None, None]:
        seen_diseases: Set[str] = set()
        with open(self.disease_mentions_file_path, "r", encoding="utf-8", errors="replace") as handle:
            for raw_line in handle:
                row = raw_line.rstrip("\n").split("\t", 1)
                if len(row) < 2:
                    continue
                doid = self._normalize_doid(row[0].strip())
                if doid is None or doid in seen_diseases:
                    continue
                pmids = self._parse_pmid_field(row[1])
                if not pmids:
                    continue
                seen_diseases.add(doid)
                yield doid, pmids
                if self.max_diseases is not None and len(seen_diseases) >= self.max_diseases:
                    break

    def _download_date(self) -> Optional[date]:
        timestamps = []
        for file_path in (
            self.protein_mentions_file_path,
            self.disease_mentions_file_path,
        ):
            if os.path.exists(file_path):
                timestamps.append(os.path.getmtime(file_path))
        if not timestamps:
            return None
        return datetime.fromtimestamp(max(timestamps)).date()

    @staticmethod
    def _parse_pmid_field(raw_pmids: str) -> Set[str]:
        return {pmid for pmid in raw_pmids.strip().split() if pmid}

    @staticmethod
    def _compute_novelty(entity_pmids: Iterable[str], pmid_counts: Dict[str, int]) -> Optional[float]:
        denominator = 0.0
        for pmid in entity_pmids:
            count = pmid_counts.get(pmid, 0)
            if count > 0:
                denominator += 1.0 / count
        if denominator == 0.0:
            return None
        return 1.0 / denominator

    @staticmethod
    def _normalize_doid(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        if value.startswith("DOID:"):
            return value
        match = re.search(r"/obo/DOID_(\d+)$", value)
        if match:
            return f"DOID:{match.group(1)}"
        return None


class TINXImportanceFileAdapter(TINXAdapter):
    sqlite_batch_size = 100_000

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        temp_db_path = None
        conn = None
        try:
            with tempfile.NamedTemporaryFile(prefix="tinx_importance_", suffix=".sqlite", delete=False) as handle:
                temp_db_path = handle.name

            print(f"TIN-X importance: staging in sqlite at {temp_db_path}")
            conn = sqlite3.connect(temp_db_path)
            self._configure_sqlite(conn)
            self._create_sqlite_tables(conn)
            self._load_protein_mentions_into_sqlite(conn)
            self._load_disease_mentions_into_sqlite(conn)
            self._build_sqlite_count_tables(conn)
            yield from self._yield_importance_edges(conn)
        finally:
            if conn is not None:
                conn.close()
            if temp_db_path and os.path.exists(temp_db_path):
                os.unlink(temp_db_path)

    @staticmethod
    def _configure_sqlite(conn: sqlite3.Connection) -> None:
        conn.execute("PRAGMA journal_mode=OFF")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.execute("PRAGMA cache_size=-200000")

    @staticmethod
    def _create_sqlite_tables(conn: sqlite3.Connection) -> None:
        conn.execute("CREATE TABLE protein_mentions (protein_id TEXT NOT NULL, pmid TEXT NOT NULL)")
        conn.execute("CREATE TABLE disease_mentions (doid TEXT NOT NULL, pmid TEXT NOT NULL)")
        conn.commit()

    def _load_protein_mentions_into_sqlite(self, conn: sqlite3.Connection) -> None:
        print("TIN-X importance: loading protein mentions into sqlite")
        batch: List[Tuple[str, str]] = []
        loaded_proteins = 0
        total_rows = 0
        for protein_id, pmids in self._iter_protein_mentions():
            loaded_proteins += 1
            batch.extend((protein_id, pmid) for pmid in pmids)
            if len(batch) >= self.sqlite_batch_size:
                conn.executemany(
                    "INSERT INTO protein_mentions (protein_id, pmid) VALUES (?, ?)",
                    batch,
                )
                total_rows += len(batch)
                batch = []
                conn.commit()
                print(
                    f"TIN-X importance: loaded {loaded_proteins} proteins and "
                    f"{total_rows} protein-pmid rows"
                )
        if batch:
            conn.executemany(
                "INSERT INTO protein_mentions (protein_id, pmid) VALUES (?, ?)",
                batch,
            )
            total_rows += len(batch)
            conn.commit()
        print(
            f"TIN-X importance: finished protein load with {loaded_proteins} proteins "
            f"and {total_rows} protein-pmid rows"
        )
        conn.execute("CREATE INDEX protein_mentions_pmid_idx ON protein_mentions (pmid)")
        conn.commit()

    def _load_disease_mentions_into_sqlite(self, conn: sqlite3.Connection) -> None:
        print("TIN-X importance: loading disease mentions into sqlite")
        batch: List[Tuple[str, str]] = []
        loaded_diseases = 0
        total_rows = 0
        for doid, pmids in self._iter_disease_mentions():
            loaded_diseases += 1
            batch.extend((doid, pmid) for pmid in pmids)
            if len(batch) >= self.sqlite_batch_size:
                conn.executemany(
                    "INSERT INTO disease_mentions (doid, pmid) VALUES (?, ?)",
                    batch,
                )
                total_rows += len(batch)
                batch = []
                conn.commit()
                print(
                    f"TIN-X importance: loaded {loaded_diseases} diseases and "
                    f"{total_rows} disease-pmid rows"
                )
        if batch:
            conn.executemany(
                "INSERT INTO disease_mentions (doid, pmid) VALUES (?, ?)",
                batch,
            )
            total_rows += len(batch)
            conn.commit()
        print(
            f"TIN-X importance: finished disease load with {loaded_diseases} diseases "
            f"and {total_rows} disease-pmid rows"
        )
        conn.execute("CREATE INDEX disease_mentions_pmid_idx ON disease_mentions (pmid)")
        conn.execute("CREATE INDEX disease_mentions_doid_idx ON disease_mentions (doid)")
        conn.commit()

    @staticmethod
    def _build_sqlite_count_tables(conn: sqlite3.Connection) -> None:
        print("TIN-X importance: building PMID count tables")
        conn.execute("""
            CREATE TABLE pmid_protein_count AS
            SELECT pmid, COUNT(*) AS n
            FROM protein_mentions
            GROUP BY pmid
        """)
        conn.execute("CREATE UNIQUE INDEX pmid_protein_count_idx ON pmid_protein_count (pmid)")
        conn.execute("""
            CREATE TABLE pmid_disease_count AS
            SELECT pmid, COUNT(*) AS n
            FROM disease_mentions
            GROUP BY pmid
        """)
        conn.execute("CREATE UNIQUE INDEX pmid_disease_count_idx ON pmid_disease_count (pmid)")
        conn.commit()

    def _yield_importance_edges(self, conn: sqlite3.Connection) -> Generator[List[TINXImportanceEdge], None, None]:
        print("TIN-X importance: emitting importance edges")
        disease_cursor = conn.execute("SELECT DISTINCT doid FROM disease_mentions ORDER BY doid")
        batch: List[TINXImportanceEdge] = []
        pair_count = 0
        processed_diseases = 0
        for (doid,) in disease_cursor:
            rows = conn.execute(
                """
                SELECT
                    pm.protein_id,
                    SUM(1.0 / (ppc.n * dpc.n)) AS importance
                FROM disease_mentions dm
                JOIN protein_mentions pm ON pm.pmid = dm.pmid
                JOIN pmid_protein_count ppc ON ppc.pmid = dm.pmid
                JOIN pmid_disease_count dpc ON dpc.pmid = dm.pmid
                WHERE dm.doid = ?
                GROUP BY pm.protein_id
                HAVING importance > 0
                """,
                (doid,),
            )
            for protein_id, importance in rows:
                batch.append(
                    TINXImportanceEdge(
                        start_node=Protein(id=EquivalentId(id=protein_id, type=Prefix.ENSEMBL).id_str()),
                        end_node=Disease(id=doid),
                        details=[
                            DiseaseAssociationDetail(
                                source="TIN-X",
                                source_id=doid,
                                doid=doid,
                                importance=[float(importance)],
                            )
                        ],
                    )
                )
                pair_count += 1
                if len(batch) >= self.batch_size:
                    print(
                        f"TIN-X importance: yielding {len(batch)} edges "
                        f"after {processed_diseases + 1} diseases and {pair_count} total pairs"
                    )
                    yield batch
                    batch = []
                if self.max_pairs is not None and pair_count >= self.max_pairs:
                    break
            processed_diseases += 1
            if processed_diseases % self.progress_every == 0:
                print(
                    f"TIN-X importance: processed {processed_diseases} diseases "
                    f"and emitted {pair_count} total pairs"
                )
            if self.max_pairs is not None and pair_count >= self.max_pairs:
                break
        if batch:
            print(
                f"TIN-X importance: final batch {len(batch)} edges "
                f"after {processed_diseases} diseases and {pair_count} total pairs"
            )
            yield batch
