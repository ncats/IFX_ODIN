import csv
import gzip
from collections import Counter, defaultdict
from datetime import datetime
from typing import DefaultDict, Dict, Generator, List, Optional

from sqlalchemy import text

from src.constants import DataSourceName, Prefix
from src.input_adapters.sql_adapter import MySqlAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.gene import Gene
from src.models.node import EquivalentId
from src.models.year_score import YearScore
from src.shared.db_credentials import DBCredentials


class PubTatorPublicationStatisticsAdapter(InputAdapter, MySqlAdapter):
    batch_size = 1000

    def __init__(
        self,
        credentials: DBCredentials,
        data_source,
        pmid_batch_size: int = 5000,
        max_rows: Optional[int] = None,
    ):
        MySqlAdapter.__init__(self, credentials)
        self.gene2pubtator3_file_path = str(data_source.file("gene2pubtator3.gz"))
        self.pmid_batch_size = pmid_batch_size
        self.max_rows = max_rows
        self.version_info = data_source.version_info()

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.PubTator

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def get_all(self) -> Generator[List[Gene], None, None]:
        yearly_scores: DefaultDict[int, Dict[int, float]] = defaultdict(dict)
        pending_pmids: Dict[int, Counter[int]] = {}
        current_pmid: Optional[int] = None
        current_gene_counts: Counter[int] = Counter()
        processed_rows = 0

        csv.field_size_limit(10_000_000)
        with gzip.open(self.gene2pubtator3_file_path, "rt", encoding="utf-8", newline="") as handle:
            reader = csv.reader(handle, delimiter="\t")
            for row in reader:
                if len(row) < 5:
                    continue
                processed_rows += 1
                if self.max_rows is not None and processed_rows > self.max_rows:
                    break

                pmid = self._parse_int(row[0])
                gene_id = self._parse_int(row[2])
                if pmid is None or gene_id is None:
                    continue

                if current_pmid is None:
                    current_pmid = pmid

                if pmid != current_pmid:
                    pending_pmids[current_pmid] = current_gene_counts
                    if len(pending_pmids) >= self.pmid_batch_size:
                        self._flush_pending_pmids(pending_pmids, yearly_scores)
                        pending_pmids = {}
                    current_pmid = pmid
                    current_gene_counts = Counter()

                current_gene_counts[gene_id] += 1

        if current_pmid is not None and current_gene_counts:
            pending_pmids[current_pmid] = current_gene_counts
        if pending_pmids:
            self._flush_pending_pmids(pending_pmids, yearly_scores)

        batch: List[Gene] = []
        for gene_id in sorted(yearly_scores):
            year_scores = sorted(
                [
                    YearScore(year=year, score=score)
                    for year, score in yearly_scores[gene_id].items()
                ],
                key=lambda entry: entry.year if entry.year is not None else 0,
            )
            total_score = sum(entry.score for entry in year_scores if entry.score is not None)
            batch.append(
                Gene(
                    id=EquivalentId(id=str(gene_id), type=Prefix.NCBIGene).id_str(),
                    pt_score=[total_score],
                    pt_score_by_year=year_scores,
                )
            )
            if len(batch) >= self.batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

    def _flush_pending_pmids(
        self,
        pending_pmids: Dict[int, Counter[int]],
        yearly_scores: DefaultDict[int, Dict[int, float]],
    ) -> None:
        year_map = self._get_pub_year_map(list(pending_pmids.keys()))
        for pmid, gene_counts in pending_pmids.items():
            pub_year = year_map.get(pmid)
            if pub_year is None:
                continue
            total_gene_count = sum(gene_counts.values())
            if total_gene_count <= 0:
                continue
            for gene_id, gene_count in gene_counts.items():
                fractional_score = gene_count / total_gene_count
                yearly_scores[gene_id][pub_year] = yearly_scores[gene_id].get(pub_year, 0.0) + fractional_score

    def _get_pub_year_map(self, pmids: List[int]) -> Dict[int, int]:
        if not pmids:
            return {}
        session = self.get_session()
        try:
            params = {f"pmid_{idx}": pmid for idx, pmid in enumerate(pmids)}
            placeholders = ", ".join(f":pmid_{idx}" for idx in range(len(pmids)))
            rows = session.execute(
                text(f"SELECT id, pub_year FROM pubmed WHERE id IN ({placeholders}) AND pub_year IS NOT NULL"),
                params
            ).fetchall()
        finally:
            session.close()

        return {
            int(row[0]): int(row[1])
            for row in rows
            if row[0] is not None and row[1] is not None
        }

    @staticmethod
    def _parse_int(raw_value: Optional[str]) -> Optional[int]:
        if raw_value is None:
            return None
        try:
            return int(str(raw_value).strip())
        except ValueError:
            return None
