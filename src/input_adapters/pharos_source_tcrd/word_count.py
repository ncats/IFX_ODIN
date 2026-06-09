import re
from collections import Counter
from dataclasses import replace
from typing import Generator, Iterable, List, Optional, Set, Union

from sqlalchemy import bindparam, text

from src.constants import DataSourceName
from src.input_adapters.sql_adapter import MySqlAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import Relationship
from src.models.word_count import WordCount
from src.shared.db_credentials import DBCredentials


class WordCountAdapter(InputAdapter):
    batch_size = 100000
    word_pattern = re.compile(r"\b[A-Za-z]+[A-Za-z0-9\-\./\+]{1,}\b|\b[0-9]+[A-Za-z]+[0-9]+\b")

    def __init__(
        self,
        credentials: DBCredentials,
        database_name: str,
        pubmed_credentials: DBCredentials | dict,
        pmid_batch_size: int = 10000,
        max_pmids: Optional[int] = None,
        max_word_length: int = 128,
    ):
        self.target_adapter = MySqlAdapter(replace(credentials, schema=database_name))
        self.pubmed_adapter = MySqlAdapter(self._coerce_credentials(pubmed_credentials))
        self.pmid_batch_size = pmid_batch_size
        self.max_pmids = max_pmids
        self.max_word_length = max_word_length

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.NCBI

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo()

    @staticmethod
    def _coerce_credentials(credentials: DBCredentials | dict) -> DBCredentials:
        if isinstance(credentials, DBCredentials):
            return credentials
        return DBCredentials(
            user=credentials.get("user"),
            url=credentials["url"],
            password=credentials.get("password"),
            schema=credentials.get("schema"),
            port=credentials.get("port"),
            internal_url=credentials.get("internal_url", credentials["url"]),
        )

    @classmethod
    def words_in_abstract(cls, abstract: str, max_word_length: int = 128) -> Set[str]:
        words = set()
        for match in cls.word_pattern.findall(abstract or ""):
            word = match.lower()
            if len(word) <= max_word_length:
                words.add(word)
        return words

    def get_all(self) -> Generator[List[Union[WordCount, Relationship]], None, None]:
        word_counts: Counter[str] = Counter()
        abstract_count = 0
        processed_pmids = 0

        for pmid_chunk in self._iter_pmid_chunks():
            processed_pmids += len(pmid_chunk)
            print(f"WordCount: processing {processed_pmids} NCBI-linked PMIDs")
            for abstract in self._get_abstracts(pmid_chunk):
                words = self.words_in_abstract(abstract, max_word_length=self.max_word_length)
                if not words:
                    continue
                abstract_count += 1
                word_counts.update(words)

        word_counts["__ABSTRACT_COUNT__"] = abstract_count

        batch: List[WordCount] = []
        for word in sorted(word_counts):
            batch.append(WordCount(id=word, word=word, count=word_counts[word]))
            if len(batch) >= self.batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

    def _iter_pmid_chunks(self) -> Generator[List[int], None, None]:
        emitted = 0
        last_pmid = -1
        query = text(
            "SELECT DISTINCT pubmed_id FROM protein2pubmed "
            "WHERE source = 'NCBI' AND pubmed_id IS NOT NULL AND pubmed_id > :last_pmid "
            "ORDER BY pubmed_id "
            "LIMIT :limit"
        )

        while True:
            remaining = None if self.max_pmids is None else self.max_pmids - emitted
            if remaining is not None and remaining <= 0:
                return

            limit = min(self.pmid_batch_size, remaining) if remaining is not None else self.pmid_batch_size

            with self.target_adapter.get_engine().connect() as conn:
                rows = conn.execute(query, {"last_pmid": last_pmid, "limit": limit}).fetchall()

            if not rows:
                return

            chunk = [int(row[0]) for row in rows]
            emitted += len(chunk)
            last_pmid = chunk[-1]
            yield chunk

    def _get_abstracts(self, pmids: Iterable[int]) -> Generator[str, None, None]:
        pmid_list = list(pmids)
        if not pmid_list:
            return

        query = text(
            "SELECT abstract FROM pubmed "
            "WHERE id IN :pmids AND abstract IS NOT NULL AND abstract <> ''"
        ).bindparams(bindparam("pmids", expanding=True))

        with self.pubmed_adapter.get_engine().connect() as conn:
            for row in conn.execute(query, {"pmids": pmid_list}):
                abstract = row[0]
                if abstract:
                    yield abstract
