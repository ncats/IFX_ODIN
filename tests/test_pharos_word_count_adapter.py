from src.input_adapters.pharos_source_tcrd.word_count import WordCountAdapter
from src.models.word_count import WordCount
from src.output_adapters.sql_converters.tcrd import TCRDOutputConverter
from src.shared.db_credentials import DBCredentials


class _FakeResult:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _FakeConnection:
    def __init__(self, engine):
        self._engine = engine

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params):
        self._engine.executed_params.append(params)
        last_pmid = params["last_pmid"]
        limit = params["limit"]
        rows = [(pmid,) for pmid in self._engine.pmids if pmid > last_pmid][:limit]
        return _FakeResult(rows)


class _FakeEngine:
    def __init__(self, pmids):
        self.pmids = pmids
        self.executed_params = []

    def connect(self):
        return _FakeConnection(self)


class _FakeAdapter:
    def __init__(self, engine):
        self._engine = engine

    def get_engine(self):
        return self._engine


def test_words_in_abstract_uses_legacy_document_frequency_tokenizer():
    words = WordCountAdapter.words_in_abstract(
        "TP53 TP53 p53 IL-2 alpha/beta 3abc4 A x C."
    )

    assert words == {"tp53", "p53", "il-2", "alpha/beta", "3abc4"}


def test_word_count_converter_emits_mysql_word_count_row():
    converter = TCRDOutputConverter()

    row = converter.word_count_converter(WordCount(id="tp53", word="tp53", count=17).__dict__)

    assert row.word == "tp53"
    assert row.count == 17


def test_iter_pmid_chunks_uses_bounded_keyset_queries():
    adapter = WordCountAdapter(
        DBCredentials(url="target", user="user", password="password"),
        database_name="target_db",
        pubmed_credentials=DBCredentials(url="pubmed", user="user", password="password"),
        pmid_batch_size=2,
    )
    engine = _FakeEngine([1, 3, 5, 8, 13])
    adapter.target_adapter = _FakeAdapter(engine)

    chunks = list(adapter._iter_pmid_chunks())

    assert chunks == [[1, 3], [5, 8], [13]]
    assert engine.executed_params == [
        {"last_pmid": -1, "limit": 2},
        {"last_pmid": 3, "limit": 2},
        {"last_pmid": 8, "limit": 2},
        {"last_pmid": 13, "limit": 2},
    ]


def test_iter_pmid_chunks_honors_max_pmids_without_extra_page():
    adapter = WordCountAdapter(
        DBCredentials(url="target", user="user", password="password"),
        database_name="target_db",
        pubmed_credentials=DBCredentials(url="pubmed", user="user", password="password"),
        pmid_batch_size=2,
        max_pmids=3,
    )
    engine = _FakeEngine([1, 3, 5, 8, 13])
    adapter.target_adapter = _FakeAdapter(engine)

    chunks = list(adapter._iter_pmid_chunks())

    assert chunks == [[1, 3], [5]]
    assert engine.executed_params == [
        {"last_pmid": -1, "limit": 2},
        {"last_pmid": 3, "limit": 1},
    ]
