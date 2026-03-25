from sqlalchemy.exc import IntegrityError

from src.output_adapters.mysql_output_adapter import MySQLOutputAdapter, TCRDOutputAdapter
from src.shared.db_credentials import DBCredentials
from src.shared.sqlalchemy_tables.pharos_tables_new import TDL_info
from src.shared.sqlalchemy_tables.test_tables import AutoIncNode


def test_serialize_rows_fills_missing_optional_columns_with_none():
    rows = MySQLOutputAdapter._serialize_rows([
        TDL_info(itype="Ab Count", protein_id=1, integer_value=75),
        TDL_info(itype="JensenLab PubMed Score", protein_id=1, number_value=25.269722),
    ])

    assert rows == [
        {
            "itype": "Ab Count",
            "protein_id": 1,
            "integer_value": 75,
            "number_value": None,
        },
        {
            "itype": "JensenLab PubMed Score",
            "protein_id": 1,
            "integer_value": None,
            "number_value": 25.269722,
        },
    ]


def test_mysql_output_adapter_selects_database_on_init():
    credentials = DBCredentials(
        url="localhost",
        user="tester",
        password="secret",
        schema=None,
    )

    adapter = MySQLOutputAdapter(credentials=credentials, database_name="pharos400", truncate_tables=False)

    assert adapter.credentials.schema == "pharos400"
    assert adapter.connection_string.endswith("/pharos400")


def test_mysql_output_adapter_store_does_not_use_insert_ignore():
    credentials = DBCredentials(
        url="localhost",
        user="tester",
        password="secret",
        schema=None,
    )
    adapter = MySQLOutputAdapter(credentials=credentials, database_name="pharos400", truncate_tables=False)

    class FakeConverter:
        def get_object_converters(self, _obj_cls):
            return lambda _obj: AutoIncNode(identifier="pathway-1", value="demo")

    class FakeSession:
        def __init__(self):
            self.executed = []

        def execute(self, stmt, rows):
            self.executed.append((stmt, rows))

        def commit(self):
            pass

        def rollback(self):
            pass

        def close(self):
            pass

    fake_session = FakeSession()
    adapter.output_converter = FakeConverter()
    adapter.get_session = lambda: fake_session
    adapter.sort_and_convert_objects = lambda _objects, keep_nested_objects=True: {
        "AutoIncNode": ([{"dummy": "value"}], ["AutoIncNode"], False, None, None, object)
    }

    adapter.store(["unused"])

    assert len(fake_session.executed) == 1
    stmt, rows = fake_session.executed[0]
    assert stmt._prefixes == ()
    assert rows == [{"identifier": "pathway-1", "value": "demo"}]


def test_tcrd_output_adapter_preloads_mappings_in_pre_processing():
    credentials = DBCredentials(
        url="localhost",
        user="tester",
        password="secret",
        schema=None,
    )
    adapter = TCRDOutputAdapter(credentials=credentials, database_name="pharos400", truncate_tables=False)

    class FakeConverter:
        def __init__(self):
            self.sessions = []

        def preload_id_mappings(self, session):
            self.sessions.append(session)

    class FakeSession:
        def __init__(self):
            self.closed = False

        def close(self):
            self.closed = True

    fake_converter = FakeConverter()
    fake_session = FakeSession()
    adapter.output_converter = fake_converter
    adapter.get_session = lambda: fake_session

    adapter.do_pre_processing()

    assert fake_converter.sessions == [fake_session]
    assert fake_session.closed is True


def test_mysql_output_adapter_detects_fk_integrity_error():
    exc = IntegrityError(
        "stmt",
        {},
        Exception("Cannot add or update a child row: a foreign key constraint fails"),
    )

    assert MySQLOutputAdapter._is_fk_integrity_error(exc) is True


def test_mysql_output_adapter_store_invokes_fk_diagnosis():
    credentials = DBCredentials(
        url="localhost",
        user="tester",
        password="secret",
        schema=None,
    )
    adapter = MySQLOutputAdapter(credentials=credentials, database_name="pharos400", truncate_tables=False)

    class FakeConverter:
        def get_object_converters(self, _obj_cls):
            return lambda _obj: AutoIncNode(identifier="pathway-1", value="demo")

    class FakeSession:
        def execute(self, stmt, rows):
            raise IntegrityError(
                "stmt",
                rows,
                Exception("Cannot add or update a child row: a foreign key constraint fails"),
            )

        def commit(self):
            pass

        def rollback(self):
            pass

        def close(self):
            pass

    calls = []

    def fake_diagnose(table_class, rows):
        calls.append((table_class, rows))
        raise RuntimeError("isolated bad row")

    adapter.output_converter = FakeConverter()
    adapter.get_session = lambda: FakeSession()
    adapter._diagnose_fk_batch_failure = fake_diagnose
    adapter.sort_and_convert_objects = lambda _objects, keep_nested_objects=True: {
        "AutoIncNode": ([{"dummy": "value"}], ["AutoIncNode"], False, None, None, object)
    }

    try:
        adapter.store(["unused"])
    except RuntimeError as exc:
        assert str(exc) == "isolated bad row"
    else:
        raise AssertionError("Expected store() to raise the diagnostic RuntimeError")

    assert len(calls) == 1
    table_class, rows = calls[0]
    assert table_class is AutoIncNode
    assert rows == [{"identifier": "pathway-1", "value": "demo"}]
