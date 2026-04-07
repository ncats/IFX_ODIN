from src.input_adapters.pharos_arango.tcrd.tissue import ExpressionAdapter


class _FakeCredentials:
    url = "unused"
    user = "unused"
    password = "unused"
    schema = None


def test_expression_adapter_honors_max_rows_across_batches():
    adapter = object.__new__(ExpressionAdapter)
    adapter.batch_size = 2
    adapter.max_rows = 3

    calls = []
    batches = [
        [
            {"_key": "001", "start_id": "P1", "end_id": "T1", "details": []},
            {"_key": "002", "start_id": "P2", "end_id": "T2", "details": []},
        ],
        [
            {"_key": "003", "start_id": "P3", "end_id": "T3", "details": []},
        ],
        [],
    ]

    def fake_run_query(query):
        calls.append(query)
        return batches[len(calls) - 1]

    adapter.runQuery = fake_run_query

    result_batches = list(adapter.get_all())

    assert [len(batch) for batch in result_batches] == [2, 1]
    assert result_batches[0][0].start_node.id == "P1"
    assert result_batches[1][0].start_node.id == "P3"
    assert "LIMIT 2" in calls[0]
    assert "LIMIT 1" in calls[1]
