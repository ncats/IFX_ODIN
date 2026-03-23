from src.input_adapters.pharos_arango.tcrd.keyword import ProteinKeywordAdapter, keyword_query


def test_protein_keyword_adapter_hydrates_end_node_from_keyword_lookup():
    adapter = ProteinKeywordAdapter.__new__(ProteinKeywordAdapter)
    adapter.batch_size = 10_000

    keyword_rows = [{
        'id': 'keyword:uniprot:biological_process:transport',
        'category': 'Biological process',
        'source': 'UniProt',
        'value': 'Transport',
    }]
    edge_rows = [{
        '_key': '1',
        'start_id': 'UniProtKB:P12345',
        'end_id': 'keyword:uniprot:biological_process:transport',
    }]

    def fake_run_query(query):
        if query == keyword_query():
            return keyword_rows
        if 'FOR rel IN `ProteinKeywordEdge`' in query:
            rows = edge_rows.copy()
            edge_rows.clear()
            return rows
        return []

    adapter.runQuery = fake_run_query

    batches = list(adapter.get_all())

    assert len(batches) == 1
    assert len(batches[0]) == 1
    edge = batches[0][0]
    assert edge.start_node.id == 'UniProtKB:P12345'
    assert edge.end_node.id == 'keyword:uniprot:biological_process:transport'
    assert edge.end_node.category == 'Biological process'
    assert edge.end_node.source == 'UniProt'
    assert edge.end_node.value == 'Transport'
