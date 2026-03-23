from src.input_adapters.pharos_arango.tcrd.pathway import ProteinPathwayAdapter, pathway_query


def test_protein_pathway_adapter_hydrates_end_node_from_pathway_lookup():
    adapter = ProteinPathwayAdapter.__new__(ProteinPathwayAdapter)
    adapter.batch_size = 10_000

    pathway_rows = [{
        'id': 'Pathway:reactome:R-HSA-1234',
        'source_id': 'R-HSA-1234',
        'type': 'Reactome',
        'name': 'Hemostasis',
        'url': 'https://reactome.org/content/detail/R-HSA-1234',
    }]
    edge_rows = [{
        '_key': '1',
        'start_id': 'UniProtKB:P12345',
        'end_id': 'Pathway:reactome:R-HSA-1234',
        'source': 'UniProt',
    }]

    def fake_run_query(query):
        if query == pathway_query():
            return pathway_rows
        if 'FOR rel IN `ProteinPathwayEdge`' in query:
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
    assert edge.end_node.id == 'Pathway:reactome:R-HSA-1234'
    assert edge.end_node.source_id == 'R-HSA-1234'
    assert edge.end_node.type == 'Reactome'
    assert edge.end_node.name == 'Hemostasis'
    assert edge.end_node.url == 'https://reactome.org/content/detail/R-HSA-1234'
    assert edge.source == 'UniProt'
