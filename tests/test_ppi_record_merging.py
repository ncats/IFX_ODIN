from src.shared.record_merger import RecordMerger, FieldConflictBehavior


def test_ppi_scores_merge_as_list_values():
    merger = RecordMerger(field_conflict_behavior=FieldConflictBehavior.KeepLast)

    records = [
        {
            "start_id": "IFXProtein:A",
            "end_id": "IFXProtein:B",
            "score": [475],
            "sources": ["STRING\t12.0\t2023-05-16\t2026-04-16"],
            "entity_resolution": "STRING\tStringPPIAdapter\tENSEMBL:ENSP1\tENSEMBL:ENSP2",
            "provenance": "STRING\t12.0\t2023-05-16\t2026-04-16",
        },
        {
            "start_id": "IFXProtein:A",
            "end_id": "IFXProtein:B",
            "score": [477],
            "sources": ["STRING\t12.0\t2023-05-16\t2026-04-16"],
            "entity_resolution": "STRING\tStringPPIAdapter\tENSEMBL:ENSP3\tENSEMBL:ENSP4",
            "provenance": "STRING\t12.0\t2023-05-16\t2026-04-16",
        },
    ]

    merged = merger.merge_records(records, {}, nodes_or_edges="edges")

    assert len(merged) == 1
    assert sorted(merged[0]["score"]) == [475, 477]
    assert any(line.startswith("score\t1 entries already there\t1 entries being merged") for line in merged[0]["updates"])
