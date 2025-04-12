from src.shared.record_merger import RecordMerger, FieldConflictBehavior

def get_existing_records():
    return {
        "1": {
            "id": "1",
            "name": "Aleece",
            "old_field": "value2",
            "old_list_field": ["old_value"],
            "xref": ["existing_xref"],
            "creation": "source2",
            "updates": ["update1"],
            "resolved_ids": ["res2"]
        }
    }

def get_merging_records():
    return [
        {"id": "1", "name": "Alice", "new_field": "value1", "new_list_field": ["new_value"], "empty_list_field": [], "old_list_field": ["another_value"], "entity_resolution": "res1", "provenance": "source1", "xref": ["xref1"]}
    ]

def test_basic_record_merging():

    for field_conflict_behavior in [FieldConflictBehavior.KeepFirst, FieldConflictBehavior.KeepLast]:
        # Arrange
        existing_record_map = get_existing_records()
        records = get_merging_records()
        merger = RecordMerger(field_conflict_behavior=field_conflict_behavior)

        # Act
        merged_records = merger.merge_records(records, existing_record_map)
        print(merged_records)
        # Assert

        assert len(merged_records) == 1
        assert merged_records[0]["id"] == "1"
        assert merged_records[0]["name"] == "Alice" if field_conflict_behavior == FieldConflictBehavior.KeepLast else "Aleece"
        assert merged_records[0]["new_field"] == "value1"
        assert "xref" not in merged_records[0]
        assert merged_records[0]['creation'] == "source2"

        assert sorted(merged_records[0]["old_list_field"]) == sorted(["old_value", "another_value"])
        assert merged_records[0]["new_list_field"] == ["new_value"]

        assert merged_records[0]["updates"] == [
            'update1',
            f'name\tAleece\tAlice\tsource1\t{field_conflict_behavior.value}',
            f'new_field\tNULL\tvalue1\tsource1\t{field_conflict_behavior.value}',
            'new_list_field\tNULL\t1 entries being merged\tsource1',
            'old_list_field\t1 entries already there\t1 entries being merged\tsource1'
        ]
        assert sorted(merged_records[0]["resolved_ids"]) == sorted(["res2", "res1"])

def test_new_record_merging():

    # Arrange
    existing_record_map = {}
    records = get_merging_records()
    merger = RecordMerger(field_conflict_behavior=FieldConflictBehavior.KeepLast)

    # Act
    merged_records = merger.merge_records(records, existing_record_map)

    assert len(merged_records) == 1
    assert merged_records[0]["id"] == "1"
    assert merged_records[0]["name"] == "Alice"
    assert merged_records[0]["new_field"] == "value1"

    assert merged_records[0]["xref"] == ["xref1"]
    assert merged_records[0]["creation"] == "source1"
    assert merged_records[0]["old_list_field"] == ["another_value"]
    assert merged_records[0]["new_list_field"] == ["new_value"]
    assert "updates" not in merged_records[0]

    assert merged_records[0]["resolved_ids"] == ["res1"]

def test_double_merge():
    # Arrange
    existing_record_map = get_existing_records()
    records = [
        {"id": "1", "name": "Alice", "new_field": "value1", "entity_resolution": "res1", "provenance": "source1", "xref": ["xref1"], "old_list_field": ["another_value"]},
        {"id": "1", "name": "Aleece", "new_field": "value1", "entity_resolution": "res2", "provenance": "source2", "xref": ["xref2"], "old_list_field": ["yet_another_value"]}
    ]
    merger = RecordMerger(field_conflict_behavior=FieldConflictBehavior.KeepLast)

    # Act
    merged_records = merger.merge_records(records, existing_record_map)

    # Assert
    assert len(merged_records) == 1
    assert merged_records[0]["id"] == "1"
    assert merged_records[0]["name"] == "Aleece"
    assert merged_records[0]["new_field"] == "value1"
    assert "xref" not in merged_records[0]
    assert merged_records[0]['creation'] == "source2"
    assert sorted(merged_records[0]["old_list_field"]) == sorted(["old_value", "another_value", "yet_another_value"])

    assert merged_records[0]["updates"] == [
        'update1',
        'name\tAleece\tAlice\tsource1\tKeepLast',
        'new_field\tNULL\tvalue1\tsource1\tKeepLast',
        'old_list_field\t1 entries already there\t1 entries being merged\tsource1',
        'name\tAlice\tAleece\tsource2\tKeepLast',
        'old_list_field\t2 entries already there\t1 entries being merged\tsource2',
    ]
    assert sorted(merged_records[0]["resolved_ids"]) == sorted(["res2", "res1"])