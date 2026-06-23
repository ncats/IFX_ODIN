import pytest
from fastapi import HTTPException

from src.qa_browser import ramp_id_graph


def test_add_ramp_diagnosis_persists_reviewer_decision_and_case_key(tmp_path):
    diagnosis_file = tmp_path / "ramp_diagnoses.json"
    ramp_id_graph.set_ramp_diagnosis_file(str(diagnosis_file))

    entry = ramp_id_graph.add_ramp_diagnosis(
        ramp_ids=["RAMP_C_000001819", "RAMP_C_000000077"],
        selected_ramp_ids=["RAMP_C_000001819"],
        diagnosis="modified_metabolite_should_not_group",
        reviewer="Keith",
        note="Sulfated form should stay separate.",
        workbook_row=12,
    )

    assert entry["case_key"] == "RAMP_C_000000077|RAMP_C_000001819"
    assert entry["reviewer"] == "Keith"
    assert entry["decision"] == "should_not_group"
    assert entry["decision_label"] == "Should not be grouped"
    assert entry["diagnosis_label"] == "Modified metabolite"
    assert entry["selected_ramp_ids"] == ["RAMP_C_000001819"]
    assert entry["workbook_row"] == 12

    diagnoses = ramp_id_graph.get_ramp_diagnoses(["RAMP_C_000000077", "RAMP_C_000001819"])
    assert len(diagnoses) == 1
    assert diagnoses[0]["note"] == "Sulfated form should stay separate."


def test_add_ramp_diagnosis_rejects_unknown_category(tmp_path):
    ramp_id_graph.set_ramp_diagnosis_file(str(tmp_path / "ramp_diagnoses.json"))

    with pytest.raises(HTTPException) as exc:
        ramp_id_graph.add_ramp_diagnosis(
            ramp_ids=["RAMP_C_000000077"],
            diagnosis="not_a_category",
            reviewer="Keith",
        )

    assert exc.value.status_code == 400


def test_attach_ramp_diagnosis_summaries_counts_decisions(tmp_path):
    ramp_id_graph.set_ramp_diagnosis_file(str(tmp_path / "ramp_diagnoses.json"))
    ramp_ids = ["RAMP_C_000000077", "RAMP_C_000001819"]
    ramp_id_graph.add_ramp_diagnosis(
        ramp_ids=ramp_ids,
        selected_ramp_ids=ramp_ids,
        diagnosis="synonym_names_can_group",
        reviewer="Keith",
    )
    ramp_id_graph.add_ramp_diagnosis(
        ramp_ids=ramp_ids,
        selected_ramp_ids=["RAMP_C_000001819"],
        diagnosis="distinct_compound_should_not_group",
        reviewer="Jessica",
    )

    rows = ramp_id_graph.attach_ramp_diagnosis_summaries([
        {"finalRampIds": "RAMP_C_000001819 | RAMP_C_000000077"}
    ])

    summary = rows[0]["curationSummary"]
    assert summary["total"] == 2
    assert summary["can_group_count"] == 1
    assert summary["should_not_group_count"] == 1
    assert summary["reviewers"] == ["Jessica", "Keith"]


def test_delete_ramp_diagnosis_removes_one_entry(tmp_path):
    ramp_id_graph.set_ramp_diagnosis_file(str(tmp_path / "ramp_diagnoses.json"))
    kept = ramp_id_graph.add_ramp_diagnosis(
        ramp_ids=["RAMP_C_1", "RAMP_C_2"],
        selected_ramp_ids=["RAMP_C_1"],
        diagnosis="synonym_names_can_group",
        reviewer="Keith",
    )
    removed = ramp_id_graph.add_ramp_diagnosis(
        ramp_ids=["RAMP_C_1", "RAMP_C_2"],
        selected_ramp_ids=["RAMP_C_2"],
        diagnosis="distinct_compound_should_not_group",
        reviewer="Jessica",
    )

    assert ramp_id_graph.delete_ramp_diagnosis(removed["id"]) is True
    assert ramp_id_graph.delete_ramp_diagnosis("missing") is False

    diagnoses = ramp_id_graph.get_ramp_diagnoses(["RAMP_C_1", "RAMP_C_2"])
    assert [entry["id"] for entry in diagnoses] == [kept["id"]]


def test_add_ramp_diagnosis_requires_reviewer(tmp_path):
    ramp_id_graph.set_ramp_diagnosis_file(str(tmp_path / "ramp_diagnoses.json"))

    with pytest.raises(HTTPException) as exc:
        ramp_id_graph.add_ramp_diagnosis(
            ramp_ids=["RAMP_C_000000077"],
            diagnosis="synonym_names_can_group",
            reviewer=" ",
        )

    assert exc.value.status_code == 400


def test_ramp_graph_compact_mode_links_inchikeys_to_ramp_ids(tmp_path, monkeypatch):
    db_path = tmp_path / "ramp.sqlite"
    db_path.write_text("")
    monkeypatch.setattr(ramp_id_graph, "fetch_analytes", lambda _db, _ids: [
        {"rampId": "RAMP_C_1", "type": "compound", "common_name": "Metabolite"}
    ])
    monkeypatch.setattr(ramp_id_graph, "fetch_source_rows", lambda _db, _ids: [
        {
            "rampId": "RAMP_C_1",
            "rampName": "Metabolite",
            "sourceId": "HMDB00001",
            "IDtype": "hmdb",
            "dataSource": "HMDB",
            "sourceName": "Metabolite source",
        }
    ])
    monkeypatch.setattr(ramp_id_graph, "fetch_chem_props", lambda _db, _ids: [
        {
            "ramp_id": "RAMP_C_1",
            "chem_data_source": "HMDB",
            "chem_source_id": "HMDB00001",
            "mw": 100,
            "monoisotop_mass": 99,
            "common_name": "Metabolite source",
            "mol_formula": "C1H1",
            "iso_smiles": "",
            "inchi_key": "AAAA-BBBB",
        }
    ])

    payload = ramp_id_graph.build_ramp_graph_payload(db_path, ["RAMP_C_1"], show_individual_ids=False)

    element_ids = {element["data"]["id"] for element in payload["elements"]}
    assert "source::HMDB00001" not in element_ids
    assert payload["stats"]["sourceCount"] == 0
    assert any(
        element["data"].get("source") == "RAMP_C_1"
        and element["data"].get("target") == "inchikey::AAAA-BBBB"
        for element in payload["elements"]
    )


def test_ramp_graph_individual_mode_keeps_source_id_layer(tmp_path, monkeypatch):
    db_path = tmp_path / "ramp.sqlite"
    db_path.write_text("")
    monkeypatch.setattr(ramp_id_graph, "fetch_analytes", lambda _db, _ids: [
        {"rampId": "RAMP_C_1", "type": "compound", "common_name": "Metabolite"}
    ])
    monkeypatch.setattr(ramp_id_graph, "fetch_source_rows", lambda _db, _ids: [
        {
            "rampId": "RAMP_C_1",
            "rampName": "Metabolite",
            "sourceId": "HMDB00001",
            "IDtype": "hmdb",
            "dataSource": "HMDB",
            "sourceName": "Metabolite source",
        }
    ])
    monkeypatch.setattr(ramp_id_graph, "fetch_chem_props", lambda _db, _ids: [
        {
            "ramp_id": "RAMP_C_1",
            "chem_data_source": "HMDB",
            "chem_source_id": "HMDB00001",
            "mw": 100,
            "monoisotop_mass": 99,
            "common_name": "Metabolite source",
            "mol_formula": "C1H1",
            "iso_smiles": "",
            "inchi_key": "AAAA-BBBB",
        }
    ])

    payload = ramp_id_graph.build_ramp_graph_payload(db_path, ["RAMP_C_1"], show_individual_ids=True)

    element_ids = {element["data"]["id"] for element in payload["elements"]}
    assert "source::HMDB00001" in element_ids
    assert payload["stats"]["sourceCount"] == 1
    assert any(
        element["data"].get("source") == "source::HMDB00001"
        and element["data"].get("target") == "inchikey::AAAA-BBBB"
        for element in payload["elements"]
    )
