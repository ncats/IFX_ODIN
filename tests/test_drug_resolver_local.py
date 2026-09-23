from src.qa_browser.drug_id_graph import DrugGraphData, _index_node
from src.qa_browser.drug_resolver import resolve_local


def _data(*nodes):
    data = DrugGraphData()
    for node in nodes:
        _index_node(data, node)
    return data


def test_resolves_inchikey_from_nodenorm_equivalent_identifiers_without_fuzzy_scan():
    data = _data({
        "drug_id": "IFXDrug:PFH5OUT",
        "standard_name": "norspermidine",
        "nodenorm_equivalent_identifiers": "CHEBI:16841|INCHIKEY:OTBHHUPVCYLGQO-UHFFFAOYSA-N",
    })

    result = resolve_local(data, ["OTBHHUPVCYLGQO-UHFFFAOYSA-N"])[0]

    assert result["resolved"] is True
    assert result["local_hits"][0]["drug_id"] == "IFXDrug:PFH5OUT"
    assert result["local_hits"][0]["_match_strategy"] == "exact_alias"


def test_resolves_equivalent_smiles_by_stereo_sensitive_canonical_structure():
    data = _data({
        "drug_id": "IFXDrug:ASPIRIN",
        "standard_name": "aspirin",
        "smiles": "CC(=O)Oc1ccccc1C(=O)O",
    })

    result = resolve_local(data, ["O=C(O)c1ccccc1OC(C)=O"])[0]

    assert result["resolved"] is True
    assert result["local_hits"][0]["drug_id"] == "IFXDrug:ASPIRIN"
    assert result["local_hits"][0]["smiles"] == "CC(=O)Oc1ccccc1C(=O)O"
    assert result["local_hits"][0]["_match_strategy"] == "canonical_structure"
    assert result["local_hits"][0]["_matched_field"] == "canonical_smiles"


def test_resolves_exact_stored_smiles_and_labels_exact_match():
    data = _data({
        "drug_id": "IFXDrug:ASPIRIN",
        "standard_name": "aspirin",
        "smiles": "CC(=O)Oc1ccccc1C(=O)O",
    })

    result = resolve_local(data, ["CC(=O)Oc1ccccc1C(=O)O"])[0]

    assert result["resolved"] is True
    assert result["local_hits"][0]["_match_strategy"] == "exact_smiles"
    assert result["local_hits"][0]["_matched_field"] == "smiles"


def test_stereoisomer_smiles_do_not_cross_resolve():
    data = _data({
        "drug_id": "IFXDrug:R",
        "standard_name": "R form",
        "smiles": "C[C@H](O)C(=O)O",
    })

    result = resolve_local(data, ["C[C@@H](O)C(=O)O"])[0]

    assert result["resolved"] is False


def test_name_and_smiles_collision_returns_both_interpretations():
    data = _data(
        {"drug_id": "IFXDrug:NITRIC_OXIDE", "standard_name": "NO"},
        {"drug_id": "IFXDrug:HYDROXYLAMINE", "standard_name": "hydroxylamine", "smiles": "NO"},
    )

    result = resolve_local(data, ["NO"])[0]

    assert result["resolved"] is True
    assert {hit["drug_id"] for hit in result["local_hits"]} == {
        "IFXDrug:NITRIC_OXIDE",
        "IFXDrug:HYDROXYLAMINE",
    }


def test_single_atom_smiles_resolves():
    data = _data({
        "drug_id": "IFXDrug:METHANE",
        "standard_name": "methane",
        "smiles": "C",
    })

    result = resolve_local(data, ["C"])[0]

    assert result["resolved"] is True
    assert result["local_hits"][0]["drug_id"] == "IFXDrug:METHANE"
    assert result["local_hits"][0]["_match_strategy"] == "exact_smiles"


def test_derived_inchikey_match_reports_actual_index_field_and_value():
    data = _data({
        "drug_id": "IFXDrug:ASPIRIN",
        "standard_name": "aspirin",
        "smiles": "CC(=O)Oc1ccccc1C(=O)O",
        "nodenorm_equivalent_identifiers": "INCHIKEY:BSYNRYMUTXBXSQ-UHFFFAOYSA-N",
    })

    result = resolve_local(data, ["O=C(O)c1ccccc1OC(C)=O"])[0]
    hit = result["local_hits"][0]

    assert hit["_match_strategy"] == "derived_inchikey"
    assert hit["_matched_field"] == "nodenorm_equivalent_identifiers"
    assert hit["_matched_value"] == "INCHIKEY:BSYNRYMUTXBXSQ-UHFFFAOYSA-N"


def test_smiles_does_not_fabricate_unii_alias():
    data = _data({
        "drug_id": "IFXDrug:DECANE",
        "standard_name": "decane",
        "smiles": "CCCCCCCCCC",
        "unii": "NK85062OIY",
    })

    false_identifier = resolve_local(data, ["UNII:CCCCCCCCCC"])[0]
    actual_identifier = resolve_local(data, ["UNII:NK85062OIY"])[0]

    assert false_identifier["resolved"] is False
    assert actual_identifier["resolved"] is True
    assert actual_identifier["local_hits"][0]["drug_id"] == "IFXDrug:DECANE"


def test_missing_text_does_not_scan_synonym_payload_as_fuzzy_candidates():
    data = _data({
        "drug_id": "IFXDrug:ONE",
        "standard_name": "aspirin",
        "synonyms": "|".join(f"large synonym payload {index}" for index in range(1000)),
    })

    result = resolve_local(data, ["this drug definitely does not exist xyzzy"])[0]

    assert result["resolved"] is False
    assert data._fuzzy_name_index == [
        ("IFXDrug:ONE", "standard_name", "aspirin", "aspirin")
    ]
