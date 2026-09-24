import json
import sqlite3

from src.qa_browser.drug_id_graph import DrugGraphData, _index_node
from src.qa_browser.drug_resolver import enrich_pubchem, resolve_and_enrich, resolve_local


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


def test_complete_resolver_index_finds_node_outside_bounded_graph(tmp_path):
    index = tmp_path / "drug_resolver_index.sqlite"
    connection = sqlite3.connect(index)
    connection.executescript("""
        CREATE TABLE nodes (drug_id TEXT PRIMARY KEY, payload TEXT NOT NULL);
        CREATE TABLE aliases (
            alias_norm TEXT NOT NULL, drug_id TEXT NOT NULL, field TEXT NOT NULL,
            matched_value TEXT NOT NULL,
            PRIMARY KEY (alias_norm, drug_id, field, matched_value)
        ) WITHOUT ROWID;
    """)
    node = {
        "drug_id": "IFXDrug:7WGWKYR",
        "standard_name": "2-Amino-N-cyclohexyl-N-methylbenzenesulfonamide",
        "pubchem_cid": "116814",
        "source_namespaces": "GSRS",
    }
    connection.execute("INSERT INTO nodes VALUES (?, ?)", (node["drug_id"], json.dumps(node)))
    connection.execute(
        "INSERT INTO aliases VALUES (?, ?, ?, ?)",
        ("pubchem.compound:116814", node["drug_id"], "xrefs", "PUBCHEM.COMPOUND:116814"),
    )
    connection.commit()
    connection.close()
    data = DrugGraphData()
    data.resolver_index_path = index

    result = resolve_local(data, ["PUBCHEM.COMPOUND:116814"])[0]

    assert result["resolved"] is True
    assert result["local_hits"][0]["drug_id"] == "IFXDrug:7WGWKYR"
    assert result["local_hits"][0]["_match_strategy"] == "complete_harmonizer_index"


def test_complete_index_resolves_equivalent_smiles_through_derived_inchikey(tmp_path):
    index = tmp_path / "drug_resolver_index.sqlite"
    connection = sqlite3.connect(index)
    connection.executescript("""
        CREATE TABLE nodes (drug_id TEXT PRIMARY KEY, payload TEXT NOT NULL);
        CREATE TABLE aliases (alias_norm TEXT NOT NULL, drug_id TEXT NOT NULL, field TEXT NOT NULL, matched_value TEXT NOT NULL);
    """)
    node = {
        "drug_id": "IFXDrug:ASPIRIN_FULL",
        "standard_name": "aspirin",
        "inchikey": "BSYNRYMUTXBXSQ-UHFFFAOYSA-N",
        "smiles": "CC(=O)Oc1ccccc1C(=O)O",
    }
    connection.execute("INSERT INTO nodes VALUES (?, ?)", (node["drug_id"], json.dumps(node)))
    connection.execute(
        "INSERT INTO aliases VALUES (?, ?, ?, ?)",
        ("bsynrymutxbxsq-uhfffaoysa-n", node["drug_id"], "inchikey", node["inchikey"]),
    )
    connection.commit()
    connection.close()
    data = DrugGraphData()
    data.resolver_index_path = index

    result = resolve_local(data, ["O=C(O)c1ccccc1OC(C)=O"])[0]

    assert result["resolved"] is True
    assert result["local_hits"][0]["drug_id"] == "IFXDrug:ASPIRIN_FULL"
    assert result["local_hits"][0]["_match_strategy"] == "complete_harmonizer_index_derived_inchikey"


class _PubChemResponse:
    status_code = 200
    headers = {}

    def raise_for_status(self):
        return None

    def json(self):
        return {"PropertyTable": {"Properties": [{
            "CID": 116814,
            "Title": "2-Amino-N-cyclohexyl-N-methylbenzenesulfonamide",
            "MolecularFormula": "C13H20N2O2S",
            "MolecularWeight": "268.38",
            "SMILES": "CN(C1CCCCC1)S(=O)(=O)C2=CC=CC=C2N",
            "InChIKey": "IPEHSCPRVOWQFQ-UHFFFAOYSA-N",
        }]}}


class _PubChemSession:
    def __init__(self):
        self.url = ""

    def get(self, url, timeout):
        self.url = url
        return _PubChemResponse()


def test_pubchem_inchikey_lookup_preserves_external_candidate_status():
    session = _PubChemSession()
    result = enrich_pubchem("IPEHSCPRVOWQFQ-UHFFFAOYSA-N", session=session)

    assert "/inchikey/IPEHSCPRVOWQFQ-UHFFFAOYSA-N/" in session.url
    assert result["pubchem_cid"] == "116814"
    assert result["pubchem_inchikey"] == "IPEHSCPRVOWQFQ-UHFFFAOYSA-N"


def test_pubchem_is_not_called_when_local_harmonizer_resolves(monkeypatch):
    data = _data({
        "drug_id": "IFXDrug:LOCAL",
        "standard_name": "aspirin",
    })
    called = []
    monkeypatch.setattr(
        "src.qa_browser.drug_resolver.enrich_pubchem",
        lambda *args, **kwargs: called.append(True),
    )

    payload = resolve_and_enrich(
        data,
        ["aspirin"],
        enable_ncats=False,
        enable_pubchem=True,
        enable_pharos=False,
        enable_inxight=False,
        enable_openfda=False,
        enable_chebi=False,
    )

    assert payload["results"][0]["resolved"] is True
    assert called == []
    assert payload["results"][0]["enrichment"]["sources_skipped"]["pubchem"] == "local_harmonizer_resolved"


def test_pubchem_candidate_does_not_mint_local_ifxdrug(monkeypatch):
    data = DrugGraphData()
    monkeypatch.setattr(
        "src.qa_browser.drug_resolver.enrich_pubchem",
        lambda *args, **kwargs: {
            "pubchem_cid": "116814",
            "pubchem_inchikey": "IPEHSCPRVOWQFQ-UHFFFAOYSA-N",
            "candidates": [{
                "pubchem_cid": "116814",
                "pubchem_inchikey": "IPEHSCPRVOWQFQ-UHFFFAOYSA-N",
            }],
        },
    )

    payload = resolve_and_enrich(
        data,
        ["IPEHSCPRVOWQFQ-UHFFFAOYSA-N"],
        enable_ncats=False,
        enable_pubchem=True,
        enable_pharos=False,
        enable_inxight=False,
        enable_openfda=False,
        enable_chebi=False,
    )

    result = payload["results"][0]
    assert result["resolved"] is False
    assert result["local_hits"] == []
    assert result["enrichment"]["pubchem"]["pubchem_cid"] == "116814"


def test_pubchem_relookup_uses_exact_local_cid(monkeypatch):
    data = _data({
        "drug_id": "IFXDrug:7WGWKYR",
        "standard_name": "2-Amino-N-cyclohexyl-N-methylbenzenesulfonamide",
        "xrefs": "PUBCHEM.COMPOUND:116814",
    })
    monkeypatch.setattr(
        "src.qa_browser.drug_resolver.enrich_pubchem",
        lambda *args, **kwargs: {
            "pubchem_cid": "116814",
            "pubchem_inchikey": "IPEHSCPRVOWQFQ-UHFFFAOYSA-N",
            "candidates": [{
                "pubchem_cid": "116814",
                "pubchem_inchikey": "IPEHSCPRVOWQFQ-UHFFFAOYSA-N",
            }],
        },
    )

    payload = resolve_and_enrich(
        data,
        ["IPEHSCPRVOWQFQ-UHFFFAOYSA-N"],
        enable_ncats=False,
        enable_pubchem=True,
        enable_pharos=False,
        enable_inxight=False,
        enable_openfda=False,
        enable_chebi=False,
    )

    result = payload["results"][0]
    assert result["resolved"] is True
    assert result["local_hits"][0]["drug_id"] == "IFXDrug:7WGWKYR"
    assert result["resolved_via"] == "PubChem exact identifier→PUBCHEM.COMPOUND:116814"


def test_pubchem_name_candidates_never_auto_resolve(monkeypatch):
    data = _data({
        "drug_id": "IFXDrug:CANDIDATE",
        "standard_name": "candidate",
        "xrefs": "PUBCHEM.COMPOUND:116814",
    })
    monkeypatch.setattr(
        "src.qa_browser.drug_resolver.enrich_pubchem",
        lambda *args, **kwargs: {
            "pubchem_cid": "116814",
            "candidates": [{"pubchem_cid": "116814", "pubchem_inchikey": "IPEHSCPRVOWQFQ-UHFFFAOYSA-N"}],
        },
    )

    result = resolve_and_enrich(
        data,
        ["ambiguous compound name"],
        enable_ncats=False,
        enable_pubchem=True,
        enable_pharos=False,
        enable_inxight=False,
        enable_openfda=False,
        enable_chebi=False,
    )["results"][0]

    assert result["resolved"] is False
    assert result["local_hits"] == []


def test_pubchem_mismatched_inchikey_never_auto_resolves(monkeypatch):
    data = _data({
        "drug_id": "IFXDrug:CANDIDATE",
        "standard_name": "candidate",
        "xrefs": "PUBCHEM.COMPOUND:116814",
    })
    monkeypatch.setattr(
        "src.qa_browser.drug_resolver.enrich_pubchem",
        lambda *args, **kwargs: {
            "pubchem_cid": "116814",
            "candidates": [{"pubchem_cid": "116814", "pubchem_inchikey": "AAAAAAAAAAAAAA-UHFFFAOYSA-N"}],
        },
    )

    result = resolve_and_enrich(
        data,
        ["IPEHSCPRVOWQFQ-UHFFFAOYSA-N"],
        enable_ncats=False,
        enable_pubchem=True,
        enable_pharos=False,
        enable_inxight=False,
        enable_openfda=False,
        enable_chebi=False,
    )["results"][0]

    assert result["resolved"] is False
