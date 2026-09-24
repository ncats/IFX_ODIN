import json
import sqlite3
import pytest

from src.qa_browser.drug_id_graph import (
    DrugGraphData,
    _index_node,
    _validate_drug_resolver_index,
    compute_drug_stats,
    load_drug_graph_data,
)
from src.qa_browser.drug_resolver import (
    _reconcile_live_identity,
    enrich_ncats_resolver,
    enrich_pubchem,
    resolve_and_enrich,
    resolve_local,
)


def _data(*nodes):
    data = DrugGraphData()
    for node in nodes:
        _index_node(data, node)
    return data


class _NCATSResponse:
    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code


class _NCATSSession:
    def __init__(self, fail_property=None, fail_status=400, blank=False):
        self.fail_property = fail_property
        self.fail_status = fail_status
        self.blank = blank
        self.groups = []

    def get(self, url, params, timeout):
        props = url.rstrip("/").split("/resolver/")[1].split("/")
        self.groups.append(props)
        if self.fail_property in props:
            return _NCATSResponse("", self.fail_status)
        if self.blank:
            return _NCATSResponse(params["structure"] + "\t" * len(props) + "\n")
        values = [params["structure"]] + ["0" if prop == "hbd" else f"value-{prop}" for prop in props]
        return _NCATSResponse("\t".join(values) + "\n")


def test_ncats_selected_properties_are_batched_and_zero_values_are_preserved():
    props = ["cid", "chembl", "chebi", "unii", "cas", "pt", "names", "devphase", "hbd"]
    session = _NCATSSession()

    result = enrich_ncats_resolver("aspirin", session=session, props=props)

    assert [len(group) for group in session.groups] == [8, 1]
    assert result["_ncats_props_requested"] == props
    assert result["_ncats_props_returned"] == props
    assert result["ncats_hbd"] == "0"
    assert result["_ncats_partial"] is False


def test_ncats_failed_batch_remains_bounded_and_reports_whole_group():
    props = ["cid", "chembl", "chebi", "unii"]
    session = _NCATSSession(fail_property="chebi")
    source_errors = []

    result = enrich_ncats_resolver(
        "aspirin", session=session, props=props, source_errors=source_errors,
    )

    assert result["_ncats_status"] == "failed"
    assert result["_ncats_props_failed"] == props
    assert result["_ncats_batch_count"] == 1
    assert source_errors


def test_ncats_transient_batch_failure_is_not_recursively_split():
    props = ["cid", "chembl", "chebi", "unii"]
    session = _NCATSSession(fail_property="chebi", fail_status=503)

    result = enrich_ncats_resolver("aspirin", session=session, props=props)

    assert result["_ncats_status"] == "failed"
    assert result["_ncats_props_failed"] == props
    assert result["_ncats_batch_count"] == 1
    assert len(session.groups) == 2  # two retries, no recursive property fan-out


def test_ncats_failed_first_batch_circuit_breaks_remaining_property_batches():
    props = [
        "cid", "chembl", "chebi", "unii", "cas", "pt", "names", "devphase",
        "description", "moa", "drugbankmoa", "npcmoa", "ptargets", "cns", "drug", "hbd", "hba",
    ]
    session = _NCATSSession(fail_property="chebi", fail_status=503)

    result = enrich_ncats_resolver("aspirin", session=session, props=props)

    assert len(session.groups) == 2  # first batch retried once; later batches were not attempted
    assert result["_ncats_status"] == "failed"
    assert result["_ncats_props_failed"] == props


def test_ncats_blank_200_response_is_no_values_not_failed():
    result = enrich_ncats_resolver(
        "aspirin", session=_NCATSSession(blank=True), props=["cid", "chembl"],
    )

    assert result["_ncats_status"] == "no_values"
    assert result["_ncats_props_failed"] == []
    assert result["_ncats_props_without_values"] == ["cid", "chembl"]


def test_ncats_explicit_empty_selection_does_not_enable_defaults():
    session = _NCATSSession()
    result = enrich_ncats_resolver("aspirin", session=session, props=[])

    assert result["_ncats_status"] == "skipped_no_properties"
    assert result["_ncats_props_requested"] == []
    assert session.groups == []


def test_ncats_metadata_only_result_tries_next_lookup_and_is_not_found(monkeypatch):
    data = _data({"drug_id": "IFXDrug:LOCAL", "standard_name": "aspirin", "synonyms": "acetylsalicylic acid"})
    calls = []

    def fake_ncats(lookup, **kwargs):
        calls.append(lookup)
        if len(calls) == 1:
            return {
                "_ncats_status": "no_values", "_ncats_props_requested": ["cid"],
                "_ncats_props_returned": [], "_ncats_props_failed": [],
                "_ncats_props_without_values": ["cid"],
            }
        return {
            "_ncats_status": "complete", "_ncats_props_requested": ["cid"],
            "_ncats_props_returned": ["cid"], "_ncats_props_failed": [],
            "_ncats_props_without_values": [], "ncats_cid": "2244",
        }

    monkeypatch.setattr("src.qa_browser.drug_resolver.enrich_ncats_resolver", fake_ncats)
    result = resolve_and_enrich(
        data, ["acetylsalicylic acid"], enable_ncats=True, ncats_props=["cid"],
        enable_pubchem=False, enable_pharos=False, enable_inxight=False,
        enable_openfda=False, enable_chebi=False,
    )["results"][0]

    assert len(calls) >= 2
    assert result["enrichment"]["ncats_resolver"]["ncats_cid"] == "2244"
    assert result["enrichment"]["sources_found"] == ["ncats_resolver"]


def test_ncats_failed_lookup_circuit_breaks_candidate_variants(monkeypatch):
    calls = []

    def fake_ncats(lookup, **kwargs):
        calls.append(lookup)
        return {
            "_ncats_status": "failed",
            "_ncats_props_requested": ["cid"],
            "_ncats_props_returned": [],
            "_ncats_props_failed": ["cid"],
            "_ncats_props_without_values": [],
            "_ncats_identity_consistency": "not_verified_request_failed",
        }

    monkeypatch.setattr("src.qa_browser.drug_resolver.enrich_ncats_resolver", fake_ncats)
    resolve_and_enrich(
        DrugGraphData(),
        ["aspirin hydrochloride tablet"],
        enable_ncats=True,
        enable_pubchem=False,
        enable_pharos=False,
        enable_inxight=False,
        enable_openfda=False,
        enable_chebi=False,
        ncats_props=["cid"],
    )

    assert calls == ["aspirin hydrochloride tablet"]


def test_ncats_explicit_empty_selection_is_skipped_not_queried_or_found():
    result = resolve_and_enrich(
        DrugGraphData(), ["aspirin"], enable_ncats=True, ncats_props=[],
        enable_pubchem=False, enable_pharos=False, enable_inxight=False,
        enable_openfda=False, enable_chebi=False,
    )["results"][0]
    enrichment = result["enrichment"]

    assert enrichment["ncats_resolver"]["_ncats_status"] == "skipped_no_properties"
    assert "ncats_resolver" not in enrichment["sources_queried"]
    assert "ncats_resolver" not in enrichment["sources_found"]
    assert enrichment["sources_skipped"]["ncats_resolver"] == "no_properties_selected"


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

    assert result["resolved"] is False
    assert result["local_match_status"] == "ambiguous_multiple_matches"
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


def test_external_resolver_contract_validates_checksum_and_drives_runtime_scope(tmp_path):
    import hashlib

    index = tmp_path / "drug_resolver_index.sqlite"
    connection = sqlite3.connect(index)
    connection.executescript("""
        CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE nodes (drug_id TEXT PRIMARY KEY, payload TEXT NOT NULL);
        CREATE TABLE aliases (alias_norm TEXT NOT NULL, drug_id TEXT NOT NULL, field TEXT NOT NULL, matched_value TEXT NOT NULL);
    """)
    connection.executemany("INSERT INTO metadata VALUES (?, ?)", [
        ("schema_version", "1"),
        ("harmonizer_version", "1.4.0"),
        ("node_count", "1304056"),
        ("scope", "all_harmonized_drug_nodes"),
    ])
    connection.commit()
    connection.close()
    digest = hashlib.sha256(index.read_bytes()).hexdigest()
    manifest = {
        "version": "v1.4.0",
        "counts": {"full_nodes_available": 1304056},
        "external_artifacts": {"resolver_index": {
            "harmonizer_version": "1.4.0",
            "schema_version": "1",
            "nodes": 1304056,
            "scope": "all_harmonized_drug_nodes",
            "bytes": index.stat().st_size,
            "sha256": digest,
        }},
    }

    metadata = _validate_drug_resolver_index(index, manifest, verify_checksum=True)
    data = DrugGraphData()
    data.manifest = manifest
    data.resolver_index_path = index
    data.resolver_index_metadata = metadata
    data.resolver_checksum_verified = True
    stats = compute_drug_stats(data)

    assert metadata["harmonizer_version"] == "1.4.0"
    assert stats["resolver_index_available"] is True
    assert stats["resolver_index_nodes"] == 1304056
    assert stats["local_lookup_scope"] == "complete_identity_lookup"
    assert stats["resolver_checksum_verified"] is True


def test_drug_bundle_loader_rejects_manifest_count_mismatch(tmp_path):
    (tmp_path / "manifest.json").write_text(json.dumps({
        "counts": {"nodes": 2, "edges": 0},
    }))
    (tmp_path / "drug_nodes.tsv").write_text("drug_id\tstandard_name\nIFXDrug:1\tOne\n")
    (tmp_path / "drug_edges.tsv").write_text("source_id\ttarget_id\n")

    with pytest.raises(Exception, match="node count mismatch"):
        load_drug_graph_data(tmp_path)


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
    assert result["resolved_via"] == "pubchem exact identity bridge→IFXDrug:7WGWKYR"
    assert result["initial_local_status"] == "no_match"
    assert result["bridge_relookup"]["status"] == "reconciled_exact_unique"
    assert payload["stats"]["local_resolved_initial"] == 0
    assert payload["stats"]["bridge_resolved"] == 1
    assert payload["stats"]["local_resolved_final"] == 1


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


def test_pubchem_returned_name_is_candidate_only(monkeypatch):
    data = _data({
        "drug_id": "IFXDrug:NAME_ONLY",
        "standard_name": "PubChem preferred title",
        "source_namespaces": "PubChem|GSRS",
    })
    monkeypatch.setattr(
        "src.qa_browser.drug_resolver.enrich_pubchem",
        lambda *args, **kwargs: {
            "pubchem_cid": "999",
            "pubchem_title": "PubChem preferred title",
            "candidates": [{
                "pubchem_cid": "999",
                "pubchem_title": "PubChem preferred title",
                "pubchem_inchikey": "AAAAAAAAAAAAAA-UHFFFAOYSA-N",
            }],
        },
    )

    result = resolve_and_enrich(
        data,
        ["unmatched name"],
        enable_ncats=False,
        enable_pubchem=True,
        enable_pharos=False,
        enable_inxight=False,
        enable_openfda=False,
        enable_chebi=False,
    )["results"][0]

    assert result["resolved"] is False
    assert result["local_hits"] == []
    assert result["bridge_relookup"]["status"] == "candidate_only_name"
    assert result["bridge_relookup"]["name_candidates"][0]["local_drug_ids"] == ["IFXDrug:NAME_ONLY"]


def test_pubchem_exact_structure_bridge_uses_returned_smiles(monkeypatch):
    data = _data({
        "drug_id": "IFXDrug:STRUCTURE",
        "standard_name": "structure match",
        "smiles": "CC(=O)Oc1ccccc1C(=O)O",
    })
    monkeypatch.setattr(
        "src.qa_browser.drug_resolver.enrich_pubchem",
        lambda *args, **kwargs: {
            "pubchem_cid": "2244",
            "pubchem_inchikey": "BSYNRYMUTXBXSQ-UHFFFAOYSA-N",
            "pubchem_smiles": "O=C(O)c1ccccc1OC(C)=O",
            "candidates": [{
                "pubchem_cid": "2244",
                "pubchem_inchikey": "BSYNRYMUTXBXSQ-UHFFFAOYSA-N",
                "pubchem_smiles": "O=C(O)c1ccccc1OC(C)=O",
            }],
        },
    )

    result = resolve_and_enrich(
        data,
        ["PUBCHEM.COMPOUND:2244"],
        enable_ncats=False,
        enable_pubchem=True,
        enable_pharos=False,
        enable_inxight=False,
        enable_openfda=False,
        enable_chebi=False,
    )["results"][0]

    assert result["resolved"] is True
    assert result["local_hits"][0]["drug_id"] == "IFXDrug:STRUCTURE"
    assert result["bridge_relookup"]["status"] == "reconciled_exact_unique"


def test_ncats_exact_identifier_bridge_rechecks_local_harmonizer(monkeypatch):
    data = _data({
        "drug_id": "IFXDrug:NCATS_BRIDGE",
        "standard_name": "NCATS bridge match",
        "unii": "ABC123DEF4",
    })

    def fake_ncats(lookup, **kwargs):
        return {
            "_ncats_status": "complete",
            "_ncats_props_requested": ["chembl", "unii"],
            "_ncats_props_returned": ["chembl", "unii"],
            "_ncats_props_failed": [],
            "_ncats_props_without_values": [],
            "_ncats_identity_consistency": "not_applicable_single_batch",
            "ncats_chembl": "CHEMBL999999",
            "ncats_unii": "ABC123DEF4",
        }

    monkeypatch.setattr("src.qa_browser.drug_resolver.enrich_ncats_resolver", fake_ncats)
    result = resolve_and_enrich(
        data,
        ["CHEMBL.COMPOUND:CHEMBL999999"],
        enable_ncats=True,
        enable_pubchem=False,
        enable_pharos=False,
        enable_inxight=False,
        enable_openfda=False,
        enable_chebi=False,
        ncats_props=["unii"],
    )["results"][0]

    assert result["resolved"] is True
    assert result["local_hits"][0]["drug_id"] == "IFXDrug:NCATS_BRIDGE"
    assert result["bridge_relookup"]["status"] == "reconciled_exact_unique"
    assert result["bridge_relookup"]["evidence"][0]["source"] == "ncats_resolver"


def test_external_identifier_conflict_is_not_auto_resolved():
    data = _data(
        {
            "drug_id": "IFXDrug:CID_MATCH",
            "standard_name": "CID match",
            "pubchem_cid": "12345",
        },
        {
            "drug_id": "IFXDrug:UNII_MATCH",
            "standard_name": "UNII match",
            "unii": "ABC123DEF4",
        },
    )
    local_result = {
        "query": "external identifier",
        "local_hits": [],
        "resolved": False,
        "total_local_matches": 0,
        "local_match_status": "no_match",
        "initial_local_status": "no_match",
    }

    result = _reconcile_live_identity(
        data,
        local_result,
        [
            {"source": "pubchem", "namespace": "pubchem_cid", "value": "12345", "lookup": "PUBCHEM.COMPOUND:12345"},
            {"source": "ncats_resolver", "namespace": "unii", "value": "ABC123DEF4", "lookup": "UNII:ABC123DEF4"},
        ],
        [],
    )

    assert result["resolved"] is False
    assert result["bridge_relookup"]["status"] == "identifier_conflict"
    assert result["bridge_relookup"]["matched_drug_ids"] == ["IFXDrug:CID_MATCH", "IFXDrug:UNII_MATCH"]
    assert {row["standard_name"] for row in result["bridge_relookup"]["matched_candidates"]} == {
        "CID match", "UNII match",
    }


@pytest.mark.parametrize(
    ("namespace", "lookup", "name_collision"),
    [
        ("unii", "UNII:ABC123DEF4", "UNII:ABC123DEF4"),
        ("smiles", "NO", "NO"),
    ],
)
def test_external_identity_does_not_resolve_name_collision(namespace, lookup, name_collision):
    data = _data({
        "drug_id": "IFXDrug:NAME_COLLISION",
        "standard_name": name_collision,
    })
    result = _reconcile_live_identity(
        data,
        {
            "query": lookup,
            "local_hits": [],
            "resolved": False,
            "total_local_matches": 0,
            "local_match_status": "no_match",
            "initial_local_status": "no_match",
        },
        [{"source": "ncats_resolver", "namespace": namespace, "value": lookup, "lookup": lookup}],
        [],
    )

    assert result["resolved"] is False
    assert result["bridge_relookup"]["matched_drug_ids"] == []


@pytest.mark.parametrize(
    ("namespace", "lookup", "wrong_xref"),
    [
        ("pubchem_cid", "PUBCHEM.COMPOUND:12345", "CHEBI:12345"),
        ("pubchem_cid", "PUBCHEM.COMPOUND:12345", "RXCUI:12345"),
        ("chebi", "CHEBI:12345", "PUBCHEM.COMPOUND:12345"),
        ("chebi", "CHEBI:12345", "DrugCentral:12345"),
    ],
)
def test_external_identity_preserves_numeric_identifier_namespace(namespace, lookup, wrong_xref):
    data = _data({
        "drug_id": "IFXDrug:WRONG_NAMESPACE",
        "standard_name": "numeric namespace collision",
        "xrefs": wrong_xref,
    })
    result = _reconcile_live_identity(
        data,
        {
            "query": lookup,
            "local_hits": [],
            "resolved": False,
            "total_local_matches": 0,
            "local_match_status": "no_match",
            "initial_local_status": "no_match",
        },
        [{"source": "pubchem", "namespace": namespace, "value": lookup, "lookup": lookup}],
        [],
    )

    assert result["resolved"] is False
    assert result["bridge_relookup"]["matched_drug_ids"] == []


def test_ncats_stereochemical_identity_mismatch_never_bridges(monkeypatch):
    data = _data({
        "drug_id": "IFXDrug:WRONG_STEREO",
        "standard_name": "wrong stereoisomer",
        "inchikey": "XGDFITZJGKUSDK-UDYGKFQRSA-N",
        "unii": "BY7Y2JX7NQ",
        "pubchem_cid": "11957481",
    })

    def fake_ncats(lookup, **kwargs):
        return {
            "_ncats_status": "complete",
            "_ncats_props_requested": ["cid", "unii", "inchikey"],
            "_ncats_props_returned": ["cid", "unii", "inchikey"],
            "_ncats_props_failed": [],
            "_ncats_props_without_values": [],
            "_ncats_identity_consistency": "not_applicable_single_batch",
            "ncats_cid": "11957481",
            "ncats_unii": "BY7Y2JX7NQ",
            "ncats_inchikey": "XGDFITZJGKUSDK-UDYGKFQRSA-N",
        }

    monkeypatch.setattr("src.qa_browser.drug_resolver.enrich_ncats_resolver", fake_ncats)
    result = resolve_and_enrich(
        data,
        ["XGDFITZJGKUSDK-NNNATCHMSA-N"],
        enable_ncats=True,
        enable_pubchem=False,
        enable_pharos=False,
        enable_inxight=False,
        enable_openfda=False,
        enable_chebi=False,
    )["results"][0]

    assert result["resolved"] is False
    assert result["bridge_relookup"]["status"] == "provider_identity_mismatch"
    assert result["bridge_relookup"]["matched_drug_ids"] == []


def test_ncats_identity_projection_is_independent_of_selected_annotations(monkeypatch):
    data = _data({
        "drug_id": "IFXDrug:IDENTITY_PROJECTION",
        "standard_name": "identity projection match",
        "unii": "ABC123DEF4",
    })
    calls = []

    def fake_ncats(lookup, **kwargs):
        props = kwargs.get("props") or []
        calls.append(list(props))
        if props == ["description"]:
            return {
                "_ncats_status": "complete",
                "_ncats_props_requested": props,
                "_ncats_props_returned": props,
                "_ncats_props_failed": [],
                "_ncats_props_without_values": [],
                "_ncats_identity_consistency": "not_applicable_single_batch",
                "ncats_description": "annotation only",
            }
        return {
            "_ncats_status": "complete",
            "_ncats_props_requested": props,
            "_ncats_props_returned": ["chembl", "unii"],
            "_ncats_props_failed": [],
            "_ncats_props_without_values": [],
            "_ncats_identity_consistency": "not_applicable_single_batch",
            "ncats_chembl": "CHEMBL999999",
            "ncats_unii": "ABC123DEF4",
        }

    monkeypatch.setattr("src.qa_browser.drug_resolver.enrich_ncats_resolver", fake_ncats)
    payload = resolve_and_enrich(
        data,
        ["CHEMBL999999"],
        enable_ncats=True,
        enable_pubchem=False,
        enable_pharos=False,
        enable_inxight=False,
        enable_openfda=False,
        enable_chebi=False,
        ncats_props=["description"],
    )
    result = payload["results"][0]

    assert ["description"] in calls
    assert ["cid", "chembl", "chebi", "unii", "smiles", "inchikey"] in calls
    assert result["resolved"] is True
    assert result["local_hits"][0]["drug_id"] == "IFXDrug:IDENTITY_PROJECTION"
    assert "ncats_resolver" in result["enrichment"]["sources_queried"]
    assert "ncats_resolver" in result["enrichment"]["sources_found"]


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
