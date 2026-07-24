import csv
import gzip
from pathlib import Path

import yaml

from src.registry.derived.chebi import ChebiEndogenousHumanMetabolitesBuilder
from src.registry.fetchers import ResolvedDependency


def _write_chebi_obo(path: Path):
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write(
            "format-version: 1.2\n"
            "data-version: 252\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:100\n"
            "name: retained human metabolite\n"
            "is_a: CHEBI:24431 ! chemical entity\n"
            "def: \"A retained endogenous human metabolite.\" [ChEBI]\n"
            "synonym: \"retained synonym\" EXACT []\n"
            "xref: HMDB:HMDB0000100\n"
            "relationship: RO:0000087 CHEBI:77746 ! has role human metabolite\n"
            "property_value: chemrof:generalized_empirical_formula \"C6H12O6\" xsd:string\n"
            "property_value: chemrof:monoisotopic_mass \"180.063388\" xsd:string\n"
            "property_value: chemrof:smiles_string \"C(C1C(C(C(C(O1)O)O)O)O)O\" xsd:string\n"
            "property_value: chemrof:inchi_key_string \"WQZGKKKJIJFFOK-GASJEMHNSA-N\" xsd:string\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:101\n"
            "name: retained descendant-role metabolite\n"
            "is_a: CHEBI:24431 ! chemical entity\n"
            "relationship: RO:0000087 CHEBI:900001 ! has role specific human metabolite\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:102\n"
            "name: drug xref human metabolite\n"
            "is_a: CHEBI:24431 ! chemical entity\n"
            "xref: DrugBank:DB00001\n"
            "relationship: RO:0000087 CHEBI:77746 ! has role human metabolite\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:103\n"
            "name: antimicrobial-role human metabolite\n"
            "is_a: CHEBI:24431 ! chemical entity\n"
            "relationship: RO:0000087 CHEBI:77746 ! has role human metabolite\n"
            "relationship: RO:0000087 CHEBI:33281 ! has role antimicrobial agent\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:104\n"
            "name: synthetic human metabolite\n"
            "is_a: CHEBI:24431 ! chemical entity\n"
            "relationship: RO:0000087 CHEBI:77746 ! has role human metabolite\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:105\n"
            "name: obsolete human metabolite\n"
            "is_a: CHEBI:24431 ! chemical entity\n"
            "is_obsolete: true\n"
            "relationship: RO:0000087 CHEBI:77746 ! has role human metabolite\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:106\n"
            "name: retained broad chemical entity\n"
            "is_a: CHEBI:24431 ! chemical entity\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:107\n"
            "name: unrelated non-chemical term\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:24431\n"
            "name: chemical entity\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:900001\n"
            "name: specific human metabolite\n"
            "is_a: CHEBI:77746 ! human metabolite\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:77746\n"
            "name: human metabolite\n"
            "is_a: CHEBI:25212 ! metabolite\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:25212\n"
            "name: metabolite\n"
            "\n"
            "[Term]\n"
            "id: CHEBI:33281\n"
            "name: antimicrobial agent\n"
        )


def _dependency(path: Path) -> ResolvedDependency:
    return ResolvedDependency(
        source="chebi",
        dataset="ontology_full",
        version="252",
        snapshot_id="chebi:ontology_full:252",
        manifest_uri="s3://ifx-registry/sources/chebi/ontology_full/252/manifest.yaml",
        manifest={"version_date": "2026-05-01", "files": [{"path": path.name}]},
        local_dir=path.parent,
    )


def test_chebi_endogenous_human_metabolites_builder_outputs_retained_terms(tmp_path: Path):
    source_path = tmp_path / "chebi.obo.gz"
    _write_chebi_obo(source_path)

    artifact = ChebiEndogenousHumanMetabolitesBuilder().build(
        config={
            "output": {
                "file_name": "chebi_endogenous_human_metabolites.tsv",
                "full_file_name": "chebi_full.tsv",
                "data_dictionary_file_name": "chebi_endogenous_human_metabolites_data_dictionary.tsv",
            },
            "transform": {"name": "chebi_endogenous_human_metabolites", "version": 1},
        },
        dependencies=[_dependency(source_path)],
        dest=tmp_path / "derived",
        version="252",
    )

    assert artifact.source == "chebi"
    assert artifact.dataset == "endogenous_human_metabolites"
    assert artifact.version == "252"
    assert artifact.version_date == "2026-05-01"
    assert artifact.derived_from == [
        {
            "snapshot_id": "chebi:ontology_full:252",
            "manifest_uri": "s3://ifx-registry/sources/chebi/ontology_full/252/manifest.yaml",
        }
    ]
    assert artifact.stats == {
        "term_count": 13,
        "row_count": 4,
        "full_row_count": 13,
        "chemical_entity_descendant_count": 8,
        "endogenous_human_metabolite_count": 4,
        "has_human_metabolite_role_count": 6,
        "obsolete_count": 1,
        "drug_xref_count": 1,
        "exogenous_descendant_count": 2,
        "forbidden_label_text_count": 4,
    }

    assert [artifact_file.path.name for artifact_file in artifact.files] == [
        "chebi_endogenous_human_metabolites.tsv",
        "chebi_full.tsv",
        "chebi_endogenous_human_metabolites_data_dictionary.tsv",
    ]

    output_path = artifact.files[0].path
    with output_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle, delimiter="\t"))

    assert "human_metabolite_role_ids" not in rows[0]
    assert "exclusion_reasons" not in rows[0]
    assert [row["chebi_id"] for row in rows] == [
        "CHEBI:100",
        "CHEBI:101",
        "CHEBI:106",
        "CHEBI:24431",
    ]
    retained = rows[0]
    assert retained["name"] == "retained human metabolite"
    assert retained["definition"] == "A retained endogenous human metabolite."
    assert retained["synonyms"] == "retained synonym"
    assert retained["xrefs"] == "HMDB:HMDB0000100"
    assert retained["formula"] == "C6H12O6"
    assert retained["monoisotopic_mass"] == "180.063388"
    assert retained["inchi_key"] == "WQZGKKKJIJFFOK-GASJEMHNSA-N"
    assert retained["is_chemical_entity_descendant"] == "true"
    assert retained["has_human_metabolite_role"] == "true"
    assert retained["is_obsolete"] == "false"
    assert rows[1]["is_chemical_entity_descendant"] == "true"
    assert rows[1]["has_human_metabolite_role"] == "true"

    with artifact.files[1].path.open("r", encoding="utf-8", newline="") as handle:
        full_rows = list(csv.DictReader(handle, delimiter="\t"))
    assert set(rows[0]) == set(full_rows[0])
    assert [row["chebi_id"] for row in full_rows] == [
        "CHEBI:100",
        "CHEBI:101",
        "CHEBI:102",
        "CHEBI:103",
        "CHEBI:104",
        "CHEBI:105",
        "CHEBI:106",
        "CHEBI:107",
        "CHEBI:24431",
        "CHEBI:25212",
        "CHEBI:33281",
        "CHEBI:77746",
        "CHEBI:900001",
    ]

    by_id = {row["chebi_id"]: row for row in full_rows}
    assert by_id["CHEBI:102"]["has_drug_xref"] == "true"
    assert by_id["CHEBI:102"]["drug_xrefs"] == "DrugBank:DB00001"
    assert by_id["CHEBI:102"]["has_forbidden_label_text"] == "true"

    assert by_id["CHEBI:103"]["is_exogenous_descendant"] == "true"
    assert by_id["CHEBI:103"]["exogenous_ancestor_ids"] == "CHEBI:33281:antimicrobial agent"
    assert by_id["CHEBI:103"]["is_pharmaceutical_descendant"] == "false"

    assert by_id["CHEBI:104"]["has_forbidden_label_text"] == "true"
    assert by_id["CHEBI:104"]["forbidden_label_terms"] == "synthetic"

    assert by_id["CHEBI:105"]["is_obsolete"] == "true"

    assert by_id["CHEBI:106"]["has_human_metabolite_role"] == "false"
    assert by_id["CHEBI:106"]["is_pharmaceutical_descendant"] == "false"
    assert by_id["CHEBI:107"]["is_chemical_entity_descendant"] == "false"
    assert by_id["CHEBI:107"]["has_human_metabolite_role"] == "false"

    with artifact.files[2].path.open("r", encoding="utf-8", newline="") as handle:
        dictionary_rows = list(csv.DictReader(handle, delimiter="\t"))
    dictionary = {row["column"]: row for row in dictionary_rows}
    assert dictionary["is_exogenous_descendant"]["transform"].startswith("true when the term")
    assert dictionary["drug_xrefs"]["type"] == "pipe-delimited string"
    forbidden_transform = dictionary["has_forbidden_label_text"]["transform"]
    assert "Forbidden terms:" in forbidden_transform
    assert "drug, pharmaceutical, xenobiotic" in forbidden_transform
    assert "food additive" in forbidden_transform
    assert set(rows[0]) == set(dictionary)


def test_registry_sources_config_defines_chebi_endogenous_human_metabolites():
    config = yaml.safe_load(Path("src/registry/registry_sources.yaml").read_text(encoding="utf-8"))

    dataset = config["sources"]["chebi"]["datasets"]["endogenous_human_metabolites"]

    assert "fetch" not in dataset
    assert dataset["derived"]["module"] == "src.registry.derived.chebi"
    assert dataset["derived"]["class"] == "ChebiEndogenousHumanMetabolitesBuilder"
    assert dataset["derived"]["dependencies"] == [
        {"source": "chebi", "dataset": "ontology_full"},
    ]
    assert dataset["derived"]["output"] == {
        "file_name": "chebi_endogenous_human_metabolites.tsv",
        "full_file_name": "chebi_full.tsv",
        "data_dictionary_file_name": "chebi_endogenous_human_metabolites_data_dictionary.tsv",
    }
    assert dataset["derived"]["transform"]["name"] == "chebi_endogenous_human_metabolites"
    assert dataset["derived"]["transform"]["code_ref"] == "src/registry/derived/chebi.py"
