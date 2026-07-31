from pathlib import Path
import gzip
import json
import zipfile

from src.input_adapters.metabolite_harmonization.chemprops import (
    ChebiMetaboliteChemPropsAdapter,
    HmdbMetaboliteChemPropsAdapter,
    LipidMapsMetaboliteChemPropsAdapter,
    PubchemMetaboliteChemPropsAdapter,
)
from src.interfaces.output_adapter import OutputAdapter
from src.models.metabolite_harmonization import MetaboliteIdentifier
from src.shared.record_merger import FieldConflictBehavior


class _ConvertingOutputAdapter(OutputAdapter):
    def store(self, objects, single_source=False, field_conflict_behavior=FieldConflictBehavior.KeepFirst) -> bool:
        return True

    def create_or_truncate_datastore(self, truncate_tables: bool = None) -> bool:
        return True


def _sdf_record(title: str, tags: dict) -> str:
    lines = [
        title,
        "  TEST",
        "",
        "  0  0  0     0  0            999 V2000",
        "M  END",
        "",
    ]
    for key, value in tags.items():
        lines.extend([f"> <{key}>", value, ""])
    lines.append("$$$$")
    return "\n".join(lines) + "\n"


def _records(adapter):
    return [record for batch in adapter.get_all() for record in batch]


def test_hmdb_chemprops_adapter_emits_chemprops(tmp_path: Path):
    zip_path = tmp_path / "structures.zip"
    sdf = _sdf_record(
        "HMDB0000001",
        {
            "DATABASE_ID": "HMDB0000001",
            "SMILES": "C[NH+]1C=NC=C1CC(N)C(=O)O",
            "INCHI_KEY": "BRMWTNUJHUMWMS-LURJTMIESA-N",
            "INCHI_IDENTIFIER": "InChI=1S/C7H11N3O2",
            "MOLECULAR_WEIGHT": "169.1811",
            "EXACT_MASS": "169.085126611",
            "GENERIC_NAME": "1-Methylhistidine",
            "FORMULA": "C7H11N3O2",
        },
    )
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("structures.sdf", sdf)

    node = _records(HmdbMetaboliteChemPropsAdapter(structures_zip_file=str(zip_path)))[0]

    assert node.id == "HMDB:HMDB0000001"
    assert node.prefix == "HMDB"
    assert node.chem_props[0].source == "HMDB"
    assert node.chem_props[0].source_id == "HMDB:HMDB0000001"
    assert node.chem_props[0].iso_smiles == "C[NH+]1C=NC=C1CC(N)C(=O)O"
    assert node.chem_props[0].inchi_key_prefix == "BRMWTNUJHUMWMS"
    assert node.chem_props[0].monoisotopic_mass == "169.085126611"
    assert node.chem_props[0].common_name == "1-Methylhistidine"
    assert node.chem_props[0].molecular_formula == "C7H11N3O2"


def test_chebi_chemprops_adapter_reads_gzipped_sdf(tmp_path: Path):
    gz_path = tmp_path / "chebi_3_stars.sdf.gz"
    sdf = _sdf_record(
        "CHEBI:27596",
        {
            "ChEBI ID": "CHEBI:27596",
            "SMILES": "Cn1cncc1C[C@H](N)C(=O)O",
            "InChIKey": "BRMWTNUJHUMWMS-LURJTMIESA-N",
            "InChI": "InChI=1S/C7H11N3O2",
            "MASS": "169.184",
            "Monoisotopic Mass": "169.085126611",
            "ChEBI Name": "1-methyl-L-histidine",
            "Formulae": "C7H11N3O2",
        },
    )
    with gzip.open(gz_path, "wt", encoding="utf-8") as handle:
        handle.write(sdf)

    node = _records(ChebiMetaboliteChemPropsAdapter(chebi_sdf_file=str(gz_path)))[0]

    assert node.id == "CHEBI:27596"
    assert node.chem_props[0].source == "ChEBI"
    assert node.chem_props[0].mw == "169.184"
    assert node.chem_props[0].common_name == "1-methyl-L-histidine"
    assert node.chem_props[0].inchi_key == "BRMWTNUJHUMWMS-LURJTMIESA-N"


def test_lipidmaps_chemprops_adapter_emits_chemprops(tmp_path: Path):
    zip_path = tmp_path / "LMSD.sdf.zip"
    sdf = _sdf_record(
        "LMFA00000001",
        {
            "LM_ID": "LMFA00000001",
            "SMILES": "CCC(=O)C(=O)O",
            "INCHI_KEY": "TYEYBOSBBBHJIV-UHFFFAOYSA-N",
            "INCHI": "InChI=1S/C4H6O3",
            "MASS": "102.09",
            "EXACT_MASS": "102.031695",
            "COMMON_NAME": "3-methyl pyruvic acid",
            "FORMULA": "C4H6O3",
        },
    )
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("structures.sdf", sdf)

    node = _records(LipidMapsMetaboliteChemPropsAdapter(sdf_zip_file=str(zip_path)))[0]

    assert node.id == "LIPIDMAPS:LMFA00000001"
    assert node.chem_props[0].source == "LipidMaps"
    assert node.chem_props[0].mw == "102.09"
    assert node.chem_props[0].monoisotopic_mass == "102.031695"
    assert node.chem_props[0].common_name == "3-methyl pyruvic acid"


def test_pubchem_chemprops_adapter_emits_chemprops(tmp_path: Path):
    tsv_path = tmp_path / "cid_molecular_info.tsv"
    tsv_path.write_text(
        "pubchem_id\tcid\tmonoisotopic_mass\tinchikey\tinchi_key_prefix\tmolecular_formula\tmolecular_weight\tcanonical_smiles\tisomeric_smiles\tinchi\tiupac_name\n"
        "PUBCHEM.COMPOUND:92105\t92105\t169.085126602\tBRMWTNUJHUMWMS-LURJTMIESA-N\tBRMWTNUJHUMWMS\tC7H11N3O2\t169.18\tCN1C=NC=C1CC(N)C(=O)O\tCN1C=NC=C1C[C@H](N)C(=O)O\tInChI=1S/C7H11N3O2\t2-amino-3-(1-methylimidazol-4-yl)propanoic acid\n",
        encoding="utf-8",
    )

    node = _records(PubchemMetaboliteChemPropsAdapter(molecular_info_file=str(tsv_path)))[0]

    assert node.id == "PUBCHEM.COMPOUND:92105"
    assert node.chem_props[0].source == "PubChem"
    assert node.chem_props[0].source_id == "PUBCHEM.COMPOUND:92105"
    assert node.chem_props[0].iso_smiles == "CN1C=NC=C1C[C@H](N)C(=O)O"
    assert node.chem_props[0].canonical_smiles == "CN1C=NC=C1CC(N)C(=O)O"
    assert node.chem_props[0].isomeric_smiles == "CN1C=NC=C1C[C@H](N)C(=O)O"
    assert node.chem_props[0].mw == "169.18"
    assert node.chem_props[0].iupac_name == "2-amino-3-(1-methylimidazol-4-yl)propanoic acid"


def test_chemprops_are_json_serializable_after_output_conversion(tmp_path: Path):
    tsv_path = tmp_path / "cid_molecular_info.tsv"
    tsv_path.write_text(
        "pubchem_id\tcid\tmonoisotopic_mass\tinchikey\tinchi_key_prefix\tmolecular_formula\tmolecular_weight\tcanonical_smiles\tisomeric_smiles\tinchi\tiupac_name\n"
        "PUBCHEM.COMPOUND:92105\t92105\t169.085126602\tBRMWTNUJHUMWMS-LURJTMIESA-N\tBRMWTNUJHUMWMS\tC7H11N3O2\t169.18\tcanonical\tisomeric\tInChI=1S/C7H11N3O2\tname\n",
        encoding="utf-8",
    )
    adapter = PubchemMetaboliteChemPropsAdapter(molecular_info_file=str(tsv_path))
    output = _ConvertingOutputAdapter()

    converted_groups = output.sort_and_convert_objects(_records(adapter))
    converted_records = [
        record
        for group in converted_groups.values()
        for record in group[0]
    ]

    json.dumps(converted_records)
    node = next(record for record in converted_records if record["id"] == "PUBCHEM.COMPOUND:92105")
    assert node["chem_props"] == [
        {
            "source": "PubChem",
            "source_id": "PUBCHEM.COMPOUND:92105",
            "iso_smiles": "isomeric",
            "canonical_smiles": "canonical",
            "isomeric_smiles": "isomeric",
            "inchi_key_prefix": "BRMWTNUJHUMWMS",
            "inchi_key": "BRMWTNUJHUMWMS-LURJTMIESA-N",
            "inchi": "InChI=1S/C7H11N3O2",
            "mw": "169.18",
            "monoisotopic_mass": "169.085126602",
            "common_name": None,
            "iupac_name": "name",
            "molecular_formula": "C7H11N3O2",
        }
    ]


def test_chemprops_adapters_honor_max_records(tmp_path: Path):
    tsv_path = tmp_path / "cid_molecular_info.tsv"
    tsv_path.write_text(
        "pubchem_id\tcid\tmonoisotopic_mass\tinchikey\tinchi_key_prefix\tmolecular_formula\tmolecular_weight\tcanonical_smiles\tisomeric_smiles\tinchi\tiupac_name\n"
        "PUBCHEM.COMPOUND:1\t1\t1\tA-B-C\tA\tH\t1\tC\tC\tInChI=1\tone\n"
        "PUBCHEM.COMPOUND:2\t2\t2\tD-E-F\tD\tH2\t2\tCC\tCC\tInChI=2\ttwo\n",
        encoding="utf-8",
    )

    records = _records(PubchemMetaboliteChemPropsAdapter(molecular_info_file=str(tsv_path), max_records=1))

    assert [record.id for record in records] == ["PUBCHEM.COMPOUND:1"]
