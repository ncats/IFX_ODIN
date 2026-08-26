"""Generate the structure panels used by the anomer harmonization design note."""

from pathlib import Path

from rdkit import Chem
from rdkit.Chem import Draw


OUTPUT_DIR = Path(__file__).parent


def free_anomeric_oh_atoms(mol: Chem.Mol) -> list[int]:
    """Find ring carbons bonded to both a ring oxygen and an external neutral OH."""
    matches: list[int] = []
    for atom in mol.GetAtoms():
        if atom.GetAtomicNum() != 6 or not atom.IsInRing():
            continue
        has_ring_oxygen = any(
            neighbor.GetAtomicNum() == 8 and neighbor.IsInRing()
            for neighbor in atom.GetNeighbors()
        )
        has_external_oh = any(
            neighbor.GetAtomicNum() == 8
            and not neighbor.IsInRing()
            and neighbor.GetDegree() == 1
            and neighbor.GetFormalCharge() == 0
            and neighbor.GetTotalNumHs() > 0
            for neighbor in atom.GetNeighbors()
        )
        if has_ring_oxygen and has_external_oh:
            matches.append(atom.GetIdx())
    return matches


def write_panel(filename: str, records: list[tuple[str, str]], columns: int = 3) -> None:
    mols = []
    highlights = []
    legends = []
    for legend, smiles in records:
        mol = Chem.MolFromSmiles(smiles)
        if mol is None:
            raise ValueError(f"Could not parse {legend}: {smiles}")
        Chem.rdDepictor.Compute2DCoords(mol)
        mols.append(mol)
        highlights.append(free_anomeric_oh_atoms(mol))
        legends.append(legend)

    svg = Draw.MolsToGridImage(
        mols,
        molsPerRow=columns,
        subImgSize=(320, 260),
        legends=legends,
        highlightAtomLists=highlights,
        useSVG=True,
    )
    (OUTPUT_DIR / filename).write_text(str(svg), encoding="utf-8")


def main() -> None:
    write_panel(
        "included_glucose.svg",
        [
            ("alpha-D-glucose\nCHEBI:17925", "OC[C@H]1O[C@H](O)[C@H](O)[C@@H](O)[C@@H]1O"),
            ("beta-D-glucose\nCHEBI:15903", "OC[C@H]1O[C@@H](O)[C@H](O)[C@@H](O)[C@@H]1O"),
            ("D-glucopyranose\nCHEBI:4167", "OC[C@H]1OC(O)[C@H](O)[C@@H](O)[C@@H]1O"),
        ],
    )
    write_panel(
        "excluded_epimers.svg",
        [
            ("D-glucopyranose\nCHEBI:4167", "OC[C@H]1OC(O)[C@H](O)[C@@H](O)[C@@H]1O"),
            ("D-mannopyranose\nCHEBI:4208", "OC[C@H]1OC(O)[C@@H](O)[C@@H](O)[C@@H]1O"),
            ("D-galactopyranose\nCHEBI:4139", "OC[C@H]1OC(O)[C@H](O)[C@@H](O)[C@H]1O"),
        ],
    )
    write_panel(
        "included_modified_sugars.svg",
        [
            ("alpha-D-glucose 6-phosphate\nCHEBI:17665", "O=P(O)(O)OC[C@H]1O[C@H](O)[C@H](O)[C@@H](O)[C@@H]1O"),
            ("beta-D-glucose 6-phosphate\nCHEBI:17719", "O=P(O)(O)OC[C@H]1O[C@@H](O)[C@H](O)[C@@H](O)[C@@H]1O"),
            ("D-glucopyranose 6-phosphate\nCHEBI:4170", "O=P(O)(O)OC[C@H]1OC(O)[C@H](O)[C@@H](O)[C@@H]1O"),
            ("N-acetyl-alpha-D-glucosamine\nCHEBI:44278", "CC(=O)N[C@@H]1[C@@H](O)[C@H](O)[C@@H](CO)O[C@@H]1O"),
            ("N-acetyl-beta-D-glucosamine\nCHEBI:28009", "CC(=O)N[C@@H]1[C@@H](O)[C@H](O)[C@@H](CO)O[C@H]1O"),
            ("N-acetyl-D-glucosamine\nCHEBI:506227", "CC(=O)N[C@H]1C(O)O[C@H](CO)[C@@H](O)[C@@H]1O"),
        ],
    )
    write_panel(
        "included_reducing_oligosaccharide.svg",
        [
            ("alpha-cellotriose\nCHEBI:41727", "OC[C@H]1O[C@@H](O[C@H]2[C@H](O)[C@@H](O)[C@H](O[C@H]3[C@H](O)[C@@H](O)[C@@H](O)O[C@@H]3CO)O[C@@H]2CO)[C@H](O)[C@@H](O)[C@@H]1O"),
            ("beta-cellotriose\nCHEBI:41753", "OC[C@H]1O[C@@H](O[C@H]2[C@H](O)[C@@H](O)[C@H](O[C@H]3[C@H](O)[C@@H](O)[C@H](O)O[C@@H]3CO)O[C@@H]2CO)[C@H](O)[C@@H](O)[C@@H]1O"),
            ("cellotriose\nCHEBI:3528", "OC[C@H]1O[C@@H](O[C@H]2[C@H](O)[C@@H](O)[C@H](O[C@H]3[C@H](O)[C@@H](O)C(O)O[C@@H]3CO)O[C@@H]2CO)[C@H](O)[C@@H](O)[C@@H]1O"),
        ],
    )
    write_panel(
        "excluded_locked_glycosides.svg",
        [
            ("methyl alpha-D-glucopyranoside\nCHEBI:320061", "CO[C@H]1O[C@H](CO)[C@@H](O)[C@H](O)[C@H]1O"),
            ("methyl beta-D-glucopyranoside\nCHEBI:320055", "CO[C@@H]1O[C@H](CO)[C@@H](O)[C@H](O)[C@H]1O"),
            ("sucrose\nCHEBI:17992", "OC[C@H]1O[C@@](CO)(O[C@H]2O[C@H](CO)[C@@H](O)[C@H](O)[C@H]2O)[C@@H](O)[C@@H]1O"),
            ("alpha,alpha-trehalose\nCHEBI:16551", "OC[C@H]1O[C@H](O[C@H]2O[C@H](CO)[C@@H](O)[C@H](O)[C@H]2O)[C@H](O)[C@@H](O)[C@@H]1O"),
        ],
        columns=2,
    )
    write_panel(
        "excluded_polymers.svg",
        [
            ("(1->4)-alpha-D-glucan\nCHEBI:15444", "[H]O[C@H]1O[C@H](CO)[C@@H](O)[C@H](O)[C@H]1O"),
            ("(1->4)-beta-D-glucan\nCHEBI:18246", "[H]O[C@@H]1O[C@H](CO)[C@@H](O)[C@H](O)[C@H]1O"),
        ],
        columns=2,
    )
    write_panel(
        "deferred_noncarbohydrate.svg",
        [
            ("(2S)-2-hydroxynaringenin\nCHEBI:141994", "O=C1C[C@@](O)(c2ccc(O)cc2)Oc2cc(O)cc(O)c21"),
            ("(2R)-2-hydroxynaringenin\nCHEBI:142229", "O=C1C[C@](O)(c2ccc(O)cc2)Oc2cc(O)cc(O)c21"),
            ("2-hydroxynaringenin\nCHEBI:142230", "O=C1CC(O)(c2ccc(O)cc2)Oc2cc(O)cc(O)c21"),
        ],
    )


if __name__ == "__main__":
    main()
