# Merge Free Anomeric Forms

**Status:** Implemented as an optional Metabolite Harmonization Studio rule.

**Rule ID:** `merge_free_anomeric_forms`

**Algorithm version:** `free-anomer-v2`

## What the rule does

The rule merges ChEBI forms that differ only in the stereochemistry of one free
cyclic hemiacetal or hemiketal carbon. Most matches are reducing sugars, but
carbohydrate ancestry is not required: the same ring-opening and reclosure
chemistry can occur in other compound classes.

For example, these forms of D-glucopyranose should merge:

- `CHEBI:17925` — alpha-D-glucose
- `CHEBI:15903` — beta-D-glucose
- `CHEBI:4167` — D-glucopyranose with no specified anomer

The rule preserves every other stereocenter. It therefore does not merge
glucose with mannose or galactose, D forms with L forms, or pyranose rings with
furanose rings.

It also does not merge fixed glycosides, nonreducing sugars, or polymers. Those
structures do not have the free anomeric hydroxyl required by the rule, or are
excluded from candidate generation.

The rule adds synthetic, auditable `IFX Harmonization Rule` edges to the stage.
It does not change source structures or overwrite documented or derived
InChIKeys.

The same RDKit key-generation helper is also used by the
[carbohydrate family conflict validation](metabolite_carbohydrate_family_validation.md),
which checks complete stage cliques for incompatible carbohydrate families.

## Why these forms can be merged

A compound with a free cyclic hemiacetal or hemiketal hydroxyl can open to a
carbonyl form and close again with either configuration at that center. For
sugars this interconversion is called mutarotation. The rule treats forms that
differ only at that labile center as members of the same harmonized metabolite
group.

This reasoning does not apply when the anomeric oxygen is part of a glycosidic
acetal bond. Alpha and beta glycosides are distinct compounds and remain
separate.

## Examples

### D-glucopyranose: merge

![Alpha, beta, and anomer-unspecified D-glucopyranose](assets/metabolite_anomer_rule/included_glucose.svg)

The alpha (`CHEBI:17925`), beta (`CHEBI:15903`), and unspecified
(`CHEBI:4167`) structures differ only at the free anomeric carbon. Clearing the
chiral tag on that carbon gives all three the same full normalized InChIKey, so
the direct child–parent pairs are merged. Together, those edges put all three
identifiers in one clique.

### Modified and larger reducing sugars: merge

The same rule can merge a modified sugar as long as the modification and all
other stereochemistry remain identical. Examples include:

- alpha-, beta-, and unspecified D-glucose 6-phosphate (`CHEBI:17665`,
  `CHEBI:17719`, and `CHEBI:4170`);
- alpha-, beta-, and unspecified N-acetyl-D-glucosamine (`CHEBI:44278`,
  `CHEBI:28009`, and `CHEBI:506227`); and
- alpha-, beta-, and unspecified cellotriose (`CHEBI:41727`, `CHEBI:41753`,
  and `CHEBI:3528`).

For cellotriose, only the free anomeric center on the terminal reducing sugar
is normalized. Its internal glycosidic bonds and their stereochemistry are not
changed.

### Non-carbohydrate cyclic hemiacetals and hemiketals: merge

Carbohydrate ancestry is not a requirement. Reviewed examples include:

- beta-dihydroartemisinin (`CHEBI:135921`) and anomer-unspecified
  dihydroartemisinin (`CHEBI:207229`);
- alpha-, beta-, and unspecified ADP-D-ribose (`CHEBI:191408`,
  `CHEBI:191409`, and `CHEBI:16960`);
- alpha-, beta-, and unspecified punicalagin (`CHEBI:233620`, `CHEBI:233621`,
  and `CHEBI:167695`); and
- stereospecific and unspecified cyclic hydroxy-oxepanone and
  2-hydroxyflavanone forms.

These matches pass the same structural comparison as sugars. The rule does not
infer equivalence from a compound name or class.

### Other sugar stereochemistry: keep separate

![D-glucose, D-mannose, and D-galactose](assets/metabolite_anomer_rule/excluded_epimers.svg)

D-glucopyranose (`CHEBI:4167`), D-mannopyranose (`CHEBI:4208`), and
D-galactopyranose (`CHEBI:4139`) differ at stereocenters other than the free
anomeric carbon. Those protected differences remain after normalization, so
their full normalized InChIKeys do not match.

### Locked or nonreducing sugars: keep separate

![Methyl glucosides, sucrose, and trehalose](assets/metabolite_anomer_rule/excluded_locked_glycosides.svg)

Methyl alpha- and beta-D-glucopyranoside (`CHEBI:320061` and `CHEBI:320055`)
have a methoxy group instead of a free anomeric hydroxyl. Sucrose
(`CHEBI:17992`) and trehalose (`CHEBI:16551`) use their anomeric centers in
glycosidic bonds. The detector therefore does not find exactly one qualifying
free anomeric center, and these structures are not merged.

## Candidate pairs

The rule evaluates direct ChEBI `is_a` child–parent pairs. This normally gives
an alpha or beta ChEBI term paired with its anomer-unspecified parent. Both
ChEBI identifiers must be active metabolite identifiers in the current stage.

Carbohydrate ancestry is not required. Descendants of these classes are always
excluded:

- `CHEBI:60027` — polymer
- `CHEBI:18154` — polysaccharide

The separate carbohydrate-family conflict validation remains scoped to ChEBI
carbohydrate descendants. It checks complete cliques for incompatible
carbohydrate families; it does not limit which pairs this merge rule evaluates.

## Matching algorithm

A candidate pair is merged only when all of these checks pass:

1. Both records have the same nonempty molecular formula and charge.
2. The formula is not a repeat formula containing `n`.
3. Both records have parseable SMILES with no wildcard atoms.
4. RDKit finds exactly one free anomeric center in each structure.
5. The original full InChIKeys are different.
6. After removing stereochemistry only at the detected anomeric centers, the
   full normalized InChIKeys are equal.

In high-level pseudocode:

```text
for each direct ChEBI child -> parent relationship:
    reject polymers and polysaccharides
    require both identifiers to be active in this stage

    require equal, concrete formulas
    require equal charges
    parse both SMILES with RDKit

    child_center  = find_exactly_one_free_anomeric_center(child)
    parent_center = find_exactly_one_free_anomeric_center(parent)

    child_key  = InChIKey(child structure)
    parent_key = InChIKey(parent structure)
    require child_key != parent_key

    normalized_child  = clear_chirality(child, child_center)
    normalized_parent = clear_chirality(parent, parent_center)

    if full_InChIKey(normalized_child) == full_InChIKey(normalized_parent):
        add an IFX Harmonization Rule edge between child and parent
```

## What RDKit does

RDKit interprets each SMILES as a molecular graph. The rule does not search for
and delete a wedge/hash bond. SMILES chirality such as `@` or `@@` is stored by
RDKit as a chiral tag on the tetrahedral atom, so the rule clears the tag on one
specific carbon atom.

### Finding the free anomeric center

The detector looks for a ring carbon that is bonded to both:

- an oxygen inside the ring; and
- a different oxygen outside the ring that is neutral, has only one heavy-atom
  bond, and bears a hydrogen — a free hydroxyl rather than an ether or
  glycosidic oxygen.

Exactly one carbon in the molecule must match.

Equivalent RDKit-style pseudocode:

```python
def find_free_anomeric_centers(molecule):
    matches = []

    for atom in molecule.atoms:
        if atom.atomic_number != CARBON or not atom.is_in_ring:
            continue

        has_ring_oxygen = any(
            neighbor.atomic_number == OXYGEN
            and neighbor.is_in_ring
            for neighbor in atom.neighbors
        )

        has_free_hydroxyl = any(
            neighbor.atomic_number == OXYGEN
            and not neighbor.is_in_ring
            and neighbor.degree == 1
            and neighbor.formal_charge == 0
            and neighbor.total_hydrogens > 0
            for neighbor in atom.neighbors
        )

        if has_ring_oxygen and has_free_hydroxyl:
            matches.append(atom.index)

    return matches
```

### Removing only the anomeric stereochemistry

The source molecule is copied. RDKit clears the chiral tag only on the detected
carbon, recalculates stereochemical assignments, and generates a full InChIKey
from the copy:

```python
normalized = copy(molecule)
center = normalized.atom(anomeric_atom_index)
center.chiral_tag = CHIRALITY_UNSPECIFIED

assign_stereochemistry(normalized, clean=True, force=True)
normalized_key = full_inchikey(normalized)
```

The rule deliberately does not call RDKit's `RemoveStereochemistry`, which
would erase every stereocenter and could incorrectly merge epimers or D/L
pairs. It does not change bonds, connectivity, isotopes, charges, double-bond
stereochemistry, or any other tetrahedral center.

The final comparison uses the complete normalized InChIKey, not just its
connectivity block.

## Audit information

Each accepted rule edge records:

- the direct ChEBI relationship;
- the original SMILES and generated InChIKey for each endpoint;
- the detected atom index for each endpoint;
- the shared normalized full InChIKey;
- algorithm version `free-anomer-v2`.

This makes every merge visible in stage comparisons and reproducible without
changing the source evidence.

## Expected boundaries

| Pair or structure | Result |
|---|---|
| Alpha, beta, and unspecified D-glucopyranose | Merge |
| Modified reducing sugar with one free anomeric OH | Merge if all checks pass |
| Finite reducing oligosaccharide with one free end | Merge if all checks pass |
| Glucose versus mannose or galactose | Keep separate |
| D versus L form | Keep separate |
| Pyranose versus furanose | Keep separate |
| Open-chain versus cyclic sugar | Keep separate |
| Alpha versus beta glycoside | Keep separate |
| Sucrose, trehalose, polymers, and generic structures | Keep separate |

## Review point

The local atom pattern is intentionally simple and explainable, but it is still
a structural heuristic. Stage comparisons and validations remain the review
surface for chemistry-wide matches, especially uncommon non-carbohydrate
hemiacetals and hemiketals. The carbohydrate-family validation continues to
guard against merging incompatible carbohydrate stereochemical families.
