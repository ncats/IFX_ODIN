# Carbohydrate Family Conflict Validation

**Status:** Implemented as an always-on Metabolite Harmonization Studio stage validation.

**Algorithm version:** `carbohydrate-family-v1`

## What it checks

The validation flags a harmonized clique when it contains ChEBI carbohydrates
from more than one structurally compatible family. It is intended to reveal
incorrect source mappings even when a later, valid merge rule merely makes the
problem more visible.

For example, RefMet `RM0034731` maps to both D-galactopyranose (`CHEBI:4139`)
and HMDB `HMDB0033704`, whose structure is L-galactose. The free-anomer rule
correctly gathers the D anomers and the L anomers, but the RefMet mapping joins
those two families. This validation reports that final clique without changing
it.

## Scope

The validator examines structured descendants of `CHEBI:16646`
(`carbohydrate`). It includes monosaccharides, oligosaccharides, locked
glycosides, and nonreducing carbohydrates when ChEBI supplies a concrete,
comparable SMILES.

Wildcard, unparseable, multiply free-anomeric, and stereochemically
under-specified structures are recorded as unclassified. An unclassified term
does not create a conflict by itself.

## Algorithm

The validator and the [free-anomer merge rule](metabolite_free_anomer_harmonization_rule.md)
use the same RDKit structure helper. Stored or source-reported InChIKeys are not
used to classify a family.

```text
for each ChEBI carbohydrate:
    molecule = RDKit.parse(ChEBI SMILES)
    original_key = RDKit.full_InChIKey(molecule)
    free_centers = find_free_anomeric_centers(molecule)

    if exactly one free center:
        require all other stereocenters to be specified
        normalized = copy(molecule)
        clear chirality only at the free center
        family_key = RDKit.full_InChIKey(normalized)
    else if no free centers:
        require all stereocenters to be specified
        family_key = original_key
    else:
        mark structure unclassified

for each harmonized clique:
    group comparable ChEBI members by family_key
    if more than one family_key is present:
        store one carbohydrate-family conflict warning for the clique
```

Alpha, beta, and anomer-unspecified forms of the same reducing carbohydrate
therefore converge. D and L forms, epimers such as glucose and galactose,
different connectivities, and distinct locked glycosides remain different.

## Relationship to InChIKey cutoff merging

The molecular-weight cutoff rules use the InChIKey duplex below the cutoff and
the connectivity prefix at or above it. This validation is independent of
those merge decisions:

- below the cutoff, it can expose an incompatible family joined through a
  source mapping even though the duplex rule would not merge the structures;
- at or above the cutoff, it audits the stereochemical families intentionally
  gathered by prefix matching; and
- after free-anomer merging, it tolerates only the anomeric difference that the
  rule explicitly normalized.

Warnings are review signals. They neither split the clique nor automatically
choose which evidence edge should be removed.
