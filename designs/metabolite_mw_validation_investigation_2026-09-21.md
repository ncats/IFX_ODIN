# Metabolite molecular-weight validation investigation (2026-09-21)

## Result

The final RaMP-ish stage (`stage-08-e8d423b6741afddc`) has 133 molecular-weight
warnings.  The comparable archived run had 92 warnings (the old QA view showed
only the first 50), so the increase is real but smaller than a comparison with
the old UI limit suggests.

The validator is not new: it has always tested both identifier chemical
properties and `ChemicalEntity` mass properties.  The current free-anomer
rule also did not add warnings: the stage counts were 828 immediately before
and immediately after that rule.  Cleanup reduces the final count to 133.

The dominant new cause is the RefMet update from
`sha256-f40f14165725` to `sha256-ef152c85ded4`.

## Evidence classification

| Finding | Count | Interpretation |
|---|---:|---|
| Final MW-warning groups | 133 | Current build |
| Direct incompatible source bridges | 100 groups | A source/native identifier has active neighbors with a greater-than-10% MW spread |
| Direct incompatible RefMet bridges | 66 records | 57 changed or new in the current RefMet pin; 9 existed previously |
| Changed/new RefMet bridges with a direct final warning | 32 groups | Strong explanation for the new warning population |
| Remaining groups traced through a source-evidence path | 25 | Mostly RefMet `kegg_id` chains joined to LipidMaps or HMDB |
| Groups whose min/max endpoints have no final-stage evidence path | 8 | The final membership and stage-evidence representations differ; inspect membership provenance before assigning a source |

## What changed in RefMet

Of the 66 directly incompatible RefMet records:

- 39 changed `kegg_id` values (usually blank to a KEGG compound ID);
- 10 changed `chebi_id` values;
- 8 are new RefMet records;
- one changed formula/exact mass; and
- nine were unchanged pre-existing mappings.

The KEGG additions are the main systematic issue.  They map chemically distinct
lipid molecular species to a shared, broader KEGG compound identifier.  The
harmonizer correctly follows the asserted equivalences, so this creates a
transitive clique with incompatible molecular weights.  Examples from paths in
the final graph include:

| RefMet records joined by the new shared KEGG ID | KEGG ID | MWs |
|---|---|---:|
| RM0053842 `PC P-16:0/2:0` and RM0052802 `PC P-20:0/22:0` | C00958 | 521.3 / 858.3 |
| RM0133398 `LPE 0:0/14:1(9Z)` and RM0174015 `LPE 0:0/24:0` | C05973 | 423.2 / 565.8 |
| RM0173973 `LPE 13:0/0:0` and RM0173810 `LPE 24:0/0:0` | C04438 | 411.2 / 565.8 |
| RM0050077 `PA P-16:0/12:0` and RM0050508 `PA P-20:0/22:0` | C15647 | 576.4 / 773.2 |
| RM0173247 `LPI 12:0/0:0` and RM0173140 `LPI 22:0/0:0` | C03819 | 516.2 / 656.8 |
| RM0173246 `LPG 12:0/0:0` and RM0173138 `LPG 22:0/0:0` | C18126 | 428.2 / 568.7 |
| RM0161504 `LPS 12:0/0:0` and RM0134444 `LPS 22:0/0:0` | C18125 | 441.2 / 581.7 |
| RM0012706 `CE 12:0` and RM0134014 `CE 24:0` | C02530 | 568.5 / 737.3 |

## Clear individual bad mappings

| RefMet | Current bad bridge | Evidence |
|---|---|---|
| [RM0139026](https://metabolomicsworkbench.org/databases/refmet/refmet_details.php?REFMET_ID=RM0139026) | PubChem CID 118701016 (vitamin B12, 1355.4) ↔ [CHEBI:49415](https://www.ebi.ac.uk/chebi/searchId.do?chebiId=CHEBI:49415) cobalt(3+) (58.933) | `chebi_id` changed from 176843 to 49415 |
| [RM0200998](https://metabolomicsworkbench.org/databases/refmet/refmet_details.php?REFMET_ID=RM0200998) | PubChem 33572 (ritodrine, 287.4) ↔ [CHEBI:147138](https://www.ebi.ac.uk/chebi/searchId.do?chebiId=CHEBI:147138) CID 91852571 (2591) | changed `chebi_id` |
| [RM0030757](https://metabolomicsworkbench.org/databases/refmet/refmet_details.php?REFMET_ID=RM0030757) | PubChem 18972857 (114.1) ↔ [CHEBI:138856](https://www.ebi.ac.uk/chebi/searchId.do?chebiId=CHEBI:138856) oxolinic acid (261.2) | changed `chebi_id` and `kegg_id` |
| [RM0228128](https://metabolomicsworkbench.org/databases/refmet/refmet_details.php?REFMET_ID=RM0228128) | PubChem 118658 (158.3) ↔ [CHEBI:152031](https://www.ebi.ac.uk/chebi/searchId.do?chebiId=CHEBI:152031) CID 71297698 (2661) | new RefMet record |
| [RM0227083](https://metabolomicsworkbench.org/databases/refmet/refmet_details.php?REFMET_ID=RM0227083) | HMDB0006044 (165.2) ↔ [CHEBI:110006](https://www.ebi.ac.uk/chebi/searchId.do?chebiId=CHEBI:110006) (516.7) | new RefMet record |
| [RM0228119](https://metabolomicsworkbench.org/databases/refmet/refmet_details.php?REFMET_ID=RM0228119) | HMDB0032386 (134.2) ↔ PubChem 11339 (422.6) | new RefMet record; likely wrong HMDB cross-reference |

## Reproducible final-stage paths

The following are representative residual paths, where `>` means a recorded
stage evidence edge.  They demonstrate that the warning is a source mapping
issue, not a mass-validation implementation change.

```text
PubChem:6419702 > LipidMaps:LMGL02010000 > KEGG:C00641
  > RefMet:RM0174535 > PubChem:9543942

LipidMaps:LMGP01030009 > RefMet:RM0053842 > KEGG:C00958
  > RefMet:RM0052802 > PubChem:52924028

CHEBI:183816 > RefMet:RM0012706 > KEGG:C02530
  > RefMet:RM0134014 > PubChem:189779

CHEBI:166677 > RefMet:RM0138851 > KEGG:C09103
  > RefMet:RM0002237 > CHEBI:5542
```

Three of the membership-only residual cliques have now also been resolved:

| Warning clique | Bad bridge | What changed |
|---:|---|---|
| 2042 | `REFMET:RM0118032` (olsalazine) ↔ KEGG:C07323 ↔ HMDB0015380 | RefMet added `kegg_id=C07323` |
| 6022 | `REFMET:RM0138607` (CpA) ↔ KEGG:C03032 ↔ `REFMET:RM0109648` (alpha-cyclopiazonic acid) | RefMet added `kegg_id=C03032` to RM0138607; the other RefMet row was unchanged |
| 24743 | LipidMaps:LMST03020620 ↔ PubChem:9986125 and CHEBI:184723 | This is a LipidMaps bridge, not a RefMet-refresh regression; the incompatible extrema are retained through older clique membership |

Two additional residual cliques are rule-created rather than directly caused by
a refreshed cross-reference:

| Warning clique | First co-membership stage | Interpretation |
|---:|---|---|
| 20 | `Merge InchiKey with cutoff` (stage 4) | CHEBI:72682 and HMDB0062314 first enter the same clique through the direct InChIKey rule |
| 259 | `Merge Derived InchiKey with cutoff` (stage 5) | PubChem:119058 and PubChem:5283544 first enter the same clique through the derived-InChIKey rule |
| 260 | `Merge InchiKey with cutoff` (stage 4) | CHEBI:32970 and CHEBI:146206 first enter the same clique through the direct InChIKey rule |
| 283 | `Merge InchiKey with cutoff` (stage 4) | CHEBI:78277 and PubChem:92731 first enter the same clique through the direct InChIKey rule |
| 365 | `Merge InchiKey with cutoff` (stage 4) | PubChem:961 and CHEBI:29412 first enter the same clique through the direct InChIKey rule |

## Consequence

The next remediation should be source/mapping curation, not disabling or
loosening MW validation.  In particular, RefMet-to-KEGG mappings for specific
lipid species should not be treated as unconditional identity bridges when the
KEGG record is a broad class.  The current warning set also retains a smaller
pre-existing population from HMDB/LipidMaps mappings; those should be reviewed
separately from the RefMet-refresh regression.

## RefMet chemistry ingestion decision

The pinned RefMet CSV already supplies `formula`, `exactmass`, and `inchi_key`,
but the harmonization adapter previously discarded all three and emitted only
the RefMet name and cross-reference edges. Payload profiling found:

- 208,141 of 208,170 rows have both a formula and exact mass;
- 35,597 rows have a reported InChIKey; and
- all 332 rows linked to generic KEGG compound `C02737` have all three fields.

The adapter now retains these values in a RefMet-owned `chem_props` record:
`formula` becomes `molecular_formula`, `exactmass` becomes
`monoisotopic_mass`, and `inchi_key` plus its connectivity prefix are retained
as reported. This intentionally exposes RefMet records to all three downstream
checks in the same rebuild: generic-structure classification, molecular-weight
validation, and reported/effective InChIKey merge rules. A concrete formula is
treated as non-generic under the harmonizer's current definition (no unresolved
R group or wildcard); that classification does not claim complete positional
or stereochemical resolution.

### RefMet exact-mass sanity check

The reported `exactmass` values were compared with monoisotopic masses
calculated from the complete RefMet formulas using RDKit's periodic-table
most-common-isotope masses (the same formula calculator used for ChEBI mass
screening):

- 208,141 rows were comparable; 29 lacked both formula and exact mass;
- every populated formula was parseable by the calculator;
- median absolute difference was 0.000001526 Da;
- the 95th percentile was 0.000004464 Da and the 99th percentile was
  0.000025326 Da;
- only 19 rows differed by more than 0.001 Da, and 18 differed by more than
  0.5 Da; and
- all 332 RefMet records linked to KEGG `C02737` were within 0.000035 Da.

The 18 material exceptions are isolated source-data inconsistencies rather
than a unit or field-semantics problem. Four reported values are lower than the
formula calculation by one oxygen mass (15.994915 Da), two are lower by one
sulfur mass (31.972070 Da), and most remaining differences are approximately
one or two daltons in halogen-containing compounds. Representative records
include `RM0189233` (N-Propionylputrescine), `RM0189207`
(N-Acetylamylamine), `RM0189210` (N-Propionylcysteine), `RM0189234`
(N-Propionyltaurine), and `RM0118236` (Mitobronitol).

The adapter intentionally preserves these reported values instead of silently
recalculating them. That keeps source fidelity and allows the MW validation to
surface the small anomalous population during the full rebuild.
