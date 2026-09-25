# KEGG Equivalence-Denylist Generic-Structure Inventory

Date: 2026-09-24

## Scope and safety

This began as a read-only inventory of the published v2
`metabolite_equivalence_edges` curation stream in the `aws-ifx-registry`
bucket. After review, temporary append-only test batches were published and
then consolidated into clean pre-release baselines on 2026-09-24.

The manifest was at revision 12 with 12 batches when inspected. Resolving the
latest operation for each edge left 1,017 active `remove_edge` decisions that
involve 200 distinct `KEGG.COMPOUND` identifiers.

## Confirmed generic structures

Thirty-one identifiers below have current KEGG records with at least one of the
following forms of evidence:

- KEGG explicitly says `Generic compound` in the record comment;
- the KEGG formula contains an `R` substituent or variable repeat count; or
- the KEGG structure contains an `R` atom.

Two retired KEGG identifiers, `C00626` and `C01808`, were additionally confirmed
as generic by the curator on 2026-09-24. Together, the 33 confirmed identifiers
occurred in 817 active KEGG-involving denylist pairs. Before the cleanup,
`C02530` and `C03033` already had published `is_generic_structure=true`
annotation overrides; the other 31 did not. After the cleanup, all 33 resolve
to `is_generic_structure=true`.

| KEGG ID | KEGG name | Pre-cleanup denylist pairs | Evidence | Pre-cleanup annotation |
|---|---|---:|---|---|
| [C00110](https://www.kegg.jp/entry/C00110) | Dolichyl phosphate | 1 | Generic comment; variable repeat formula | — |
| [C00157](https://www.kegg.jp/entry/C00157) | Phosphatidylcholine | 143 | Generic comment; `R2` formula/structure | — |
| [C00165](https://www.kegg.jp/entry/C00165) | Diacylglycerol | 19 | Generic comment; `R2` formula/structure | — |
| [C00195](https://www.kegg.jp/entry/C00195) | N-Acylsphingosine | 12 | Generic comment; `R` formula/structure | — |
| [C00350](https://www.kegg.jp/entry/C00350) | Phosphatidylethanolamine | 56 | Generic comment; `R2` formula/structure | — |
| [C00399](https://www.kegg.jp/entry/C00399) | Ubiquinone | 2 | Generic comment; variable repeat formula | — |
| [C00416](https://www.kegg.jp/entry/C00416) | Phosphatidate | 15 | Generic comment; `R2` formula/structure | — |
| [C00422](https://www.kegg.jp/entry/C00422) | Triacylglycerol | 119 | Generic comment; `R3` formula/structure | — |
| [C00550](https://www.kegg.jp/entry/C00550) | Sphingomyelin | 20 | Generic comment; `R` formula/structure | — |
| `C00626` | Phosphatidylinositol (retired KEGG ID) | 188 | Curator-confirmed generic; legacy RaMP links to specific phosphatidylinositols | — |
| [C00760](https://www.kegg.jp/entry/C00760) | Cellulose | 1 | Polymer formula/structure | — |
| [C01190](https://www.kegg.jp/entry/C01190) | Glucosylceramide | 14 | Generic comment; `R` formula/structure | — |
| [C01246](https://www.kegg.jp/entry/C01246) | Dolichyl beta-D-glucosyl phosphate | 1 | Variable repeat formula/structure | — |
| [C01290](https://www.kegg.jp/entry/C01290) | Lactosylceramide | 7 | Generic comment; `R` formula/structure | — |
| `C01808` | Ganglioside (retired KEGG ID) | 3 | Curator-confirmed generic; legacy mappings link to specific gangliosides | — |
| [C02530](https://www.kegg.jp/entry/C02530) | Cholesterol ester | 23 | `R` formula/structure | `true` |
| [C02686](https://www.kegg.jp/entry/C02686) | Galactosylceramide | 8 | Generic comment; `R` formula/structure | — |
| [C02737](https://www.kegg.jp/entry/C02737) | Phosphatidylserine | 8 | Generic comment; `R2` formula/structure | — |
| [C03033](https://www.kegg.jp/entry/C03033) | beta-D-Glucuronoside | 31 | Generic comment; `R` formula/structure | `true` |
| [C03272](https://www.kegg.jp/entry/C03272) | Globoside | 11 | Generic comment; `R` formula/structure | — |
| [C03323](https://www.kegg.jp/entry/C03323) | (2,1-beta-D-Fructosyl)n | 1 | Polymer formula/structure | — |
| [C04230](https://www.kegg.jp/entry/C04230) | 1-Acyl-sn-glycero-3-phosphocholine | 43 | Generic comment; `R` formula/structure | — |
| [C04537](https://www.kegg.jp/entry/C04537) | N,N'-Diacetylchitobiosyldiphosphodolichol | 2 | Variable repeat formula/structure | — |
| [C04730](https://www.kegg.jp/entry/C04730) | GM3 | 11 | Generic comment; `R` formula/structure | — |
| [C04737](https://www.kegg.jp/entry/C04737) | alpha-D-Galactosyl-(1→4)-beta-D-galactosyl-(1→4)-beta-D-glucosyl-(1↔1)-ceramide | 7 | Generic comment; `R` formula/structure | — |
| [C04884](https://www.kegg.jp/entry/C04884) | N-Acetyl-D-galactosaminyl-(N-acetylneuraminyl)-D-galactosyl-D-glucosylceramide | 10 | Generic comment; `R` formula/structure | — |
| [C04911](https://www.kegg.jp/entry/C04911) | D-Galactosyl-N-acetyl-D-galactosaminyl-(N-acetylneuraminyl)-D-galactosyl-D-glucosylceramide | 11 | Generic comment; `R` formula/structure | — |
| [C06125](https://www.kegg.jp/entry/C06125) | Sulfatide | 16 | Generic comment; `R` formula/structure | — |
| [C06126](https://www.kegg.jp/entry/C06126) | Digalactosylceramide | 10 | Generic comment; `R` formula/structure | — |
| [C06133](https://www.kegg.jp/entry/C06133) | GD3 | 11 | Generic comment; `R` formula/structure | — |
| [C06135](https://www.kegg.jp/entry/C06135) | GA2 | 3 | Generic comment; `R` formula/structure | — |
| [C06136](https://www.kegg.jp/entry/C06136) | GA1 | 9 | Generic comment; `R` formula/structure | — |
| [C12030](https://www.kegg.jp/entry/C12030) | Mycosubtilin | 1 | Generic comment; `R = fatty acid` | — |

These 817 pre-cleanup pairs occurred in five published edge batches:

| Manifest position | Batch | Pairs involving confirmed-generic KEGG IDs |
|---:|---|---:|
| 1 | `ramp-mapping-denylist-cbf616e2e7fb` | 802 |
| 6 | `qa-browser-b6f5319f-6667-4fcf-be88-d993afc3abbb` | 10 |
| 7 | `qa-browser-b8f6082e-b9af-4911-9c26-5c1efdb4644e` | 1 |
| 8 | `qa-browser-1c95fdef-b1ed-43fd-835e-4bfe92ec5908` | 3 |
| 10 | `qa-browser-e70d0686-e1a0-4589-9fb1-4bf9134302b7` | 1 |

The generic-mismatch rule replaces a pair-specific deny decision only when
exactly one endpoint is generic. Graph-wide structure classification found 809
generic-to-specific pairs, which made their pair-specific deny decisions
unnecessary. The remaining eight pairs had generic structures at both ends and
were reviewed separately:

```text
CHEBI:11750  KEGG.COMPOUND:C02737
CHEBI:16038  KEGG.COMPOUND:C00350
CHEBI:16337  KEGG.COMPOUND:C00416
CHEBI:17002  KEGG.COMPOUND:C02530
CHEBI:17855  KEGG.COMPOUND:C00422
CHEBI:18035  KEGG.COMPOUND:C00165
CHEBI:18303  KEGG.COMPOUND:C02737
CHEBI:64674  KEGG.COMPOUND:C00350
```

Seven pairs have explicit matching ChEBI and KEGG cross-references. The eighth,
`CHEBI:64674` to `KEGG.COMPOUND:C00350`, is a stereospecific
phosphatidylethanolamine mapped to KEGG's broader generic
phosphatidylethanolamine representation. The curator determined that
stereochemistry is enforced by other harmonization rules and should not be an
equivalence-denylist concern. All eight were therefore removed from the
denylist along with the 809 generic-to-specific decisions.

## Final published state

Two append-only test batches were first used to validate the behavior:

| Curation type | Batch | Operations | Resulting manifest revision |
|---|---|---:|---:|
| `metabolite_annotations` | `pre-release-kegg-generic-annotations-388b12b4a906007c` | 33 `set_properties` | 2 |
| `metabolite_equivalence_edges` | `pre-release-kegg-generic-edge-retentions-83e1ba7c3d4fd47b` | 809 `retain_edge` | 13 |

After scientific review of the eight generic-to-generic pairs, the pre-release
history was compacted. The annotation manifest at revision 3 references only
the reviewed 33-operation annotation batch. The equivalence-edge manifest at
revision 14 references only
`pre-release-equivalence-denylist-baseline-f98ae4a78a6766a9`, containing 1,339
active `remove_edge` decisions and no `retain_edge` operations. This is eight
fewer active removals than the temporary revision-13 state and 817 fewer than
the original denylist state for the reviewed generic KEGG identifiers.

Fourteen unreferenced old batch objects were deleted after the new manifests
and effective states were verified. A local rollback archive was created before
the destructive cleanup. The annotation decision fingerprint remained
unchanged across compaction. The saved `curation test 1` and `curation test 2`
pipelines both apply the annotation and edge curation types before
`ignore_generic_structure_mismatch`; they were not synchronized or rerun as
part of this cleanup.

## Retired record not classified as generic

KEGG's current REST service and GenomeNet entry pages also return no data for
`C03324`. Unlike `C00626` and `C01808`, the available evidence does not support
classifying it as generic:

| KEGG ID | Active denylist pairs | Available evidence | Status |
|---|---:|---|---|
| `C03324` | 1 | The counterpart is ChEBI 146129, beta-D-fructofuranosyl-(2→1)-beta-D-fructofuranose | Appears specific; do not mark generic without contrary historical evidence |

## All 200 KEGG IDs on the pre-cleanup active equivalence denylist

```text
C00013 C00014 C00042 C00108 C00110 C00124 C00127 C00157 C00165 C00178
C00190 C00195 C00203 C00243 C00265 C00303 C00334 C00350 C00399 C00416
C00422 C00542 C00546 C00550 C00626 C00744 C00760 C00806 C00992 C01094
C01103 C01104 C01190 C01228 C01246 C01290 C01324 C01327 C01464 C01470
C01479 C01536 C01553 C01571 C01589 C01594 C01657 C01692 C01808 C01839
C01956 C01996 C02008 C02018 C02088 C02154 C02198 C02220 C02373 C02445
C02530 C02595 C02686 C02737 C02740 C02951 C03033 C03107 C03173 C03219
C03272 C03323 C03324 C03325 C03351 C03541 C03589 C03885 C03958 C04132
C04230 C04537 C04730 C04737 C04771 C04780 C04884 C04911 C05276 C05465
C05487 C05579 C05619 C05643 C05654 C05720 C05768 C05844 C06050 C06079
C06125 C06126 C06133 C06135 C06136 C06335 C06353 C06397 C06551 C06818
C07078 C07119 C07184 C07213 C07216 C07359 C07432 C07475 C07560 C07593
C07645 C07649 C07733 C08070 C08075 C08114 C08355 C08374 C08452 C08594
C08653 C09312 C09506 C09678 C09685 C09752 C09801 C09808 C09826 C09840
C09893 C09994 C10009 C10011 C10023 C10029 C10054 C10187 C10370 C10443
C10487 C10650 C10822 C10897 C11002 C11118 C11221 C11455 C11521 C11647
C11682 C12030 C12139 C12144 C12303 C13235 C13297 C13422 C13482 C13798
C13809 C13828 C13881 C14285 C14422 C14519 C14701 C14996 C15497 C15670
C16302 C16526 C16576 C16635 C16678 C16679 C16797 C16927 C16947 C16996
C17058 C17441 C17488 C17489 C17512 C17568 C18690 C19506 C19748 C20046
```

## Compaction method

The published batches were not edited in place. A new immutable edge baseline
was written, both manifests were replaced conditionally using their previously
read ETags, and the resolved states were verified before obsolete batch objects
were deleted. The manifest revisions remain monotonic even though each manifest
now references one clean batch.
