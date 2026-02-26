"""Mapping coverage checks for POUNCE analyte sheets.

For each analyte type (Metabolite, Gene), checks what fraction of the
submitted IDs will resolve to a canonical node in the graph after ETL.
"""

from dataclasses import dataclass, field
from typing import List, Optional

from src.input_adapters.pounce_sheets.parsed_classes import ParsedGene, ParsedMetab
from src.interfaces.id_resolver import IdResolver


@dataclass
class AnalyteCoverage:
    analyte_type: str        # "Metabolite" or "Gene"
    sheet: str               # "MetabMeta" or "GeneMeta"
    total: int
    mapped: int
    unmapped_ids: List[str] = field(default_factory=list)

    @property
    def mapped_pct(self) -> float:
        return 100.0 * self.mapped / self.total if self.total else 0.0


def check_metabolite_coverage(
    metabolites: List[ParsedMetab], resolver: IdResolver
) -> AnalyteCoverage:
    """Check how many MetabMeta entries will resolve to canonical Metabolite nodes.

    Uses the same resolver that runs during ETL.  A metabolite is considered
    mapped if its primary ID *or* any of its alternate IDs resolves.
    """
    from src.models.metabolite import Metabolite

    # Deduplicate by metab_id — the same ID can appear in multiple experiment
    # files when a submission has more than one experiment workbook.
    seen: dict[str, ParsedMetab] = {}
    for m in metabolites:
        if m.metab_id and m.metab_id not in seen:
            seen[m.metab_id] = m
    unique_metabolites = list(seen.values())

    mapped = 0
    unmapped_ids = []

    for m in unique_metabolites:
        ids = [m.metab_id]
        if m.alternate_metab_id:
            ids += [x.strip() for x in m.alternate_metab_id.split("|") if x.strip()]

        stubs = [Metabolite(id=i) for i in ids]
        results = resolver.resolve_internal(stubs)
        did_map = any(len(results.get(stub.id, [])) > 0 for stub in stubs)

        if did_map:
            mapped += 1
        else:
            unmapped_ids.append(m.metab_id)

    return AnalyteCoverage(
        analyte_type="Metabolite",
        sheet="MetabMeta",
        total=len(unique_metabolites),
        mapped=mapped,
        unmapped_ids=unmapped_ids,
    )


def check_gene_coverage(
    genes: List[ParsedGene], resolver: Optional[IdResolver] = None
) -> Optional[AnalyteCoverage]:
    """Check how many GeneMeta entries will resolve to canonical Gene nodes.

    Returns None when no gene resolver is configured.
    """
    if resolver is None:
        return None

    from src.constants import Prefix
    from src.models.gene import Gene
    from src.models.node import EquivalentId

    # Deduplicate by gene_id across experiment files.
    unique_gene_ids = list(dict.fromkeys(g.gene_id for g in genes if g.gene_id))

    mapped = 0
    unmapped_ids = []

    for gene_id in unique_gene_ids:
        ensembl_id = EquivalentId(id=gene_id, type=Prefix.ENSEMBL).id_str()
        results = resolver.resolve_internal([Gene(id=ensembl_id)])
        did_map = len(results.get(ensembl_id, [])) > 0

        if did_map:
            mapped += 1
        else:
            unmapped_ids.append(gene_id)

    return AnalyteCoverage(
        analyte_type="Gene",
        sheet="GeneMeta",
        total=len(unique_gene_ids),
        mapped=mapped,
        unmapped_ids=unmapped_ids,
    )