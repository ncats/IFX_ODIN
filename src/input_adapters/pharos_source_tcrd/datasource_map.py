from __future__ import annotations

from datetime import date
from typing import Any, Generator, Iterable, Optional

import yaml
from sqlalchemy import text

from src.constants import DataSourceName
from src.input_adapters.sql_adapter import MySqlAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.ncats_datasource import NcatsDataSourceInfo, NcatsDataSourceMapEntry
from src.shared.arango_adapter import ArangoAdapter
from src.shared.db_credentials import DBCredentials


DATASOURCE_URLS = {
    "Antibodypedia": "https://www.antibodypedia.com/",
    "ARCHS4": "https://archs4.org/",
    "BioPlex Protein-Protein Interactions": "https://bioplex.hms.harvard.edu/",
    "ChEMBL Activities": "https://www.ebi.ac.uk/chembl/",
    "ChEMBL IDs": "https://www.ebi.ac.uk/chembl/",
    "CTD": "https://ctdbase.org/",
    "Dark Kinase Knowledgebase": "https://darkkinome.org/",
    "Disease Ontology": "http://www.disease-ontology.org/",
    "Drug Central - ChEMBL": "http://drugcentral.org/",
    "Drug Central - Drug Label": "http://drugcentral.org/",
    "Drug Central - GtoPdb": "http://drugcentral.org/",
    "Drug Central - Kegg Drug": "http://drugcentral.org/",
    "Drug Central - Scientific Literature": "http://drugcentral.org/",
    "Drug Central Indication": "http://drugcentral.org/",
    "Drug Target Ontology IDs and Classifications": "http://drugtargetontology.org/",
    "eRAM": "http://www.unimd.org/eram/",
    "Gene Ontology": "http://geneontology.org/",
    "GlyGen": "https://glygen.org/",
    "GTEx": "http://www.gtexportal.org/",
    "Guide to Pharmacology": "https://www.guidetopharmacology.org/",
    "HGNC": "https://www.genenames.org/",
    "Harmonizome": "http://amp.pharm.mssm.edu/Harmonizome/",
    "Human Protein Atlas Protein": "https://www.proteinatlas.org/",
    "Human Protein Atlas RNA": "https://www.proteinatlas.org/",
    "Human Proteome Map": "https://www.humanproteomemap.org/",
    "IMPC Phenotypes": "https://www.mousephenotype.org/",
    "JAX/MGI Mouse/Human Orthology Phenotypes": "http://www.informatics.jax.org/",
    "JensenLab Experiment TIGA": "https://diseases.jensenlab.org/",
    "JensenLab Knowledge AmyCo": "https://diseases.jensenlab.org/",
    "JensenLab Knowledge MedlinePlus": "https://diseases.jensenlab.org/",
    "JensenLab Knowledge UniProtKB-KW": "https://diseases.jensenlab.org/",
    "JensenLab PubMed Text-mining Scores": "https://diseases.jensenlab.org/",
    "JensenLab TISSUES": "https://tissues.jensenlab.org/",
    "JensenLab Text Mining": "https://diseases.jensenlab.org/",
    "LinkedOmicsKB": "https://linkedomics.org/",
    "MONDO": "https://mondo.monarchinitiative.org/",
    "NCBI Gene": "https://www.ncbi.nlm.nih.gov/gene/",
    "NCBI GeneRIFs": "https://www.ncbi.nlm.nih.gov/gene/about-generif/",
    "Orthologs": "https://www.genenames.org/tools/hcop/",
    "P-HIPSTer Viral PPIs": "http://phipster.org/",
    "PANTHER Protein Classes": "http://www.pantherdb.org/",
    "PathwayCommons": "https://www.pathwaycommons.org/",
    "PubChem": "https://pubchem.ncbi.nlm.nih.gov/",
    "PubChem CIDs": "https://pubchem.ncbi.nlm.nih.gov/",
    "PubMed": "https://pubmed.ncbi.nlm.nih.gov/",
    "PubTator Text-mining Scores": "https://www.ncbi.nlm.nih.gov/research/pubtator/",
    "RDAS": "https://rdas.ncats.nih.gov/",
    "Reactome Pathways": "https://reactome.org/",
    "Reactome Protein-Protein Interactions": "https://reactome.org/",
    "RESOLUTE": "https://re-solute.eu/",
    "STRINGDB": "https://string-db.org/",
    "SureChEMBL Patent Family Count": "https://www.surechembl.org/",
    "Target Illumination GWAS Analytics (TIGA)": "https://unmtid-shinyapps.net/shiny/tiga/",
    "TIN-X Data": "https://www.newdrugtargets.org/",
    "UniProt": "https://www.uniprot.org/",
    "UniProt Disease": "https://www.uniprot.org/",
    "WikiPathways": "https://www.wikipathways.org/",
}


PROTEIN_SQL_RULES = [
    ("Antibodypedia", "SELECT DISTINCT protein_id FROM tdl_info WHERE integer_value > 0 AND itype = 'Ab Count'"),
    ("ARCHS4", "SELECT DISTINCT protein_id FROM extlink WHERE source = 'ARCHS4'"),
    ("BioPlex Protein-Protein Interactions", """
        SELECT DISTINCT protein_id FROM (
          SELECT protein_id FROM ncats_ppi WHERE ppitypes LIKE '%BioPlex%'
          UNION
          SELECT other_id AS protein_id FROM ncats_ppi WHERE ppitypes LIKE '%BioPlex%'
        ) x WHERE protein_id IS NOT NULL
    """),
    ("CTD", "SELECT DISTINCT protein_id FROM disease WHERE dtype = 'CTD' AND protein_id IS NOT NULL"),
    ("Dark Kinase Knowledgebase", "SELECT DISTINCT protein_id FROM extlink WHERE source = 'Dark Kinome'"),
    ("DRGC Resources", """
        SELECT DISTINCT t2tc.protein_id
        FROM drgc_resource
        JOIN t2tc ON t2tc.target_id = drgc_resource.target_id
        WHERE t2tc.protein_id IS NOT NULL
    """),
    ("Drug Central Indication", "SELECT DISTINCT protein_id FROM disease WHERE dtype = 'DrugCentral Indication' AND protein_id IS NOT NULL"),
    ("Drug Target Ontology IDs and Classifications", "SELECT DISTINCT id AS protein_id FROM protein WHERE dtoid IS NOT NULL"),
    ("Ensembl Gene IDs", "SELECT DISTINCT protein_id FROM xref WHERE xtype = 'Ensembl' AND value LIKE 'ENSG%' AND protein_id IS NOT NULL"),
    ("eRAM", "SELECT DISTINCT protein_id FROM disease WHERE dtype = 'eRAM' AND protein_id IS NOT NULL"),
    ("Gene Ontology", "SELECT DISTINCT protein_id FROM goa WHERE protein_id IS NOT NULL"),
    ("GlyGen", "SELECT DISTINCT protein_id FROM extlink WHERE source = 'GlyGen'"),
    ("GTEx", "SELECT DISTINCT protein_id FROM gtex WHERE protein_id IS NOT NULL"),
    ("HGNC", "SELECT DISTINCT protein_id FROM xref WHERE xtype = 'HGNC' AND protein_id IS NOT NULL"),
    ("Harmonizome", "SELECT DISTINCT protein_id FROM hgram_cdf WHERE protein_id IS NOT NULL"),
    ("Human Protein Atlas Protein", "SELECT DISTINCT protein_id FROM expression WHERE etype = 'HPA Protein' AND protein_id IS NOT NULL"),
    ("Human Protein Atlas RNA", "SELECT DISTINCT protein_id FROM expression WHERE etype = 'HPA RNA' AND protein_id IS NOT NULL"),
    ("Human Proteome Map", "SELECT DISTINCT protein_id FROM expression WHERE etype IN ('HPM Protein', 'HPM Gene') AND protein_id IS NOT NULL"),
    ("IDG Families", """
        SELECT DISTINCT t2tc.protein_id
        FROM target
        JOIN t2tc ON t2tc.target_id = target.id
        WHERE target.fam IS NOT NULL AND t2tc.protein_id IS NOT NULL
    """),
    ("IMPC Phenotypes", "SELECT DISTINCT protein_id FROM phenotype WHERE ptype = 'IMPC' AND protein_id IS NOT NULL"),
    ("JAX/MGI Mouse/Human Orthology Phenotypes", "SELECT DISTINCT protein_id FROM phenotype WHERE ptype = 'JAX/MGI Human Ortholog Phenotype' AND protein_id IS NOT NULL"),
    ("JensenLab Experiment TIGA", "SELECT DISTINCT protein_id FROM disease WHERE dtype = 'JensenLab Experiment TIGA' AND protein_id IS NOT NULL"),
    ("JensenLab Knowledge AmyCo", "SELECT DISTINCT protein_id FROM disease WHERE dtype = 'JensenLab Knowledge AmyCo' AND protein_id IS NOT NULL"),
    ("JensenLab Knowledge MedlinePlus", "SELECT DISTINCT protein_id FROM disease WHERE dtype = 'JensenLab Knowledge MedlinePlus' AND protein_id IS NOT NULL"),
    ("JensenLab Knowledge UniProtKB-KW", "SELECT DISTINCT protein_id FROM disease WHERE dtype = 'JensenLab Knowledge UniProtKB-KW' AND protein_id IS NOT NULL"),
    ("JensenLab PubMed Text-mining Scores", "SELECT DISTINCT protein_id FROM tdl_info WHERE itype = 'JensenLab PubMed Score' AND number_value > 0 AND protein_id IS NOT NULL"),
    ("JensenLab TISSUES", "SELECT DISTINCT protein_id FROM expression WHERE etype = 'JensenLab TISSUES' AND protein_id IS NOT NULL"),
    ("JensenLab Text Mining", "SELECT DISTINCT protein_id FROM disease WHERE dtype = 'JensenLab Text Mining' AND protein_id IS NOT NULL"),
    ("LinkedOmicsKB", "SELECT DISTINCT protein_id FROM extlink WHERE source = 'LinkedOmicsKB'"),
    ("NCBI Gene", "SELECT DISTINCT protein_id FROM xref WHERE xtype = 'NCBIGene' AND protein_id IS NOT NULL"),
    ("NCBI GeneRIFs", "SELECT DISTINCT protein_id FROM generif WHERE protein_id IS NOT NULL"),
    ("Orthologs", "SELECT DISTINCT protein_id FROM ortholog WHERE protein_id IS NOT NULL"),
    ("P-HIPSTer Viral PPIs", "SELECT DISTINCT protein_id FROM viral_ppi WHERE protein_id IS NOT NULL"),
    ("PANTHER Protein Classes", "SELECT DISTINCT protein_id FROM p2pc WHERE protein_id IS NOT NULL"),
    ("PathwayCommons", "SELECT DISTINCT protein_id FROM pathway WHERE pwtype LIKE 'PathwayCommons%' AND protein_id IS NOT NULL"),
    ("PubChem", "SELECT DISTINCT protein_id FROM extlink WHERE source = 'PubChem'"),
    ("PubMed", "SELECT DISTINCT protein_id FROM protein2pubmed WHERE protein_id IS NOT NULL"),
    ("PubTator Text-mining Scores", "SELECT DISTINCT protein_id FROM tdl_info WHERE itype = 'PubTator Score' AND protein_id IS NOT NULL"),
    ("Reactome Pathways", "SELECT DISTINCT protein_id FROM pathway WHERE pwtype = 'Reactome' AND protein_id IS NOT NULL"),
    ("Reactome Protein-Protein Interactions", """
        SELECT DISTINCT protein_id FROM (
          SELECT protein_id FROM ncats_ppi WHERE ppitypes LIKE '%Reactome%'
          UNION
          SELECT other_id AS protein_id FROM ncats_ppi WHERE ppitypes LIKE '%Reactome%'
        ) x WHERE protein_id IS NOT NULL
    """),
    ("RESOLUTE", "SELECT DISTINCT protein_id FROM extlink WHERE source = 'RESOLUTE'"),
    ("STRINGDB", """
        SELECT DISTINCT protein_id FROM (
          SELECT protein_id FROM ncats_ppi WHERE ppitypes LIKE '%STRING%'
          UNION
          SELECT other_id AS protein_id FROM ncats_ppi WHERE ppitypes LIKE '%STRING%'
        ) x WHERE protein_id IS NOT NULL
    """),
    ("SureChEMBL Patent Family Count", "SELECT DISTINCT protein_id FROM patent_count WHERE protein_id IS NOT NULL"),
    ("Target Illumination GWAS Analytics (TIGA)", "SELECT DISTINCT protein_id FROM tiga WHERE protein_id IS NOT NULL"),
    ("TIN-X Data", "SELECT DISTINCT protein_id FROM tinx_importance WHERE protein_id IS NOT NULL"),
    ("UniProt", "SELECT DISTINCT id AS protein_id FROM protein WHERE uniprot IS NOT NULL AND uniprot != ''"),
    ("UniProt Disease", "SELECT DISTINCT protein_id FROM disease WHERE dtype = 'UniProt' AND protein_id IS NOT NULL"),
    ("WikiPathways", "SELECT DISTINCT protein_id FROM pathway WHERE pwtype = 'WikiPathways' AND protein_id IS NOT NULL"),
]


DISEASE_SQL_RULES = [
    ("CTD", "SELECT DISTINCT ncats_name AS disease_name FROM disease WHERE dtype = 'CTD' AND ncats_name IS NOT NULL AND ncats_name != ''"),
    ("Disease Ontology", "SELECT DISTINCT name AS disease_name FROM do WHERE name IS NOT NULL AND name != ''"),
    ("Drug Central Indication", "SELECT DISTINCT ncats_name AS disease_name FROM disease WHERE dtype = 'DrugCentral Indication' AND ncats_name IS NOT NULL AND ncats_name != ''"),
    ("eRAM", "SELECT DISTINCT ncats_name AS disease_name FROM disease WHERE dtype = 'eRAM' AND ncats_name IS NOT NULL AND ncats_name != ''"),
    ("JensenLab Experiment TIGA", "SELECT DISTINCT ncats_name AS disease_name FROM disease WHERE dtype = 'JensenLab Experiment TIGA' AND ncats_name IS NOT NULL AND ncats_name != ''"),
    ("JensenLab Knowledge AmyCo", "SELECT DISTINCT ncats_name AS disease_name FROM disease WHERE dtype = 'JensenLab Knowledge AmyCo' AND ncats_name IS NOT NULL AND ncats_name != ''"),
    ("JensenLab Knowledge MedlinePlus", "SELECT DISTINCT ncats_name AS disease_name FROM disease WHERE dtype = 'JensenLab Knowledge MedlinePlus' AND ncats_name IS NOT NULL AND ncats_name != ''"),
    ("JensenLab Knowledge UniProtKB-KW", "SELECT DISTINCT ncats_name AS disease_name FROM disease WHERE dtype = 'JensenLab Knowledge UniProtKB-KW' AND ncats_name IS NOT NULL AND ncats_name != ''"),
    ("JensenLab Text Mining", "SELECT DISTINCT ncats_name AS disease_name FROM disease WHERE dtype = 'JensenLab Text Mining' AND ncats_name IS NOT NULL AND ncats_name != ''"),
    ("MONDO", "SELECT DISTINCT name AS disease_name FROM mondo WHERE name IS NOT NULL AND name != ''"),
    ("RDAS", "SELECT DISTINCT name AS disease_name FROM ncats_disease WHERE gard_rare = 1 AND name IS NOT NULL AND name != ''"),
    ("TIN-X Data", """
        SELECT DISTINCT d.name AS disease_name
        FROM tinx_importance ti
        JOIN ncats_disease d ON d.id = ti.ncats_disease_id
        WHERE d.name IS NOT NULL AND d.name != ''
    """),
    ("UniProt Disease", "SELECT DISTINCT ncats_name AS disease_name FROM disease WHERE dtype = 'UniProt' AND ncats_name IS NOT NULL AND ncats_name != ''"),
]


LIGAND_SQL_RULES = [
    ("ChEMBL IDs", "SELECT DISTINCT id AS ncats_ligand_id FROM ncats_ligands WHERE ChEMBL IS NOT NULL AND ChEMBL != ''"),
    ("Guide to Pharmacology", "SELECT DISTINCT id AS ncats_ligand_id FROM ncats_ligands WHERE `Guide to Pharmacology` IS NOT NULL AND `Guide to Pharmacology` != ''"),
    ("PubChem CIDs", "SELECT DISTINCT id AS ncats_ligand_id FROM ncats_ligands WHERE PubChem IS NOT NULL AND PubChem != ''"),
]


GRAPH_ACTIVITY_LABELS = [
    "Guide to Pharmacology",
    "Drug Central - Scientific Literature",
    "Drug Central - Drug Label",
    "Drug Central - Kegg Drug",
    "Drug Central - GtoPdb",
    "Drug Central - ChEMBL",
]


class DataSourceMapAdapter(InputAdapter, MySqlAdapter):
    batch_size = 25_000
    version = DatasourceVersionInfo(
        version="pharos400-datasource-map-design",
        version_date=date.fromisoformat("2026-06-04"),
    )

    def __init__(
        self,
        credentials: DBCredentials,
        database_name: str,
        source_graph_credentials_path: Optional[str | dict] = None,
        source_graph_database: str = "pharos",
        include_graph_activity_rules: bool = True,
        include_chembl_activity_rule: bool = False,
    ):
        MySqlAdapter.__init__(self, credentials)
        self.update_database(database_name)
        self.source_graph_database = source_graph_database
        self.include_graph_activity_rules = include_graph_activity_rules
        self.include_chembl_activity_rule = include_chembl_activity_rule
        self.graph_adapter = self._build_graph_adapter(source_graph_credentials_path)
        self._source_protein_ifx_by_id: Optional[dict[int, str]] = None
        self._source_ligand_identifier_by_id: Optional[dict[int, str]] = None

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.PostProcessing

    def get_version(self) -> DatasourceVersionInfo:
        return self.version

    def _build_graph_adapter(self, credentials_path: Optional[str | dict]) -> Optional[ArangoAdapter]:
        if not credentials_path:
            return None
        if isinstance(credentials_path, dict):
            credentials = DBCredentials.from_yaml(credentials_path)
        else:
            with open(credentials_path, "r", encoding="utf-8") as stream:
                credentials = DBCredentials.from_yaml(yaml.safe_load(stream))
        return ArangoAdapter(credentials, self.source_graph_database)

    def _datasource_labels(self) -> list[str]:
        labels = set(DATASOURCE_URLS)
        for label, _ in PROTEIN_SQL_RULES + DISEASE_SQL_RULES + LIGAND_SQL_RULES:
            labels.add(label)
        for label in GRAPH_ACTIVITY_LABELS:
            labels.add(label)
        if self.include_chembl_activity_rule:
            labels.add("ChEMBL Activities")
        return sorted(labels)

    def _datasource_info(self, label: str) -> NcatsDataSourceInfo:
        return NcatsDataSourceInfo(
            dataSource=label,
            url=DATASOURCE_URLS.get(label),
        )

    def _entry(
        self,
        label: str,
        *,
        protein_id: Optional[int] = None,
        protein_ifx_id: Optional[str] = None,
        disease_name: Optional[str] = None,
        ncats_ligand_id: Optional[int] = None,
        ligand_identifier: Optional[str] = None,
    ) -> NcatsDataSourceMapEntry:
        return NcatsDataSourceMapEntry(
            dataSource=label,
            url=DATASOURCE_URLS.get(label),
            protein_id=protein_id,
            protein_ifx_id=protein_ifx_id,
            disease_name=disease_name,
            ncats_ligand_id=ncats_ligand_id,
            ligand_identifier=ligand_identifier,
        )

    def _source_protein_ifx_map(self) -> dict[int, str]:
        if self._source_protein_ifx_by_id is None:
            session = self.get_session()
            try:
                rows = session.execute(text("SELECT id, ifx_id FROM protein WHERE ifx_id IS NOT NULL")).fetchall()
                self._source_protein_ifx_by_id = {
                    int(row._mapping["id"]): row._mapping["ifx_id"]
                    for row in rows
                }
            finally:
                session.close()
        return self._source_protein_ifx_by_id

    def _source_ligand_identifier_map(self) -> dict[int, str]:
        if self._source_ligand_identifier_by_id is None:
            session = self.get_session()
            try:
                rows = session.execute(text("SELECT id, identifier FROM ncats_ligands WHERE identifier IS NOT NULL")).fetchall()
                self._source_ligand_identifier_by_id = {
                    int(row._mapping["id"]): row._mapping["identifier"]
                    for row in rows
                }
            finally:
                session.close()
        return self._source_ligand_identifier_by_id

    def _iter_sql_entries(self, label: str, sql: str, column: str) -> Iterable[NcatsDataSourceMapEntry]:
        session = self.get_session()
        try:
            result = session.execute(text(sql))
            while True:
                rows = result.fetchmany(self.batch_size)
                if not rows:
                    break
                for row in rows:
                    value = row._mapping[column]
                    if value is None:
                        continue
                    if column == "protein_id":
                        protein_ifx_id = self._source_protein_ifx_map().get(int(value))
                        if protein_ifx_id:
                            yield self._entry(label, protein_ifx_id=protein_ifx_id)
                    elif column == "ncats_ligand_id":
                        ligand_identifier = self._source_ligand_identifier_map().get(int(value))
                        if ligand_identifier:
                            yield self._entry(label, ligand_identifier=ligand_identifier)
                    elif column == "disease_name":
                        yield self._entry(label, disease_name=str(value))
        finally:
            session.close()

    @staticmethod
    def _chunks(values: list[str], size: int = 1000):
        for index in range(0, len(values), size):
            yield values[index:index + size]

    def _lookup_protein_ids(self, ifx_ids: set[str]) -> dict[str, int]:
        if not ifx_ids:
            return {}
        mapping: dict[str, int] = {}
        session = self.get_session()
        try:
            for chunk in self._chunks(sorted(ifx_ids)):
                params = {f"id_{i}": value for i, value in enumerate(chunk)}
                placeholders = ", ".join(f":id_{i}" for i in range(len(chunk)))
                sql = text(f"SELECT id, ifx_id FROM protein WHERE ifx_id IN ({placeholders})")
                for row in session.execute(sql, params):
                    mapping[row._mapping["ifx_id"]] = int(row._mapping["id"])
        finally:
            session.close()
        return mapping

    def _lookup_ligand_ids(self, identifiers: set[str]) -> dict[str, int]:
        if not identifiers:
            return {}
        mapping: dict[str, int] = {}
        session = self.get_session()
        try:
            for chunk in self._chunks(sorted(identifiers)):
                params = {f"id_{i}": value for i, value in enumerate(chunk)}
                placeholders = ", ".join(f":id_{i}" for i in range(len(chunk)))
                sql = text(f"SELECT id, identifier FROM ncats_ligands WHERE identifier IN ({placeholders})")
                for row in session.execute(sql, params):
                    mapping[row._mapping["identifier"]] = int(row._mapping["id"])
        finally:
            session.close()
        return mapping

    @staticmethod
    def _activity_query() -> str:
        return """
        FOR rel IN `ProteinLigandEdge`
            FOR detail IN (rel.details || [])
                LET labels = UNIQUE(FLATTEN([
                    detail.activity_source == "IUPHAR/BPS Guide to PHARMACOLOGY"
                        ? ["Guide to Pharmacology"]
                        : [],
                    detail.activity_source == "DrugCentral" && (
                        detail.act_source == "SCIENTIFIC LITERATURE"
                        OR detail.moa_source == "SCIENTIFIC LITERATURE"
                    ) ? ["Drug Central - Scientific Literature"] : [],
                    detail.activity_source == "DrugCentral" && (
                        detail.act_source == "DRUG LABEL"
                        OR detail.moa_source == "DRUG LABEL"
                    ) ? ["Drug Central - Drug Label"] : [],
                    detail.activity_source == "DrugCentral" && (
                        detail.act_source == "KEGG DRUG"
                        OR detail.moa_source == "KEGG DRUG"
                    ) ? ["Drug Central - Kegg Drug"] : [],
                    detail.activity_source == "DrugCentral" && (
                        detail.act_source == "IUPHAR"
                        OR detail.moa_source == "IUPHAR"
                    ) ? ["Drug Central - GtoPdb"] : [],
                    detail.activity_source == "DrugCentral" && (
                        detail.act_source == "CHEMBL"
                        OR detail.moa_source == "CHEMBL"
                    ) ? ["Drug Central - ChEMBL"] : []
                ]))
                FOR label IN labels
                    COLLECT dataSource = label, protein = rel.start_id, ligand = rel.end_id
                    RETURN {dataSource, protein, ligand}
        """

    def _iter_graph_activity_entries(self):
        if self.graph_adapter is None:
            return
        rows = list(self.graph_adapter.runQuery(self._activity_query()))
        protein_ids = {row.get("protein") for row in rows if row.get("protein")}
        ligand_ids = {row.get("ligand") for row in rows if row.get("ligand")}
        protein_map = self._lookup_protein_ids(protein_ids)
        ligand_map = self._lookup_ligand_ids(ligand_ids)
        seen_proteins = set()
        seen_ligands = set()
        for row in rows:
            label = row.get("dataSource")
            if label not in GRAPH_ACTIVITY_LABELS:
                continue
            protein_ifx_id = row.get("protein")
            protein_key = (label, protein_ifx_id)
            if protein_ifx_id in protein_map and protein_key not in seen_proteins:
                seen_proteins.add(protein_key)
                yield self._entry(label, protein_ifx_id=protein_ifx_id)
            ligand_identifier = row.get("ligand")
            ligand_key = (label, ligand_identifier)
            if ligand_identifier in ligand_map and ligand_key not in seen_ligands:
                seen_ligands.add(ligand_key)
                yield self._entry(label, ligand_identifier=ligand_identifier)

    def _iter_chembl_activity_entries(self):
        if self.graph_adapter is None:
            return
        query = """
        FOR rel IN `ProteinLigandEdge`
            FOR detail IN (rel.details || [])
                FILTER detail.activity_source == "ChEMBL"
                COLLECT protein = rel.start_id, ligand = rel.end_id
                RETURN {protein, ligand}
        """
        rows = list(self.graph_adapter.runQuery(query))
        protein_ifx_ids = {row.get("protein") for row in rows if row.get("protein")}
        ligand_identifiers = {row.get("ligand") for row in rows if row.get("ligand")}
        known_proteins = set(self._lookup_protein_ids(protein_ifx_ids))
        known_ligands = set(self._lookup_ligand_ids(ligand_identifiers))

        for protein_ifx_id in sorted(known_proteins):
            yield self._entry("ChEMBL Activities", protein_ifx_id=protein_ifx_id)
        for ligand_identifier in sorted(known_ligands):
            yield self._entry("ChEMBL Activities", ligand_identifier=ligand_identifier)

    def _yield_batches(self, entries: Iterable[NcatsDataSourceInfo | NcatsDataSourceMapEntry]):
        batch = []
        for entry in entries:
            batch.append(entry)
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _all_entries(self) -> Iterable[NcatsDataSourceInfo | NcatsDataSourceMapEntry]:
        for label in self._datasource_labels():
            yield self._datasource_info(label)

        for label, sql in PROTEIN_SQL_RULES:
            yield from self._iter_sql_entries(label, sql, "protein_id")
        for label, sql in DISEASE_SQL_RULES:
            yield from self._iter_sql_entries(label, sql, "disease_name")
        for label, sql in LIGAND_SQL_RULES:
            yield from self._iter_sql_entries(label, sql, "ncats_ligand_id")

        if self.include_graph_activity_rules:
            yield from self._iter_graph_activity_entries()

        if self.include_chembl_activity_rule:
            yield from self._iter_chembl_activity_entries()

    def get_all(self) -> Generator[list[NcatsDataSourceInfo | NcatsDataSourceMapEntry], None, None]:
        yield from self._yield_batches(self._all_entries())

    def get_resolved_and_provenanced_list(self, resolver_map: dict[str, Any]):
        for entries in self.get_all():
            version_info = self.get_version()
            version_data = [
                self.get_datasource_name(),
                version_info.version,
                version_info.version_date,
                version_info.download_date,
            ]
            version_string = "\t".join([str(e) for e in version_data])
            for entry in entries:
                if not getattr(entry, "provenance", None):
                    entry.provenance = version_string
            yield entries
