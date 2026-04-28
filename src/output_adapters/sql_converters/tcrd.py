import hashlib
from typing import Union, List, Optional
from src.constants import Prefix
from src.models.disease import Disease, DiseaseParentEdge, DODiseaseParentEdge, ProteinDiseaseEdge, TINXImportanceEdge
from src.models.dto_class import DTOClass, DTOClassParentEdge, ProteinDTOClassEdge
from src.models.expression import ProteinTissueExpressionEdge
from src.models.generif import GeneGeneRifEdge
from src.models.go_term import GoType, GoTerm, GoTermHasParent, ProteinGoTermEdge
from src.models.keyword import ProteinKeywordEdge
from src.models.ligand import Ligand, ProteinLigandEdge
from src.models.node import EquivalentId
from src.models.panther_class import PantherClass, ProteinPantherClassEdge
from src.models.pathway import ProteinPathwayEdge
from src.models.ppi import PPIEdge
from src.models.protein import Protein
from src.models.tiga import ProteinGwasTraitEdge, GwasTrait
from src.models.tcrd_disease_ontology import MondoTerm, MondoTermParentEdge, DOTerm, DOTermParentEdge
from src.models.tissue import Tissue, TissueParentEdge
from src.shared.sqlalchemy_tables.pharos_tables_new import (
    Protein as mysqlProtein, Xref, Alias, Target, TDL_info, T2TC, GO, GOParent, GoA,
    GeneRif, GeneRif2Pubmed, Protein2Pubmed, Ligand as mysqlLigand, LigandActivity,
    Uberon, UberonParent, Tissue as mysqlTissue, Expression, Gtex,
    Mondo, MondoParent, MondoXref, Disease as mysqlDisease, DiseaseType, DO, DOParent,
    NcatsDisease, NcatsD2DA, Pathway as mysqlPathway, PantherClass as mysqlPantherClass, P2PC, PPI as mysqlPPI,
    Tiga as mysqlTiga, TigaProvenance, TinxImportance,
    DTO as mysqlDTO, DTOParent, P2DTO, Pmscore,
)
from src.output_adapters.sql_converters.output_converter_base import SQLOutputConverter
from src.shared.sqlalchemy_tables.pharos_tables_new import Base as TCRDBase


class TCRDOutputConverter(SQLOutputConverter):

    def __init__(self):
        super().__init__(sql_base=TCRDBase)
        self._known_disease_types: set = set()
        self._disease_name_by_id: dict[str, str] = {}
        self._known_mondo_ids: set[str] = set()
        self._seen_protein2pubmed: set[tuple[int, str, int | None, str]] = set()
        self._converters = {
            # Protein
            Protein: [self.protein_converter, self.target_converter, self.t2tc_converter,
                      self.protein_alias_converter, self.protein_xref_converter, self.tdl_info_converter,
                      self.pmscore_converter],
            # GeneRif
            GeneGeneRifEdge: [self.generif_converter, self.generif_assoc_converter, self.p2p_converter],
            # GO
            GoTerm: [self.goterm_converter],
            GoTermHasParent: [self.goterm_parent_converter],
            ProteinGoTermEdge: [self.goa_converter],
            # Ligand
            Ligand: [self.ligand_converter],
            ProteinLigandEdge: [self.ligand_edge_converter],
            # Tissue / Expression
            Tissue: [self.uberon_converter],
            TissueParentEdge: [self.uberon_parent_converter],
            ProteinTissueExpressionEdge: [self.tissue_lookup_converter,
                                          self.expression_converter,
                                          self.gtex_converter],
            MondoTerm: [self.mondo_table_converter, self.mondo_xref_converter],
            MondoTermParentEdge: [self.mondo_parent_table_converter],
            DOTerm: [self.do_table_converter],
            DOTermParentEdge: [self.do_parent_table_converter],
            # Disease
            Disease: [self.ncats_disease_converter],
            DiseaseParentEdge: None,
            DODiseaseParentEdge: None,
            ProteinDiseaseEdge: [self.disease_type_converter, self.disease_converter, self.ncats_d2da_converter],
            TINXImportanceEdge: [self.tinx_importance_converter],
            # TIGA
            GwasTrait: None,
            ProteinGwasTraitEdge: [self.tiga_converter, self.tiga_provenance_converter],
            # Pathway
            ProteinPathwayEdge: [self.pathway_converter],
            # PPI
            PPIEdge: [self.ppi_converter],
            # Panther
            PantherClass: [self.panther_class_converter],
            ProteinPantherClassEdge: [self.p2pc_converter],
            # DTO
            DTOClass: [self.dto_converter],
            DTOClassParentEdge: [self.dto_parent_converter],
            ProteinDTOClassEdge: [self.p2dto_converter],
            # Keyword
            ProteinKeywordEdge: [self.keyword_xref_converter],
        }

    def get_object_converters(self, obj_cls) -> Union[callable, List[callable], None]:
        return self._converters.get(obj_cls)

    def get_preload_queries(self, session):
        return [{
            "table": 'protein',
            "data": session.query(mysqlProtein.id, mysqlProtein.ifx_id).all()
        }, {
            "table": 'ligand',
            "data": session.query(mysqlLigand.id, mysqlLigand.identifier).all()
        }]

    def preload_id_mappings(self, session):
        super().preload_id_mappings(session)
        self._known_disease_types = {
            row[0] for row in session.query(DiseaseType.name).all() if row[0]
        }
        self._known_mondo_ids = {
            row[0] for row in session.query(Mondo.mondoid).all() if row[0]
        }

    # --- Protein ---

    def protein_converter(self, obj: dict) -> mysqlProtein:
        gene_ids = [p for p in obj['xref'] if p.startswith('NCBIGene:')]
        gene_id = gene_ids[0].split(':')[-1] if gene_ids else None
        ensembl_ids = [p for p in obj['xref'] if p.startswith('ENSEMBL:ENSP')]
        string_id = ensembl_ids[0].split(':')[-1] if ensembl_ids and gene_id is None else None
        return mysqlProtein(
            id=self.resolve_id('protein', obj['id']),
            ifx_id=obj['id'],
            name=obj.get('gene_name'),
            description=obj.get('name'),
            uniprot=obj['uniprot_id'],
            sym=obj.get('symbol'),
            geneid=gene_id,
            stringid=string_id,
            seq=obj['sequence'],
            dtoid=(obj.get('dtoid') or '').replace(':', '_') or None,
            dtoclass=obj.get('dtoclass'),
            provenance=obj['provenance'],
            preferred_symbol=obj.get('preferred_symbol'),
            novelty=min(obj.get('novelty') or []) if obj.get('novelty') else None,
        )

    def tdl_info_converter(self, obj: dict) -> List[TDL_info]:
        tdl_infos = []
        uniprot_description = obj.get('uniprot_function')
        if uniprot_description:
            tdl_infos.append(TDL_info(
                itype="UniProt Function",
                protein_id=self.resolve_id('protein', obj['id']),
                string_value=uniprot_description
            ))
        if obj.get('antibody_count') and len(obj['antibody_count']) > 0:
            antibody_count = max([int(p) for p in obj['antibody_count']])
            if antibody_count > 0:
                tdl_infos.append(TDL_info(
                    itype="Ab Count",
                    protein_id=self.resolve_id('protein', obj['id']),
                    integer_value=antibody_count
                ))
        if obj.get('pm_score') and len(obj['pm_score']) > 0:
            pm_score = max([float(p) for p in obj['pm_score']])
            tdl_infos.append(TDL_info(
                itype="JensenLab PubMed Score",
                protein_id=self.resolve_id('protein', obj['id']),
                number_value=pm_score
            ))
        return tdl_infos

    def protein_alias_converter(self, obj: dict) -> List[Alias]:
        protein_id = self.resolve_id('protein', obj['id'])
        ids = [EquivalentId.parse(x) for x in obj['xref']]
        aliases = []
        for s in [x for x in ids if x.type in (Prefix.OldSymbol, Prefix.Symbol)]:
            aliases.append(Alias(protein_id=protein_id, type='symbol', value=s.id))
        for u in [x for x in ids if x.type == Prefix.UniProtKB]:
            aliases.append(Alias(protein_id=protein_id, type='uniprot', value=u.id))
        for n in [x for x in ids if x.type == Prefix.NCBIGene]:
            aliases.append(Alias(protein_id=protein_id, type='NCBI Gene ID', value=n.id))
        return aliases

    def protein_xref_converter(self, obj: dict) -> List[Xref]:
        protein_id = self.resolve_id('protein', obj['id'])
        ids = [EquivalentId.parse(x) for x in obj['xref']]
        return [
            Xref(
                protein_id=protein_id,
                xtype="Ensembl" if a.type == Prefix.ENSEMBL else a.type.value,
                value=a.id
            )
            for a in ids
        ]

    def pmscore_converter(self, obj: dict) -> List[Pmscore]:
        protein_id = self.resolve_id('protein', obj['id'])
        rows = []
        for entry in obj.get('pm_score_by_year') or []:
            year = entry.get('year')
            score = entry.get('score')
            if year is None or score is None:
                continue
            rows.append(
                Pmscore(
                    protein_id=protein_id,
                    year=int(year),
                    score=float(score),
                )
            )
        return rows

    def t2tc_converter(self, obj: dict) -> T2TC:
        return T2TC(
            target_id=self.resolve_id('protein', obj['id']),
            protein_id=self.resolve_id('protein', obj['id']),
            provenance=obj['provenance'])

    def target_converter(self, obj: dict) -> Target:
        return Target(
            id=self.resolve_id("protein", obj['id']),
            ifx_id=obj['id'],
            name=obj['name'],
            ttype='Single Protein',
            fam=obj['idg_family'],
            tdl=obj['tdl'],
            provenance=obj['provenance']
        )

    # --- GeneRif ---

    def _generif_hash(self, obj: dict) -> str:
        data_to_hash = f"{obj['start_id']}-{obj['gene_id']}-{obj['date']}-{obj['end_node']['text']}"
        return hashlib.sha256(data_to_hash.encode('utf-8')).hexdigest()

    def generif_converter(self, obj: dict) -> GeneRif:
        return GeneRif(
            id=self._generif_hash(obj),
            protein_id=self.resolve_id('protein', obj['start_id']),
            gene_id=obj['gene_id'],
            date=obj['date'],
            text=obj['end_node']['text'],
            provenance=obj['provenance']
        )

    def generif_assoc_converter(self, obj: dict) -> List[GeneRif2Pubmed]:
        hashed_id = self._generif_hash(obj)
        return [
            GeneRif2Pubmed(generif_id=hashed_id, pubmed_id=pmid, provenance=obj['provenance'])
            for pmid in obj['pmids']
        ]

    def p2p_converter(self, obj: dict) -> List[Protein2Pubmed]:
        protein_id = self.resolve_id('protein', obj['start_id'])
        rows = []
        for pmid in obj['pmids']:
            key = (protein_id, str(pmid), obj['gene_id'], 'NCBI')
            if key in self._seen_protein2pubmed:
                continue
            self._seen_protein2pubmed.add(key)
            rows.append(
                Protein2Pubmed(
                    protein_id=protein_id,
                    pubmed_id=pmid,
                    gene_id=obj['gene_id'],
                    source='NCBI',
                    provenance=obj['provenance']
                )
            )
        return rows

    # --- GO ---

    def goterm_converter(self, obj: dict) -> GO:
        def get_namespace(obj: dict):
            if obj['type'] == GoType.Process:
                return 'biological_process'
            if obj['type'] == GoType.Function:
                return 'molecular_function'
            if obj['type'] == GoType.Component:
                return 'cellular_component'
            return 'unknown'
        return GO(
            go_id=obj['id'],
            name=obj['term'],
            namespace=get_namespace(obj),
            def_=obj['definition'],
            provenance=obj['provenance'])

    def goterm_parent_converter(self, obj: dict) -> GOParent:
        return GOParent(
            go_id=obj['start_id'],
            parent_id=obj['end_id'],
            provenance=obj['provenance'])

    def goa_converter(self, obj: dict) -> List[GoA]:
        return [
            GoA(
                protein_id=self.resolve_id('protein', obj['start_id']),
                go_id=obj['end_id'],
                evidence=e['abbreviation'],
                goeco=e['evidence'],
                assigned_by=e['assigned_by'],
                go_term=f"{obj['end_node']['type']}:{obj['end_node']['term']}",
                go_type={'C': 'Component', 'P': 'Process', 'F': 'Function'}.get(obj['end_node']['type'], obj['end_node']['type']),
                go_term_text=obj['end_node']['term'],
                provenance=obj['provenance']
            )
            for e in obj['evidence']]

    # --- Ligand ---

    def ligand_converter(self, obj: dict) -> mysqlLigand:
        xrefs = [EquivalentId.parse(eq_id) for eq_id in (obj.get('xref') or [])]
        ligand_identifier = obj.get('id')
        return mysqlLigand(
            id=self.resolve_id('ligand', ligand_identifier),
            identifier=ligand_identifier,
            name=obj.get('name'),
            isDrug=obj.get('isDrug', False),
            smiles=obj.get('smiles'),
            description=obj.get('description'),
            PubChem=",".join(x.id for x in xrefs if x.type == Prefix.PUBCHEM_COMPOUND),
            ChEMBL=",".join(x.id for x in xrefs if x.type == Prefix.CHEMBL_COMPOUND),
            guide_to_pharmacology=",".join(x.id for x in xrefs if x.type == Prefix.GTOPDB),
            DrugCentral=",".join(x.id for x in xrefs if x.type == Prefix.DrugCentral),
            unii=",".join(x.id for x in xrefs if x.type == Prefix.UNII),
            provenance=obj['provenance']
        )

    def ligand_edge_converter(self, obj: dict) -> List[LigandActivity]:
        activity_objects = []
        ligand_id = self.resolve_id('ligand', obj['end_id'])
        for detail in obj.get('details', []):
            pubmed_ids = list(detail.get('act_pmids') or [])
            if detail.get('moa_pmid'):
                pubmed_ids.append(detail['moa_pmid'])
            activity_objects.append(LigandActivity(
                ncats_ligand_id=ligand_id,
                target_id=self.resolve_id('protein', obj['start_id']),
                act_value=detail.get('act_value'),
                act_type=detail.get('act_type'),
                action_type=detail.get('action_type'),
                reference=detail.get('reference'),
                pubmed_ids="|".join(list(set(str(i) for i in pubmed_ids))),
                provenance=obj['provenance']
            ))
        return activity_objects

    # --- Tissue / Uberon ---

    def uberon_converter(self, obj: dict) -> Uberon:
        return Uberon(
            uid=obj['id'],
            name=obj['name'],
            def_=obj.get('definition'),
            provenance=obj['provenance'],
        )

    def uberon_parent_converter(self, obj: dict) -> UberonParent:
        return UberonParent(
            uid=obj['start_id'],
            parent_id=obj['end_id'],
            provenance=obj['provenance'],
        )

    def tissue_lookup_converter(self, obj: dict) -> List[mysqlTissue]:
        """Populates the tissue name→id lookup table; one row per new unique tissue name."""
        rows = []
        for detail in obj.get('details', []):
            if detail.get('source') == 'GTEx':
                continue
            tissue_name = detail.get('tissue')
            if not tissue_name:
                continue
            is_new = tissue_name not in self.id_mapping.get('tissue', {})
            tissue_id = self.resolve_id('tissue', tissue_name)
            if is_new:
                rows.append(mysqlTissue(id=tissue_id, name=tissue_name))
        return rows

    def expression_converter(self, obj: dict) -> List[Expression]:
        protein_id = self.resolve_id('protein', obj['start_id'])
        rows = []
        for detail in obj.get('details', []):
            if detail.get('source') == 'GTEx':
                continue
            tissue_name = detail.get('tissue') or ''
            tissue_id = self.resolve_id('tissue', tissue_name) if tissue_name else None
            rows.append(Expression(
                etype=detail['source'],
                protein_id=protein_id,
                source_id=detail.get('source_id') or '',
                tissue=tissue_name,
                tissue_id=tissue_id,
                qual_value=detail.get('qual_value'),
                number_value=detail.get('number_value'),
                expressed=1 if detail.get('expressed') else 0,
                source_rank=detail.get('source_rank'),
                evidence=detail.get('evidence'),
                uberon_id=detail.get('source_tissue_id'),
                provenance=obj['provenance'],
            ))
        return rows

    def gtex_converter(self, obj: dict) -> List[Gtex]:
        """One Gtex row per protein-tissue pair, with tpm/tpm_male/tpm_female from sex-split details."""
        protein_id = self.resolve_id('protein', obj['start_id'])
        gtex_details = [d for d in obj.get('details', []) if d.get('source') == 'GTEx']
        if not gtex_details:
            return []

        # Group by source_tissue_id (uberon id of the gtex tissue)
        by_tissue: dict = {}
        for detail in gtex_details:
            tissue_id = detail.get('source_tissue_id') or detail.get('tissue') or ''
            if tissue_id not in by_tissue:
                by_tissue[tissue_id] = {
                    'tissue': detail.get('tissue'),
                    'uberon_id': detail.get('source_tissue_id'),
                    'tpm': None, 'tpm_rank': None,
                    'tpm_male': None, 'tpm_male_rank': None,
                    'tpm_female': None, 'tpm_female_rank': None,
                }
            sex = detail.get('sex')
            if sex == 'male':
                by_tissue[tissue_id]['tpm_male'] = detail.get('number_value')
                by_tissue[tissue_id]['tpm_male_rank'] = detail.get('source_rank')
            elif sex == 'female':
                by_tissue[tissue_id]['tpm_female'] = detail.get('number_value')
                by_tissue[tissue_id]['tpm_female_rank'] = detail.get('source_rank')
            else:
                by_tissue[tissue_id]['tpm'] = detail.get('number_value')
                by_tissue[tissue_id]['tpm_rank'] = detail.get('source_rank')

        return [
            Gtex(
                protein_id=protein_id,
                tissue=d['tissue'],
                tpm=d['tpm'],
                tpm_rank=d['tpm_rank'],
                tpm_male=d['tpm_male'],
                tpm_male_rank=d['tpm_male_rank'],
                tpm_female=d['tpm_female'],
                tpm_female_rank=d['tpm_female_rank'],
                uberon_id=d['uberon_id'],
                provenance=obj['provenance'],
            )
            for d in by_tissue.values()
        ]

    # --- Disease / MONDO ---

    def mondo_table_converter(self, obj: dict) -> Mondo:
        mondoid = obj['id']
        self._known_mondo_ids.add(mondoid)
        return Mondo(
            mondoid=mondoid,
            name=obj.get('name') or mondoid,
            def_=obj.get('mondo_description'),
            comment=obj.get('comment'),
            provenance=obj['provenance'],
        )

    def mondo_xref_converter(self, obj: dict) -> List[MondoXref]:
        rows = []
        seen = set()
        exact_matches = set(obj.get('exact_matches') or [])
        for xref in obj.get('mondo_xrefs') or []:
            if not xref or ':' not in xref or xref in seen:
                continue
            seen.add(xref)
            db, value = xref.split(':', 1)
            rows.append(MondoXref(
                mondoid=obj['id'],
                db=db,
                value=value,
                equiv_to=(xref in exact_matches),
                xref=xref,
                provenance=obj['provenance'],
            ))
        return rows

    def mondo_parent_table_converter(self, obj: dict) -> MondoParent:
        return MondoParent(
            mondoid=obj['start_id'],
            parentid=obj['end_id'],
            provenance=obj['provenance'],
        )

    def do_table_converter(self, obj: dict) -> DO:
        return DO(
            doid=obj['id'],
            name=obj.get('name') or obj['id'],
            def_=obj.get('do_description'),
            provenance=obj['provenance'],
        )

    def do_parent_table_converter(self, obj: dict) -> DOParent:
        return DOParent(
            doid=obj['start_id'],
            parent_id=obj['end_id'],
            provenance=obj['provenance'],
        )

    def mondo_converter(self, obj: dict) -> Optional[Mondo]:
        if not (obj.get('id') or '').startswith('MONDO:'):
            return None
        comments = obj.get('comments') or []
        mondoid = obj['id']
        self._known_mondo_ids.add(mondoid)
        return Mondo(
            mondoid=mondoid,
            name=obj['name'],
            def_=obj.get('mondo_description'),
            comment=comments[0] if comments else None,
            provenance=obj['provenance'],
        )

    def do_converter(self, obj: dict) -> Optional[DO]:
        doid = obj.get('id') or ''
        if not doid.startswith('DOID:'):
            return None
        return DO(
            doid=doid,
            name=obj.get('name') or '',
            def_=obj.get('do_description'),
        )

    def do_parent_converter(self, obj: dict) -> Optional[DOParent]:
        child_id = obj.get('start_id') or ''
        parent_id = obj.get('end_id') or ''
        if not child_id.startswith('DOID:') or not parent_id.startswith('DOID:'):
            return None
        return DOParent(
            doid=child_id,
            parent_id=parent_id,
        )

    def mondo_parent_converter(self, obj: dict) -> MondoParent:
        return MondoParent(
            mondoid=obj['start_id'],
            parentid=obj['end_id'],
            provenance=obj['provenance'],
        )

    def disease_type_converter(self, obj: dict) -> Optional[DiseaseType]:
        """Seeds the disease_type lookup table; one row per new unique source."""
        details = obj.get('details') or []
        source = obj.get('source') or (details[0].get('source') if details else None)
        if not source or source in self._known_disease_types:
            return None
        self._known_disease_types.add(source)
        return DiseaseType(name=source, provenance=obj['provenance'])

    @staticmethod
    def _disease_assoc_key(protein_id: str, disease_id: str, detail: dict, ordinal: int) -> str:
        evidence_terms = "|".join(sorted(detail.get('evidence_terms') or []))
        pmids = "|".join(sorted(str(p) for p in (detail.get('pmids') or [])))
        evidence_codes = "|".join(sorted(detail.get('evidence_codes') or []))
        return "\t".join([
            protein_id or '',
            disease_id or '',
            detail.get('source') or '',
            detail.get('source_id') or '',
            evidence_terms,
            pmids,
            evidence_codes,
            str(ordinal),
        ])

    def _iter_disease_details(self, obj: dict):
        details = obj.get('details') or []
        if details:
            return details
        source = obj.get('source')
        if source:
            return [{
                'source': source,
                'source_id': obj.get('source_id'),
                'evidence_terms': obj.get('evidence_terms') or [],
                'pmids': obj.get('pmids') or [],
                'evidence_codes': obj.get('evidence_codes') or [],
            }]
        return []

    def _disease_name(self, obj: dict) -> str:
        end_id = obj['end_id']
        end_name = (obj.get('end_node') or {}).get('name')
        return end_name or self._disease_name_by_id.get(end_id) or end_id

    def ncats_disease_converter(self, obj: dict) -> NcatsDisease:
        disease_id = obj['id']
        self._disease_name_by_id[disease_id] = obj.get('name') or disease_id
        mondoid = disease_id if disease_id.startswith('MONDO:') and disease_id in self._known_mondo_ids else None
        return NcatsDisease(
            id=self.resolve_id('ncats_disease', disease_id),
            name=obj.get('name') or disease_id,
            uniprot_description=obj.get('uniprot_description'),
            do_description=obj.get('do_description'),
            mondo_description=obj.get('mondo_description'),
            mondoid=mondoid,
            novelty=min(obj.get('novelty') or []) if obj.get('novelty') else None,
            provenance=obj['provenance'],
        )

    def disease_converter(self, obj: dict) -> List[mysqlDisease]:
        resolved_disease_id = obj['end_id']
        mondoid = (
            resolved_disease_id
            if resolved_disease_id and resolved_disease_id.startswith('MONDO:') and resolved_disease_id in self._known_mondo_ids
            else None
        )
        disease_name = self._disease_name(obj)
        rows = []
        for ordinal, detail in enumerate(self._iter_disease_details(obj)):
            assoc_key = self._disease_assoc_key(obj['start_id'], resolved_disease_id, detail, ordinal)
            rows.append(mysqlDisease(
                id=self.resolve_id('disease_assoc', assoc_key),
                dtype=detail.get('source') or '',
                protein_id=self.resolve_id('protein', obj['start_id']),
                name=disease_name,
                ncats_name=disease_name,
                did=detail.get('source_id'),
                evidence="|".join(detail.get('evidence_terms') or detail.get('evidence_codes') or []) or None,
                zscore=detail.get('zscore'),
                conf=detail.get('confidence'),
                reference=detail.get('url'),
                drug_name=detail.get('drug_name'),
                mondoid=mondoid,
                provenance=obj['provenance'],
            ))
        return rows

    def ncats_d2da_converter(self, obj: dict) -> List[NcatsD2DA]:
        resolved_disease_id = obj['end_id']
        ncats_disease_id = self.resolve_id('ncats_disease', resolved_disease_id)
        links = []
        for ordinal, detail in enumerate(self._iter_disease_details(obj)):
            assoc_key = self._disease_assoc_key(obj['start_id'], resolved_disease_id, detail, ordinal)
            links.append(NcatsD2DA(
                ncats_disease_id=ncats_disease_id,
                disease_assoc_id=self.resolve_id('disease_assoc', assoc_key),
                direct=1,
                provenance=obj['provenance'],
            ))
        return links

    def tinx_importance_converter(self, obj: dict) -> List[TinxImportance]:
        best_doid = None
        best_score = None
        for detail in self._iter_disease_details(obj):
            importance = detail.get('importance') or []
            doid = detail.get('doid') or detail.get('source_id')
            if not importance:
                continue
            score = max(importance)
            if best_score is None or score > best_score:
                best_score = score
                best_doid = doid
        if best_score is None:
            return []
        return [TinxImportance(
            ncats_disease_id=self.resolve_id('ncats_disease', obj['end_id']),
            protein_id=self.resolve_id('protein', obj['start_id']),
            doid=best_doid,
            score=best_score,
        )]

    # --- TIGA ---

    def tiga_converter(self, obj: dict) -> List[mysqlTiga]:
        trait = obj.get('end_node') or {}
        if not trait:
            return []
        rows = []
        disease_ids = obj.get('disease_ids') or [None]
        for detail in obj.get('details') or []:
            for disease_id in disease_ids:
                rows.append(mysqlTiga(
                    protein_id=self.resolve_id('protein', obj['start_id']),
                    ensg=detail.get('ensg') or '',
                    efoid=obj.get('end_id') or '',
                    trait=trait.get('name') or '',
                    n_study=detail.get('n_study'),
                    n_snp=detail.get('n_snp'),
                    n_snpw=detail.get('n_snpw'),
                    geneNtrait=detail.get('geneNtrait'),
                    geneNstudy=detail.get('geneNstudy'),
                    traitNgene=detail.get('traitNgene'),
                    traitNstudy=detail.get('traitNstudy'),
                    pvalue_mlog_median=detail.get('pvalue_mlog_median'),
                    pvalue_mlog_max=detail.get('pvalue_mlog_max'),
                    or_median=detail.get('or_median'),
                    n_beta=detail.get('n_beta'),
                    study_N_mean=int(detail['study_N_mean']) if detail.get('study_N_mean') is not None else None,
                    rcras=detail.get('rcras'),
                    meanRank=detail.get('meanRank'),
                    meanRankScore=detail.get('meanRankScore'),
                    ncats_disease_id=self.resolve_id('ncats_disease', disease_id) if disease_id else None,
                    provenance=obj['provenance'],
                ))
        return rows

    def tiga_provenance_converter(self, obj: dict) -> List[TigaProvenance]:
        if not obj.get('end_node'):
            return []
        rows = []
        for edge_detail in obj.get('details') or []:
            for prov_detail in edge_detail.get('provenance_details') or []:
                study_acc = prov_detail.get('study_acc')
                pubmedid = prov_detail.get('pubmedid')
                if not study_acc or pubmedid is None:
                    continue
                rows.append(TigaProvenance(
                    ensg=edge_detail.get('ensg') or '',
                    efoid=obj.get('end_id') or '',
                    study_acc=study_acc,
                    pubmedid=int(pubmedid),
                    provenance=obj['provenance'],
                ))
        return rows

    # --- Pathway ---

    def pathway_converter(self, obj: dict) -> mysqlPathway:
        protein_id = self.resolve_id('protein', obj['start_id'])
        end = obj['end_node']
        return mysqlPathway(
            target_id=protein_id,
            protein_id=protein_id,
            pwtype=end.get('type'),
            id_in_source=end.get('source_id'),
            name=end.get('name') or '',
            url=end.get('url'),
            provenance=obj['provenance'],
        )

    # --- PPI ---

    @staticmethod
    def _ppi_source_label(source: str) -> str:
        if not source:
            return source
        label = source.split('\t', 1)[0]
        return {'STRING': 'StringDB'}.get(label, label)

    @staticmethod
    def _max_or_value(value):
        if isinstance(value, list):
            return max(value) if value else None
        return value

    @staticmethod
    def _join_list(value):
        if isinstance(value, list):
            normalized = [str(v) for v in value if v is not None and str(v) != ""]
            return "|".join(dict.fromkeys(normalized)) if normalized else None
        return value

    def ppi_converter(self, obj: dict) -> List[mysqlPPI]:
        protein_id = self.resolve_id('protein', obj['start_id'])
        other_id = self.resolve_id('protein', obj['end_id'])

        source_labels = sorted({
            self._ppi_source_label(source)
            for source in (obj.get('sources') or [])
            if source
        })

        shared = dict(
            ppitypes=",".join(source_labels) if source_labels else self._ppi_source_label(obj['provenance']),
            p_int=self._max_or_value(obj.get('p_int')),
            p_ni=self._max_or_value(obj.get('p_ni')),
            p_wrong=self._max_or_value(obj.get('p_wrong')),
            evidence=self._join_list(obj.get('pmids')) or obj.get('evidence'),
            interaction_type=self._join_list(obj.get('interaction_type')),
            score=self._max_or_value(obj.get('score')),
        )

        return [
            mysqlPPI(
                protein_id=protein_id,
                other_id=other_id,
                **shared,
            ),
            mysqlPPI(
                protein_id=other_id,
                other_id=protein_id,
                **shared,
            ),
        ]

    # --- Panther ---

    def panther_class_converter(self, obj: dict) -> mysqlPantherClass:
        return mysqlPantherClass(
            id=self.resolve_id('panther_class', obj['id']),
            pcid=obj.get('source_id') or obj['id'],
            parent_pcids=obj.get('parent_pcids'),
            name=obj.get('name') or '',
            description=obj.get('description'),
        )

    def p2pc_converter(self, obj: dict) -> P2PC:
        return P2PC(
            panther_class_id=self.resolve_id('panther_class', obj['end_id']),
            protein_id=self.resolve_id('protein', obj['start_id']),
        )

    # --- DTO ---

    def dto_converter(self, obj: dict) -> mysqlDTO:
        return mysqlDTO(
            dtoid=obj['id'],
            name=obj.get('name') or '',
            parent_id=None,
            def_=obj.get('description'),
        )

    def dto_parent_converter(self, obj: dict) -> DTOParent:
        return DTOParent(
            dtoid=obj['start_id'],
            parent_id=obj['end_id'],
        )

    def p2dto_converter(self, obj: dict) -> P2DTO:
        return P2DTO(
            dtoid=obj['end_id'],
            protein_id=self.resolve_id('protein', obj['start_id']),
        )

    # --- Keyword ---

    def keyword_xref_converter(self, obj: dict) -> Xref:
        end = obj['end_node']
        return Xref(
            protein_id=self.resolve_id('protein', obj['start_id']),
            xtype='UniProt Keyword',
            value=end.get('source_id') or end.get('value') or end.get('name') or '',
            xtra=end.get('value') or end.get('name'),
            provenance=obj['provenance'],
        )
