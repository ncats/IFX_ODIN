import hashlib
from typing import Union, List

from src.constants import Prefix
from src.models.generif import GeneGeneRifRelationship
from src.models.ligand import Ligand, ProteinLigandRelationship
from src.models.protein import Protein
from src.shared.sqlalchemy_tables.pharos_tables_new import Protein as mysqlProtein, Xref, Alias, Target, TDL_info, T2TC, GO, GOParent, GoA, \
    GeneRif, GeneRif2Pubmed, Protein2Pubmed, Ligand as mysqlLigand, LigandActivity
from src.models.go_term import GoType, GoTerm, GoTermHasParent, ProteinGoTermRelationship
from src.models.node import EquivalentId
from src.output_adapters.sql_converters.output_converter_base import SQLOutputConverter

class TCRDOutputConverter(SQLOutputConverter):

    def get_object_converters(self, obj_cls) -> Union[callable, List[callable], None]:
        if obj_cls == GoTerm:
            return self.goterm_converter
        if obj_cls == GoTermHasParent:
            return self.goterm_parent_converter
        if obj_cls == Protein:
            return [self.protein_converter, self.target_converter, self.t2tc_converter,
                    self.protein_alias_converter, self.protein_xref_converter, self.tdl_info_converter]
        if obj_cls == ProteinGoTermRelationship:
            return self.goa_converter
        if obj_cls == GeneGeneRifRelationship:
            return [self.generif_converter, self.generif_assoc_converter, self.p2p_converter]
        if obj_cls == Ligand:
            return self.ligand_converter
        if obj_cls == ProteinLigandRelationship:
            return self.ligand_edge_converter
        return None

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
            provenance = obj['provenance'])


    def goterm_parent_converter(self, obj: dict) -> GOParent:
        return GOParent(
            go_id=obj['start_id'],
            parent_id=obj['end_id'],
            provenance = obj['provenance'])


    def protein_converter(self, obj: dict) -> mysqlProtein:
        gene_ids = [p for p in obj['xref'] if p.startswith('NCBIGene:')]
        if len(gene_ids) > 0:
            gene_id = gene_ids[0].split(':')[-1]
        else:
            gene_id = None
        ensembl_ids = [p for p in obj['xref'] if p.startswith('ENSEMBL:ENSP')]
        if len(ensembl_ids) > 0 and gene_id is None:
            string_id = ensembl_ids[0].split(':')[-1]
        else:
            string_id = None

        return mysqlProtein(
            id = self.resolve_id('protein', obj['id']),
            ifx_id = obj['id'],
            description = obj['name'],
            uniprot = obj['uniprot_id'],
            sym = obj['symbol'],
            geneid = gene_id,
            stringid = string_id,
            seq = obj['sequence'],
            provenance = obj['provenance'],
            preferred_symbol = obj['preferred_symbol']
        )

    def tdl_info_converter(self, obj: dict) -> List[TDL_info]:
        tdl_infos = []
        if 'antibody_count' in obj and obj['antibody_count'] is not None and len(obj['antibody_count']) > 0:
            antibody_count = max([int(p) for p in obj['antibody_count']])
            if antibody_count > 0:
                tdl_infos.append(TDL_info(
                    itype="Ab Count",
                    protein_id=self.resolve_id('protein', obj['id']),
                    integer_value=antibody_count
                ))
        if 'pm_score' in obj and obj['pm_score'] is not None and len(obj['pm_score']) > 0:
            pm_score = max([float(p) for p in obj['pm_score']])
            tdl_infos.append(TDL_info(
                itype="JensenLab PubMed Score",
                protein_id=self.resolve_id('protein', obj['id']),
                number_value=pm_score
            ))
        return tdl_infos

    def protein_alias_converter(self, obj: dict) -> List[Alias]:
        aliases = []
        protein_id = self.resolve_id('protein', obj['id'])

        ids = [EquivalentId.parse(x) for x in obj['xref']]

        symbols = [x for x in ids if x.type in (Prefix.OldSymbol, Prefix.Symbol)]
        uniprot_ids = [x for x in ids if x.type == Prefix.UniProtKB]
        ncbi_ids = [x for x in ids if x.type == Prefix.NCBIGene]

        for s in symbols:
            aliases.append(Alias(protein_id=protein_id, type='symbol', value=s.id))
        for u in uniprot_ids:
            aliases.append(Alias(protein_id=protein_id, type='uniprot', value=u.id))
        for n in ncbi_ids:
            aliases.append(Alias(protein_id=protein_id, type='NCBI Gene ID', value=n.id))
        return aliases

    def protein_xref_converter(self, obj: dict) -> List[Xref]:
        aliases = []
        protein_id = self.resolve_id('protein', obj['id'])

        ids = [EquivalentId.parse(x) for x in obj['xref']]
        for a in ids:
            if a.type == Prefix.ENSEMBL:
                typeText = "Ensembl"
            else:
                typeText = a.type.value
            aliases.append(
                Xref(protein_id=protein_id,
                           xtype=typeText,
                           value=a.id)
            )
        return aliases

    def t2tc_converter(self, obj: dict) -> T2TC:
        return T2TC(
            target_id= self.resolve_id('protein', obj['id']),
            protein_id=self.resolve_id('protein', obj['id']),
            provenance = obj['provenance'])

    id_mapping = {}
    def resolve_id(self, table, id):
        if table not in self.id_mapping:
            self.id_mapping[table] = {}
        mapping = self.id_mapping[table]
        if id not in mapping:
            mapping[id] = len(mapping.values()) + 1
        return mapping[id]

    def target_converter(self, obj: dict) -> Target:
        return Target(
            id = self.resolve_id("protein", obj['id']),
            ifx_id = obj['id'],
            name = obj['name'],
            ttype = 'Single Protein',
            fam = obj['idg_family'],
            tdl = obj['tdl'],
            provenance = obj['provenance']
        )

    def goa_converter(self, obj: dict) -> List[GoA]:
        return [
            GoA(
                protein_id = self.resolve_id('protein', obj['start_id']),
                go_id = obj['end_id'],
                evidence = e['abbreviation'],
                goeco = e['evidence'],
                assigned_by = e['assigned_by'],
                go_term = f"{obj['end_node']['type']}:{obj['end_node']['term']}",
                go_type = {'C': 'Component', 'P': 'Process', 'F': 'Function'}.get(obj['end_node']['type'], obj['end_node']['type']),
                go_term_text = obj['end_node']['term'],
                provenance = obj['provenance']
            )
            for e in obj['evidence']]

    def get_generif_hash(self, obj: dict) -> str:
        data_to_hash = f"{obj['start_id']}-{obj['gene_id']}-{obj['date']}-{obj['end_node']['text']}"
        hashed_id = hashlib.sha256(data_to_hash.encode('utf-8')).hexdigest()
        return hashed_id

    def generif_converter(self, obj: dict) -> GeneRif:
        hashed_id = self.get_generif_hash(obj)

        return GeneRif(
            id=hashed_id,
            protein_id=self.resolve_id('protein', obj['start_id']),
            gene_id=obj['gene_id'],
            date=obj['date'],
            text=obj['end_node']['text'],
            provenance=obj['provenance']
        )
    setattr(generif_converter, 'merge_anyway', True)
    setattr(generif_converter, 'deduplicate', True)

    def generif_assoc_converter(self, obj: dict) -> List[GeneRif2Pubmed]:
        hashed_id = self.get_generif_hash(obj)
        pubmed_links = []
        for pmid in obj['pmids']:
            pubmed_link = GeneRif2Pubmed(
                generif_id=hashed_id,
                pubmed_id=pmid,
                provenance=obj['provenance']
            )
            pubmed_links.append(pubmed_link)

        return pubmed_links
    setattr(generif_assoc_converter, 'merge_anyway', True)

    def p2p_converter(self, obj: dict) -> List[Protein2Pubmed]:
        pubmed_links = []
        for pmid in obj['pmids']:
            pubmed_link = Protein2Pubmed(
                protein_id=self.resolve_id('protein', obj['start_id']),
                pubmed_id=pmid,
                gene_id=obj['gene_id'],
                source='NCBI',
                provenance=obj['provenance']
            )
            pubmed_links.append(pubmed_link)
        return pubmed_links

    setattr(p2p_converter, 'merge_anyway', True)


    def ligand_converter(self, obj: dict) -> mysqlLigand:
        xrefs = [
            EquivalentId.parse(eq_id)
            for eq_id in (obj.get('xref') or [])
        ]
        PubChemIDs = [x.id for x in xrefs if x.type == Prefix.PUBCHEM_COMPOUND]
        ChemblIDs = [x.id for x in xrefs if x.type == Prefix.CHEMBL_COMPOUND]
        IUPHARIDs = [x.id for x in xrefs if x.type == Prefix.GTOPDB]
        DCIDs = [x.id for x in xrefs if x.type == Prefix.DrugCentral]
        UNIIs = [x.id for x in xrefs if x.type == Prefix.UNII]
        return mysqlLigand(
            id = obj.get('id'),
            identifier=obj.get('id'),
            name = obj.get('name'),
            isDrug = obj.get('isDrug', False),
            smiles=obj.get('smiles'),
            description=obj.get('description'),
            PubChem=",".join(PubChemIDs),
            ChEMBL=",".join(ChemblIDs),
            guide_to_pharmacology=",".join(IUPHARIDs),
            DrugCentral=",".join(DCIDs),
            unii=",".join(UNIIs),
            provenance=obj['provenance']
        )

    def ligand_edge_converter(self, obj: dict) -> List[LigandActivity]:
        activity_objects = []

        for detail in obj.get('details', []):
            pubmed_ids = detail.get('act_pmids') or []
            if detail.get('moa_pmid'):
                pubmed_ids.append(detail.get('moa_pmid'))
            pubmed_ids = [str(i) for i in pubmed_ids]
            activity_object = LigandActivity(
                ncats_ligand_id = obj['end_id'],
                target_id = self.resolve_id('protein', obj['start_id']),
                act_value=detail.get('act_value'),
                act_type=detail.get('act_type'),
                action_type=detail.get('action_type'),
                reference=detail.get('reference'),
                pubmed_ids="|".join(list(set(pubmed_ids))),
                provenance=obj['provenance']
            )
            activity_objects.append(activity_object)

        return activity_objects
