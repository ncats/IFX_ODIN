from typing import List, Dict

from sqlalchemy import text, or_

from src.constants import Prefix, CHEMBL_PATENT_SOURCE_ID, CHEMBL_FUNCTIONAL_ASSAY_CODE, CHEMBL_BINDING_ASSAY_CODE, \
    CHEMBL_SMALL_MOLECULE_CODE, CHEMBL_SINGLE_PROTEIN_CODE, HUMAN_TAX_ID
from src.input_adapters.chembl.tables import Activities, CompoundRecords, CompoundStructures, MoleculeDictionary, \
    Assays, TargetDictionary, ComponentSequence, TargetComponents, Docs, Version
from src.input_adapters.drug_central.drug_node import DatabaseVersionInfo
from src.input_adapters.sql_adapter import MySqlAdapter
from src.interfaces.input_adapter import NodeInputAdapter, RelationshipInputAdapter
from src.models.ligand import Ligand, ProteinLigandRelationship, ActivityDetails
from src.models.node import Node, EquivalentId, Relationship
from src.models.protein import Protein
from src.shared.db_credentials import DBCredentials


class ChemblAdapter(MySqlAdapter):
    pchembl_cutoff: float
    version_info: DatabaseVersionInfo

    def __init__(self, credentials: DBCredentials, pchembl_cutoff: float = 5):
        MySqlAdapter.__init__(self, credentials)
        self.initialize_version()
        self.pchembl_cutoff = pchembl_cutoff

    def initialize_version(self):
        results = self.get_session().query(
            Version.name,
            Version.creation_date
        ).filter(Version.name.op('REGEXP')('^ChEMBL_[0-9]+$')).first()
        self.version_info = DatabaseVersionInfo(version=results.name, timestamp=results.creation_date)

    @classmethod
    def fetch_activity_data(cls, session, pchembl_cutoff: float):
        query = (session.query(
            Activities.activity_id,
            MoleculeDictionary.chembl_id.label('compound_id'),
            CompoundRecords.compound_name,
            CompoundStructures.canonical_smiles,
            TargetDictionary.chembl_id.label('target_id'),
            ComponentSequence.accession.label('uniprot_id'),
            TargetDictionary.pref_name,
            Activities.action_type,
            Activities.pchembl_value,
            Activities.standard_type,
            Assays.assay_type,
            Docs.journal,
            Docs.year,
            Docs.volume,
            Docs.issue,
            Docs.first_page,
            Docs.pubmed_id,
            CompoundRecords.src_id
        ).join(CompoundRecords, Activities.molregno == CompoundRecords.molregno)
        .join(CompoundStructures, Activities.molregno == CompoundStructures.molregno)
        .join(MoleculeDictionary, Activities.molregno == MoleculeDictionary.molregno)
        .join(Assays, Activities.assay_id == Assays.assay_id)
        .join(TargetDictionary, Assays.tid == TargetDictionary.tid)
        .join(TargetComponents, TargetDictionary.tid == TargetComponents.tid)
        .join(ComponentSequence, TargetComponents.component_id == ComponentSequence.component_id)
        .join(Docs, Activities.doc_id == Docs.doc_id, isouter=True)
        .filter(
            Activities.standard_flag == 1,
            Activities.pchembl_value >= pchembl_cutoff,
            Activities.standard_relation == '=',
            MoleculeDictionary.structure_type == CHEMBL_SMALL_MOLECULE_CODE,
            Assays.assay_type.in_([CHEMBL_FUNCTIONAL_ASSAY_CODE, CHEMBL_BINDING_ASSAY_CODE]),
            TargetDictionary.target_type == CHEMBL_SINGLE_PROTEIN_CODE,
            ComponentSequence.tax_id == HUMAN_TAX_ID,
            or_(
                Activities.doc_id.isnot(None),
                CompoundRecords.src_id == CHEMBL_PATENT_SOURCE_ID
            )
        ))

        results = []
        for row in query.yield_per(50000):
            results.append(row)
        return results


class DrugNodeAdapter(NodeInputAdapter, ChemblAdapter):
    name = "ChEMBL Drug Adapter"

    def get_all(self) -> List[Node]:
        activity_results = ChemblAdapter.fetch_activity_data(self.get_session(), self.pchembl_cutoff)

        drug_dict: Dict[str, Ligand] = {}

        for row in activity_results:
            id = EquivalentId(id=row.compound_id, type=Prefix.CHEMBL_COMPOUND)
            id_str = id.id_str()

            if id_str not in drug_dict:
                drug_dict[id_str] = Ligand(
                    id=id.id_str(),
                    smiles=row.canonical_smiles,
                    name=row.compound_name
                )

        return list(drug_dict.values())

    def get_audit_trail_entries(self, obj: Node) -> List[str]:
        version_info = [
            f"Node created by {self.name} based on ChEMBL version: {self.version_info.version} ({self.version_info.timestamp})"]
        return version_info


class ProteinDrugEdgeAdapter(RelationshipInputAdapter, ChemblAdapter):
    name = "ChEMBL Protein Drug Edge Adapter"
    def get_all(self) -> List[Relationship]:
        activity_results = ChemblAdapter.fetch_activity_data(self.get_session(), self.pchembl_cutoff)

        relationships: List[ProteinLigandRelationship] = []

        for row in activity_results:
            compound_id = EquivalentId(id=row.compound_id, type=Prefix.CHEMBL_COMPOUND)
            lig = Ligand(
                id=compound_id.id_str()
            )

            uniprot = row.uniprot_id
            if uniprot is not None and len(uniprot) > 0:
                pro_id = EquivalentId(id=uniprot, type=Prefix.UniProtKB)
            else:
                target_id = row.target_id
                pro_id = EquivalentId(id=target_id, type=Prefix.CHEMBL_PROTEIN)
            pro = Protein(id = pro_id.id_str())

            pro_lig_edge = ProteinLigandRelationship(
                start_node=pro, end_node=lig
            )
            activity_details = ActivityDetails(
                ref_id=row.activity_id,
                act_value=float(row.pchembl_value),
                act_type=row.standard_type,
                action_type=row.action_type,
                assay_type=row.assay_type,
                act_pmid=row.pubmed_id,
                comment=row.src_id
            )
            pro_lig_edge.details = activity_details
            relationships.append(pro_lig_edge)

        return relationships

    def get_audit_trail_entries(self, obj: Node) -> List[str]:
        version_info = [
            f"Relationship created by {self.name} based on ChEMBL version: {self.version_info.version} ({self.version_info.timestamp})"]
        return version_info