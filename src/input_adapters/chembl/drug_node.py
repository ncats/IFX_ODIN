import os
import pickle
from typing import List, Dict, Generator

from sqlalchemy import or_

from src.constants import Prefix, CHEMBL_PATENT_SOURCE_ID, CHEMBL_FUNCTIONAL_ASSAY_CODE, CHEMBL_BINDING_ASSAY_CODE, \
    CHEMBL_SMALL_MOLECULE_CODE, CHEMBL_SINGLE_PROTEIN_CODE, HUMAN_TAX_ID, DataSourceName
from src.input_adapters.chembl.tables import Activities, CompoundRecords, CompoundStructures, MoleculeDictionary, \
    Assays, TargetDictionary, ComponentSequence, TargetComponents, Docs, Version
from src.models.datasource_version_info import DatasourceVersionInfo
from src.input_adapters.sql_adapter import MySqlAdapter
from src.interfaces.input_adapter import InputAdapter
from src.models.ligand import Ligand, ProteinLigandRelationship, ActivityDetails
from src.models.node import EquivalentId
from src.models.protein import Protein
from src.shared.db_credentials import DBCredentials


class ChemblAdapter(MySqlAdapter):
    pchembl_cutoff: float
    version_info: DatasourceVersionInfo

    def __init__(self, credentials: DBCredentials, pchembl_cutoff: float = 5):
        MySqlAdapter.__init__(self, credentials)
        self.initialize_version()
        self.pchembl_cutoff = pchembl_cutoff

    def initialize_version(self):
        results = self.get_session().query(
            Version.name,
            Version.creation_date
        ).filter(Version.name.op('REGEXP')('^ChEMBL_[0-9]+$')).first()
        self.version_info = DatasourceVersionInfo(version=results.name, version_date=results.creation_date)

    def fetch_activity_data(self, pchembl_cutoff: float):
        cache_file = f"input_files/chembl/activities_{pchembl_cutoff}.pkl"
        if os.path.exists(cache_file):
            with open(cache_file, 'rb') as f:
                print('loading chembl data from cache')
                return pickle.load(f)

        query = self.get_all_activities_query(pchembl_cutoff)

        results = []
        for row in query.yield_per(50000):
            results.append(row)

        if not os.path.exists(os.path.dirname(cache_file)):
            os.makedirs(os.path.dirname(cache_file))
        with open(cache_file, 'wb') as f:
            pickle.dump(results, f)

        return results

    def get_all_activities_query(self, pchembl_cutoff):
        return (self.get_session().query(
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
            MoleculeDictionary.structure_type == CHEMBL_SMALL_MOLECULE_CODE,
            TargetDictionary.target_type == CHEMBL_SINGLE_PROTEIN_CODE,
            ComponentSequence.tax_id == HUMAN_TAX_ID,
        )
                .filter(
            Activities.standard_flag == 1,
            Activities.pchembl_value >= pchembl_cutoff,
            Activities.standard_relation == '=',
            Assays.assay_type.in_([CHEMBL_FUNCTIONAL_ASSAY_CODE, CHEMBL_BINDING_ASSAY_CODE]),
            or_(
                Activities.doc_id.isnot(None),
                CompoundRecords.src_id == CHEMBL_PATENT_SOURCE_ID
            )
        )
                .distinct()
                )


class DrugNodeAdapter(InputAdapter, ChemblAdapter):

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.ChEMBL

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def get_all(self) -> Generator[List[Ligand], None, None]:
        activity_results = self.fetch_activity_data(self.pchembl_cutoff)

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

        yield list(drug_dict.values())


class ProteinDrugEdgeAdapter(InputAdapter, ChemblAdapter):
    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.ChEMBL

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def get_all(self) -> Generator[List[ProteinLigandRelationship], None, None]:
        activity_results = self.fetch_activity_data(self.pchembl_cutoff)

        relationship_map: Dict[str, ProteinLigandRelationship] = {}

        for row in activity_results:
            compound_id = EquivalentId(id=row.compound_id, type=Prefix.CHEMBL_COMPOUND)
            uniprot = row.uniprot_id
            if uniprot is not None and len(uniprot) > 0:
                pro_id = EquivalentId(id=uniprot, type=Prefix.UniProtKB)
            else:
                target_id = row.target_id
                pro_id = EquivalentId(id=target_id, type=Prefix.CHEMBL_PROTEIN)

            rel_id = f"{compound_id.id_str()}|{pro_id.id_str()}"
            if rel_id in relationship_map:
                pro_lig_edge = relationship_map[rel_id]
            else:
                lig = Ligand(
                    id=compound_id.id_str()
                )
                pro = Protein(id = pro_id.id_str())
                pro_lig_edge = ProteinLigandRelationship(
                    start_node=pro, end_node=lig
                )
                relationship_map[rel_id] = pro_lig_edge

            activity_details = ActivityDetails(
                ref_id=row.activity_id,
                act_value=float(row.pchembl_value),
                act_type=row.standard_type,
                action_type=row.action_type,
                assay_type=row.assay_type,
                act_pmid=row.pubmed_id,
                comment=f"Chembl Source ID: {row.src_id}"
            )
            pro_lig_edge.details.append(activity_details)

        yield list(relationship_map.values())
