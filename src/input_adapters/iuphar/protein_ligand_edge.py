import csv
from typing import Generator, List, Optional
from src.api_adapters.strawberry_models.pharos_query_models import EquivalentId
from src.constants import Prefix
from src.input_adapters.iuphar.ligand_node import IUPHARAdapter
from src.models.ligand import ProteinLigandRelationship, Ligand, ActivityDetails
from src.models.protein import Protein


class ProteinLigandEdgeAdapter(IUPHARAdapter):
    interaction_file_path: str
    id_map: dict
    pchembl_cutoff: float

    def __init__(self, file_path: str, interaction_file_path: str, pchembl_cutoff: float):
        super().__init__(file_path)
        self.interaction_file_path = interaction_file_path
        self.id_map = self.get_id_map()
        self.pchembl_cutoff = pchembl_cutoff

    def get_id(self, ligand_id: str) -> Optional[str]:
        if ligand_id in self.id_map:
            cid = self.id_map[ligand_id]['cid']
            chembl = self.id_map[ligand_id]['chembl']
            inchikey = self.id_map[ligand_id]['inchikey']
            if cid:
                return EquivalentId(id=cid, type=Prefix.PUBCHEM_COMPOUND).id_str()
            elif chembl:
                return EquivalentId(id=chembl, type=Prefix.CHEMBL_COMPOUND).id_str()
            elif inchikey:
                return EquivalentId(id=inchikey, type=Prefix.INCHIKEY).id_str()
        return None

    def get_id_map(self):
        id_map = {}
        with open(self.file_path, mode='r') as file:
            next(file)
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                if row['Type'] in ['Peptide', 'Antibody']:
                    continue
                id = row['Ligand ID']
                cid = row['PubChem CID']
                chembl = row['ChEMBL ID']
                inchikey = row['InChIKey']

                if (not cid or cid == '') and (not chembl or chembl == '') and (not inchikey or inchikey == ''):
                    continue
                id_map[id]= {
                    'cid': cid,
                    'chembl': chembl,
                    'inchikey': inchikey
                }

        return id_map


    def get_all(self) -> Generator[List[ProteinLigandRelationship], None, None]:
        edges: List[ProteinLigandRelationship] = []
        with open(self.interaction_file_path, mode='r') as file:
            next(file)
            csv_reader = csv.DictReader(file)
            protein_dict = {}
            ligand_dict = {}

            for row in csv_reader:
                if row['Target Species'] != 'Human':
                    continue
                if row['Ligand Type'] in ['Peptide', 'Antibody']:
                    continue
                uniprot_id_column = row['Target UniProt ID']
                if not uniprot_id_column or uniprot_id_column == '':
                    continue

                ligand_id = row['Ligand ID']
                ligand_id_to_use = self.get_id(ligand_id)
                if ligand_id_to_use is None:
                    continue

                if ligand_id_to_use in ligand_dict:
                    ligand_node = ligand_dict[ligand_id_to_use]
                else:
                    ligand_node = Ligand(id=ligand_id_to_use)
                    ligand_dict[ligand_id_to_use] = ligand_node

                act_value = row['Affinity Median']
                if act_value is None or act_value == '':
                    continue

                act_value_float: float = float(row['Affinity Median'])
                if act_value_float < self.pchembl_cutoff:
                    continue

                act_type = row['Original Affinity Units']
                comment = row['Assay Description']
                if comment is None or comment == '':
                    comment = None

                pmids = row['PubMed ID'].split('|') if row['PubMed ID'] != '' else None

                for uniprot_id in uniprot_id_column.split('|'):
                    uniprot_id_to_use = EquivalentId(id=uniprot_id, type=Prefix.UniProtKB).id_str()

                    if uniprot_id_to_use in protein_dict:
                        protein_node = protein_dict[uniprot_id_to_use]
                    else:
                        protein_node = Protein(id=uniprot_id_to_use)
                        protein_dict[uniprot_id_to_use] = protein_node

                activity_details = ActivityDetails(
                    act_value=act_value_float,
                    act_type=act_type,
                    act_pmids=pmids,
                    comment=comment
                )

                edges.append(ProteinLigandRelationship(
                    start_node=protein_node,
                    end_node=ligand_node,
                    details=[activity_details]
                ))

        yield edges

