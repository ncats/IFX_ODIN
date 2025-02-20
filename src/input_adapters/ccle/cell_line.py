import csv
from typing import List

from src.constants import Prefix
from src.input_adapters.ccle.experiment_and_project import CCLEFileInputAdapter
from src.models.node import EquivalentId
from src.models.pounce.data import Biospecimen


class CellLine(CCLEFileInputAdapter):

    def get_all(self) -> List[Biospecimen]:
        biospecimens = []
        with open(self.file_path, 'r') as file:
            reader = csv.DictReader(file, delimiter='\t')
            for line in reader:
                depMapID = line.get('depMapID')
                ccle_id = line.get('CCLE_ID')
                if depMapID is not None and depMapID != 'NA':
                    id = EquivalentId(id = depMapID, type = Prefix.DepMap).id_str()
                else:
                    id = EquivalentId(id = ccle_id, type = Prefix.CCLE_ID).id_str()

                biospecimen = Biospecimen(
                    id=id,
                    name=line.get('Name'),
                    type="Cancer Cell Line",
                    organism=['Homo sapiens (Human)'],
                    part=line.get('Site_Primary'),
                    cell_line=line.get('CCLE_ID'),
                    sex=line.get('Gender').lower(),
                    age=line.get('Age')
                )
                biospecimens.append(biospecimen)
        return biospecimens
