import os
from abc import ABC
from datetime import datetime
import xml.etree.ElementTree as ET
from typing import List, Union
from src.constants import DataSourceName, Prefix
from src.interfaces.input_adapter import NodeInputAdapter, RelationshipInputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import EquivalentId, Node, Relationship
from src.models.pounce.data import Biospecimen

class XMLAdapter:
    file_path: str
    root_node = None

    def get_root_node(self):
        if self.root_node is None:
            tree = ET.parse(self.file_path)
            self.root_node = tree.getroot()
        return self.root_node

    def extract_data_version(self) -> DatasourceVersionInfo:
        download_date = datetime.fromtimestamp(os.path.getmtime(self.file_path)).date()
        root_node = self.get_root_node()
        version = root_node.find("./header/release").attrib.get("version")
        version_date = datetime.strptime(root_node.find("./header/release").attrib.get("updated"), "%Y-%m-%d").date()

        return DatasourceVersionInfo(
            version=version,
            download_date=download_date,
            version_date=version_date
        )

    def __init__(self, file_path: str):
        self.file_path = file_path

class CellosaurusBaseAdapter(NodeInputAdapter, RelationshipInputAdapter, XMLAdapter, ABC):
    dataSourceVersionInfo: DatasourceVersionInfo
    include_only_human: bool

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.Cellosaurus

    def get_version(self) -> DatasourceVersionInfo:
        return self.dataSourceVersionInfo

    def __init__(self, include_only_human = True, **kwargs):
        NodeInputAdapter.__init__(self)
        RelationshipInputAdapter.__init__(self)
        XMLAdapter.__init__(self, **kwargs)
        self.dataSourceVersionInfo = self.extract_data_version()
        self.include_only_human = include_only_human


class CellosaurusCellLineAdapter(CellosaurusBaseAdapter):

    def get_all(self) -> List[Union[Node, Relationship]]:
        root_node = self.get_root_node()
        biospecimens = []
        for cell_line in root_node.findall('./cell-line-list/cell-line'):
            category = cell_line.get('category')
            age = cell_line.get('age')
            sex = cell_line.get('sex', '').lower()
            accession = cell_line.find("./accession-list/accession[@type='primary']").text
            id = EquivalentId(id = accession, type = Prefix.Cellosaurus).id_str()
            name = cell_line.find("./name-list/name[@type='identifier']").text
            organisms = [xref.find('label').text for xref in cell_line.findall("./species-list/xref[@category='Taxonomy']")]

            parts = []
            for site in cell_line.findall("./derived-from-site-list/derived-from-site/site"):
                found = False
                part = None
                for xref in site.findall("./xref[@category='Anatomy/cell type resources']"):
                    part = xref.find('label').text
                    found = True
                if not found:
                    part = site.text.strip()
                if part is not None:
                    parts.append(part)

            if self.include_only_human == True:
                if "Homo sapiens (Human)" not in organisms:
                    continue
                organisms = ["Homo sapiens (Human)"]
            biospecimen = Biospecimen(
                id = id,
                name=name,
                part="|".join(parts),
                category=category,
                organism=organisms,
                sex=sex,
                age=age
            )
            biospecimens.append(biospecimen)
        return biospecimens
