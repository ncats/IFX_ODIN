import json
import os
from typing import List, Generator, Union

from src.constants import Prefix, DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.go_term import GoTerm, GoType, GoTermHasParent
from src.models.node import EquivalentId

class GOTermAdapter(InputAdapter):
    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.GO

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def __init__(self, data_source):
        InputAdapter.__init__(self)
        self.version_info = data_source.version_info()
        file_path = str(data_source.file("go-basic.json"))
        self.file_path = file_path
        self.file_name = os.path.basename(file_path)

    def get_all(self) -> Generator[List[Union[GoTerm, GoTermHasParent]], None, None]:
        go_terms: List[GoTerm] = []
        go_term_edges: List[GoTermHasParent] = []

        with open(self.file_path, 'r') as file:
            data = json.load(file)
            nodes = data['graphs'][0]['nodes']

            for node in nodes:
                if node['type'] != 'CLASS':
                    continue
                if node.get('meta',{}).get('deprecated', False):
                    continue

                meta = node.get('meta')
                definition = meta.get('definition', {}).get('val', None)
                subsets = meta.get('subsets', [])

                term = node['lbl']
                go_url = node['id']  # "http://purl.obolibrary.org/obo/GO_0002626",
                go_term_id = go_url.split('_')[-1]
                equivalent_id = EquivalentId(id=go_term_id, type=Prefix.GO)

                property_values = node['meta']['basicPropertyValues']
                type_entry = next((pv for pv in property_values if
                                   pv['pred'] == 'http://www.geneontology.org/formats/oboInOwl#hasOBONamespace'), None)
                if type_entry is not None:
                    go_type = GoType.parse(type_entry['val'])
                    go_term = GoTerm(
                        id=equivalent_id.id_str(),
                        type=go_type,
                        term=term,
                        definition=definition,
                        subsets=subsets
                    )
                    go_terms.append(go_term)
            yield go_terms

            edges = data['graphs'][0]['edges']
            for edge in edges:
                if edge['pred'] != 'is_a':
                    continue

                sub_id = edge['sub'].split('_')[-1]
                obj_id = edge['obj'].split('_')[-1]

                sub_id_obj = EquivalentId(id=sub_id, type=Prefix.GO)
                obj_id_obj = EquivalentId(id=obj_id, type=Prefix.GO)

                go_term_edges.append(GoTermHasParent(
                    start_node=GoTerm(id=sub_id_obj.id_str()),
                    end_node=GoTerm(id=obj_id_obj.id_str())
                ))
            yield go_term_edges


