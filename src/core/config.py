import importlib.util
import os
from typing import List

import yaml

from src.interfaces.id_resolver import IdResolver
from src.interfaces.input_adapter import InputAdapter
from src.interfaces.labeler import Labeler
from src.interfaces.output_adapter import OutputAdapter
from src.shared.db_credentials import DBCredentials


def create_object_from_config(config: dict):
    print(f'creating: {config["class"]}')
    module_path = config['import']
    class_name = config['class']
    module_name = os.path.splitext(os.path.basename(module_path))[0]

    # Load the module from the file path
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Get the class from the module
    cls = getattr(module, class_name)

    kwargs = {}
    if 'kwargs' in config:
        kwargs = config['kwargs']

    if 'credentials' in config:
        cred_node = config['credentials']
        kwargs['credentials'] = (
            DBCredentials(
                user=cred_node.get('user', None),
                url=cred_node['url'],
                password=cred_node.get('password', None),
                schema=cred_node.get('schema', None),
                port=cred_node.get('port', None),
                internal_url=cred_node.get('internal_url', cred_node['url'])
            ))

    return cls(**kwargs)

class Config:
    config_dict: {}
    yaml_files: list = []
    def __init__(self, yaml_file):
        self.config_dict = self.load_config_from_yaml(yaml_file)

    def create_labeler(self):
        if 'labeler' in self.config_dict:
            obj = create_object_from_config(self.config_dict['labeler'])
            return obj
        return Labeler()

    def load_config_from_yaml(self, file_path):
        config_dict = self.load_one_yaml(file_path)
        config_dict = self._load_nested_yamls(config_dict)
        print('Configuration loaded from yaml file(s)')
        return config_dict

    def load_one_yaml(self, yaml_file):
        with open(yaml_file, "r") as file:
            config_dict = yaml.safe_load(file)
            self.yaml_files.append(yaml_file)
            return config_dict

    def _load_nested_yamls(self, config_node):
        if isinstance(config_node, str) and (config_node.endswith(".yaml") or config_node.endswith(".yml")):
            nested_config = self.load_one_yaml(config_node)
            return self._load_nested_yamls(nested_config)
        if isinstance(config_node, dict):
            for key, value in config_node.items():
                if isinstance(value, list):
                    for index, entry in enumerate(value):
                        value[index] = self._load_nested_yamls(entry)
                if isinstance(value, dict):
                    config_node[key] = self._load_nested_yamls(value)
                if isinstance(value, str) and (value.endswith(".yaml") or value.endswith(".yml")):
                    nested_config = self.load_one_yaml(value)
                    config_node[key] = self._load_nested_yamls(nested_config)
        if isinstance(config_node, list):
            if isinstance(config_node, list):
                for index, entry in enumerate(config_node):
                    config_node[index] = self._load_nested_yamls(entry)
        return config_node

    def __repr__(self):
        return f"{self.__class__.__name__}({self.config_dict})"

    def create_object_list(self, key, required = True):
        objects = []
        if key not in self.config_dict and required == False:
            return objects
        config = self.config_dict[key]
        for c in config:
            obj = create_object_from_config(c)
            objects.append(obj)
        return objects


class Dashboard_Config(Config):
    pass


class ETL_Config(Config):
    resolvers: dict[str, IdResolver] = {}

    def __init__(self, yaml_file):
        Config.__init__(self, yaml_file)
        self.create_resolvers()

    def create_resolvers(self):
        if 'resolvers' in self.config_dict:
            for config in self.config_dict['resolvers']:
                key = config['label']
                obj = create_object_from_config(config)
                self.resolvers[key] = obj

    def create_output_adapters(self) -> List[OutputAdapter]:
        return self.create_object_list('output_adapters')

    def create_input_adapters(self) -> List[InputAdapter]:
        return self.create_object_list('input_adapters', False)

