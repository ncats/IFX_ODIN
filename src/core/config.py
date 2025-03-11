import importlib.util
import os
from typing import List

import yaml

from src.interfaces.id_resolver import IdResolver
from src.interfaces.input_adapter import InputAdapter
from src.interfaces.output_adapter import OutputAdapter
from src.shared.db_credentials import DBCredentials


def create_object_from_config(config: dict):
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
                port=cred_node.get('port', None)
            ))

    return cls(**kwargs)

class Config:
    config_dict: {}
    yaml_files: list = []
    resolvers: dict[str, IdResolver] = {}

    def __init__(self, yaml_file):
        self.config_dict = self.load_config_from_yaml(yaml_file)
        self.create_resolvers()

    def is_testing(self):
        if 'testing' in self.config_dict:
            return self.config_dict['testing']
        return False

    def create_labeler(self):
        if 'labeler' in self.config_dict:
            obj = create_object_from_config(self.config_dict['labeler'])
            return obj
        return None

    def create_resolvers(self):
        if 'resolvers' in self.config_dict:
            for config in self.config_dict['resolvers']:
                key = config['label']
                obj = create_object_from_config(config)
                self.resolvers[key] = obj

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

    def create_output_adapters(self) -> List[OutputAdapter]:
        config = self.config_dict['output_adapters']
        output_adapters = []
        for c in config:
            obj = create_object_from_config(c)
            output_adapters.append(obj)
        return output_adapters

    def create_adapters(self) -> List[InputAdapter]:
        input_adapters = []
        if 'input_adapters' not in self.config_dict:
            return input_adapters
        config = self.config_dict['input_adapters']
        for c in config:
            obj = create_object_from_config(c)
            input_adapters.append(obj)
        return input_adapters

    def __repr__(self):
        return f"{self.__class__.__name__}({self.config_dict})"
