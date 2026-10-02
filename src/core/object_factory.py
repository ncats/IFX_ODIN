"""Construct configured objects without loading graph configuration or Registry."""

import importlib.util
import os
import sys

from src.shared.db_credentials import DBCredentials


def create_object_from_config(config: dict):
    module_path = config['import']
    class_name = config['class']
    abs_module_path = os.path.abspath(module_path)
    normalized_module_path = os.path.normpath(abs_module_path)
    module_name = (
        "yaml_import__"
        + normalized_module_path.replace(":", "").replace(os.sep, "_").replace(".", "_")
    )

    # Cache modules by full file path-derived name so multiple YAML entries that point
    # at the same file reuse one module object, and same-basename files do not collide.
    module = sys.modules.get(module_name)
    if module is None:
        spec = importlib.util.spec_from_file_location(module_name, abs_module_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        try:
            spec.loader.exec_module(module)
        except Exception:
            sys.modules.pop(module_name, None)
            raise

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
