"""Regression checks for running local QA tools without the Registry SDK."""

import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

from src.core.object_factory import create_object_from_config
from src.core.registry_integration import RegistryIntegration


def test_qa_browser_and_local_config_without_registry(tmp_path):
    config_path = tmp_path / "local.yaml"
    config_path.write_text("input_adapters: []\n", encoding="utf-8")
    result = subprocess.run(
        [sys.executable, "-c", """
import importlib.abc
import sys

class BlockRegistry(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'ifx_registry' or fullname.startswith('ifx_registry.'):
            raise ModuleNotFoundError('Registry intentionally unavailable', name=fullname)

sys.meta_path.insert(0, BlockRegistry())
from src.input_adapters.pounce_sheets import validator_loader
assert 'src.core.config' not in sys.modules
from src.qa_browser import app
from src.core.config import Config, create_object_from_config, resolve_registry_references
from src.core.object_factory import create_object_from_config as factory
assert create_object_from_config is factory
assert Config(sys.argv[1]).config_dict == {'input_adapters': []}
assert not any(name == 'ifx_registry' or name.startswith('ifx_registry.') for name in sys.modules)
try:
    resolve_registry_references({'registry': {'cache_dir': sys.argv[2]}})
except ModuleNotFoundError as error:
    assert error.name == 'ifx_registry'
else:
    raise AssertionError('Registry-backed configuration must require Registry')
""", str(config_path), str(tmp_path)],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_registry_connect_loads_client_and_forwards_configuration(monkeypatch, tmp_path):
    calls = []
    client = object()

    def connect(credentials, **kwargs):
        calls.append((credentials, kwargs))
        return client

    monkeypatch.setitem(
        sys.modules, "ifx_registry",
        SimpleNamespace(RegistryClient=SimpleNamespace(connect=connect)),
    )
    integration = RegistryIntegration.connect({
        "credentials": "credentials.yaml", "cache_dir": str(tmp_path),
        "bucket": "test-bucket", "region": "test-region", "prefix": "test/",
    })
    assert integration._client is client
    assert calls == [("credentials.yaml", {
        "cache_dir": tmp_path, "bucket": "test-bucket",
        "region": "test-region", "prefix": "test/",
    })]


def test_factory_preserves_module_identity_and_credentials(tmp_path):
    configs = []
    for folder in ("first", "second"):
        directory = tmp_path / folder
        directory.mkdir()
        module = directory / "example.py"
        module.write_text(
            "class Example:\n"
            "    def __init__(self, value, credentials):\n"
            "        self.value = value\n"
            "        self.credentials = credentials\n",
            encoding="utf-8",
        )
        configs.append({
            "import": str(module), "class": "Example", "kwargs": {"value": 7},
            "credentials": {"url": "localhost", "user": "test", "port": 1234},
        })
    first = create_object_from_config(configs[0])
    again = create_object_from_config(configs[0])
    second = create_object_from_config(configs[1])
    assert type(first) is type(again)
    assert type(first) is not type(second)
    assert first.value == 7
    assert first.credentials.url == "localhost"
    assert first.credentials.internal_url == "localhost"
    assert first.credentials.user == "test"
    assert first.credentials.port == 1234
    assert first.credentials.password is None
