from dataclasses import dataclass

from src.interfaces.data_api_adapter import APIAdapter


@dataclass
class Dashboard:
    api_adapter: APIAdapter