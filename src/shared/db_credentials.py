from dataclasses import dataclass


@dataclass
class DBCredentials:
    url: str
    user: str
    password: str
    port: int = None
    schema: str = None
    internal_url: str = None