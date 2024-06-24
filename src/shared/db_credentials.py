from dataclasses import dataclass


@dataclass
class DBCredentials:
    url: str
    user: str
    password: str