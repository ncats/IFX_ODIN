from dataclasses import dataclass
from datetime import datetime


@dataclass
class DatabaseVersion:
    id: str
    timestamp: datetime
    notes: str


@dataclass
class DataVersion:
    id: str
    name: str
    url: str
    version: str

@dataclass
class DatabaseDataVersionRelationship:
    database: DatabaseVersion
    data: DataVersion