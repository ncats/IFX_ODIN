import re

from arango import ArangoClient
from arango.database import StandardDatabase
from arango.graph import Graph

from src.shared.db_credentials import DBCredentials


class ArangoAdapter:
    credentials: DBCredentials
    use_internal_url: bool
    database_name: str
    client: ArangoClient = None
    db: StandardDatabase = None

    def __init__(self, credentials: DBCredentials, database_name: str, internal: bool = False):
        self.use_internal_url = internal
        self.credentials = credentials
        self.database_name = database_name
        self.initialize()

    @staticmethod
    def safe_key(key):
        key = key.strip()
        key = re.sub(r'\s+', '_', key)
        key = re.sub(r'[^a-zA-Z0-9_\-\.@()\+,=;\$!\*\'%:]', '', key)
        return key

    def initialize(self):
        url = self.credentials.internal_url if self.use_internal_url else self.credentials.url
        print(f"Connecting to ArangoDB at {url}")
        self.client = ArangoClient(hosts=url, request_timeout=600)

    def get_db(self):
        if self.db is None:
            self.db = self.client.db(self.database_name, username=self.credentials.user,
                                     password=self.credentials.password)
        return self.db

    def get_graph(self) -> Graph:
        db = self.get_db()
        if not db.has_graph("graph"):
            db.create_graph("graph")
        return db.graph("graph")

    def runQuery(self, query: str, bind_vars: dict = None):
        db = self.get_db()
        cursor = db.aql.execute(query, bind_vars=bind_vars or {})
        return list(cursor)

