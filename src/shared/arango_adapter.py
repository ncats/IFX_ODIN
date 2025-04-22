from arango import ArangoClient
from arango.database import StandardDatabase
from src.shared.db_credentials import DBCredentials


class ArangoAdapter():
    credentials: DBCredentials
    database_name: str
    client: ArangoClient = None
    db: StandardDatabase = None

    def __init__(self, credentials: DBCredentials, database_name: str):
        self.credentials = credentials
        self.database_name = database_name
        self.initialize()

    def initialize(self):
        self.client = ArangoClient(hosts=self.credentials.url, request_timeout=600)

    def get_db(self):
        if self.db is None:
            self.db = self.client.db(self.database_name, username=self.credentials.user,
                                     password=self.credentials.password)
        return self.db

    def get_graph(self):
        db = self.get_db()
        if not db.has_graph("graph"):
            db.create_graph("graph")
        return db.graph("graph")

    def runQuery(self, query: str, bind_vars: dict = None):
        db = self.get_db()
        cursor = db.aql.execute(query, bind_vars=bind_vars or {})
        return list(cursor)

