from neo4j import Driver, GraphDatabase

from src.shared.db_credentials import DBCredentials

class Neo4jAdapter():
    driver: Driver
    credentials: DBCredentials

    def __init__(self, db_credentials: DBCredentials):
        self.credentials = db_credentials
        self.driver = GraphDatabase.driver(db_credentials.url, auth=(db_credentials.user, db_credentials.password))

    def runQuery(self, query: str):
        with self.driver.session() as session:
            return session.run(query).values()
