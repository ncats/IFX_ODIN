from neo4j import Driver, GraphDatabase

from src.shared.db_credentials import DBCredentials


class Neo4jAdapter():
    driver: Driver
    credentials: DBCredentials

    def __init__(self, credentials: DBCredentials):
        self.credentials = credentials
        self.driver = GraphDatabase.driver(credentials.url, auth=(credentials.user, credentials.password))

    def runQuery(self, query: str):
        with self.driver.session() as session:
            return session.run(query).values()
