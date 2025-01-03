from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker

from src.shared.db_credentials import DBCredentials


class SqlAdapter:
    engine: Engine

    def __init__(self, connection_string):
        self.engine = create_engine(connection_string)

    def get_session(self):
        Session = sessionmaker(bind=self.engine)
        return Session()


class HostedSqlAdapter(SqlAdapter):
    credentials: DBCredentials
    dialect: str

    def __init__(self, credentials: DBCredentials, dialect: str):
        self.credentials = credentials
        self.dialect = dialect
        port = credentials.port or 3306
        connection_string = f"{dialect}://{credentials.user}:{credentials.password}@{credentials.url}:{port}/{credentials.schema}"
        if self.credentials.password is None:
            connection_string = f"{dialect}://{credentials.user}@{credentials.url}:{port}/{credentials.schema}"
        super().__init__(connection_string)


class MySqlAdapter(HostedSqlAdapter):
    def __init__(self, credentials: DBCredentials):
        super().__init__(credentials, dialect="mysql+pymysql")


class PostgreSqlAdapter(HostedSqlAdapter):
    def __init__(self, credentials: DBCredentials):
        super().__init__(credentials, dialect="postgresql+psycopg2")


class SqliteAdapter(SqlAdapter):

    def __init__(self, sqlite_file):
        super().__init__(f"sqlite:///{sqlite_file}")
