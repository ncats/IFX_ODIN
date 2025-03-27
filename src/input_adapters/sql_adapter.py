from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.shared.db_credentials import DBCredentials


class SqlAdapter:
    connection_string: str

    def __init__(self, connection_string):
        self.connection_string = connection_string

    def get_session(self):
        engine = create_engine(self.connection_string)
        Session = sessionmaker(bind=engine)
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
