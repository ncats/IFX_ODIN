from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

from src.shared.db_credentials import DBCredentials


class SqlAdapter:
    connection_string: str

    def __init__(self, connection_string):
        self.set_connection_string(connection_string)

    def get_engine(self):
        if not hasattr(self, '_engine'):
            self._engine = create_engine(self.connection_string)
        return self._engine

    def get_session(self) -> Session:
        engine = self.get_engine()
        sessionClass = sessionmaker(bind=engine)
        return sessionClass()

    def set_connection_string(self, connection_string: str):
        self.connection_string = connection_string
        if hasattr(self, '_engine'):
            del self._engine


class HostedSqlAdapter(SqlAdapter):
    credentials: DBCredentials
    dialect: str

    def get_connection_string(self):
        port = self.credentials.port or 3306
        if self.credentials.password is None:
            if self.credentials.schema is None:
                return f"{self.dialect}://{self.credentials.user}@{self.credentials.url}:{port}"
            else:
                return f"{self.dialect}://{self.credentials.user}@{self.credentials.url}:{port}/{self.credentials.schema}"
        else:
            if self.credentials.schema is None:
                return f"{self.dialect}://{self.credentials.user}:{self.credentials.password}@{self.credentials.url}:{port}"
            else:
                return f"{self.dialect}://{self.credentials.user}:{self.credentials.password}@{self.credentials.url}:{port}/{self.credentials.schema}"

    def update_database(self, new_database):
        self.credentials.schema = new_database
        self.set_connection_string(self.get_connection_string())

    def __init__(self, credentials: DBCredentials, dialect: str):
        self.credentials = credentials
        self.dialect = dialect
        connection_string = self.get_connection_string()
        super().__init__(connection_string)


class MySqlAdapter(HostedSqlAdapter):
    def __init__(self, credentials: DBCredentials):
        super().__init__(credentials, dialect="mysql+pymysql")

    def recreate_mysql_db(self, db_name):
        engine = self.get_engine()

        with engine.connect() as conn:
            conn.execute(text(f"DROP DATABASE IF EXISTS `{db_name}`"))
            conn.execute(text(f"CREATE DATABASE `{db_name}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"))
            print(f"Created empty MySQL database: {db_name}")
            self.update_database(db_name)



class PostgreSqlAdapter(HostedSqlAdapter):
    def __init__(self, credentials: DBCredentials):
        super().__init__(credentials, dialect="postgresql+psycopg2")


class SqliteAdapter(SqlAdapter):

    def __init__(self, sqlite_file):
        super().__init__(f"sqlite:///{sqlite_file}")
