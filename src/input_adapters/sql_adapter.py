from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker

from src.shared.db_credentials import DBCredentials


class SqlAdapter():
    engine: Engine

    def __init__(self, connection_string):
        self.engine = create_engine(connection_string)

    def get_session(self):
        Session = sessionmaker(bind=self.engine)
        return Session()


class MySqlAdapter(SqlAdapter):
    db_credentials: DBCredentials
    def __init__(self, db_credentials: DBCredentials):
        self.db_credentials = db_credentials
        port = db_credentials.port or 3306
        super().__init__(f"mysql+pymysql://{db_credentials.user}:{db_credentials.password}@{db_credentials.url}:{port}/{db_credentials.schema}")


class SqliteAdapter(SqlAdapter):
    
    def __init__(self, sqlite_file):
        super().__init__(f"sqlite:///{sqlite_file}")
