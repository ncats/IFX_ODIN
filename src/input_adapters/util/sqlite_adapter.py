from sqlalchemy import create_engine, Engine
from sqlalchemy.orm import sessionmaker


class SqliteAdapter():
    engine: Engine
    def __init__(self, sqlite_file):
        self.engine = create_engine(f"sqlite:///{sqlite_file}")

    def get_session(self):
        Session = sessionmaker(bind=self.engine)
        return Session()
