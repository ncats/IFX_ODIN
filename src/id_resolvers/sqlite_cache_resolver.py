import os.path
import sqlite3
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Dict
from src.interfaces.id_resolver import IdResolver, IdMatch
from src.models.node import Node


@dataclass(frozen=True)
class MatchingPair:
    id: str
    match: str
    type: str


class SqliteCacheResolver(IdResolver, ABC):
    connection: sqlite3.Connection = None

    def cache_location(self):
        return f"input_files/sqlite_resolver/{self.__class__.__name__}.sqlite"

    def __init__(self, **kwargs):
        IdResolver.__init__(self, **kwargs)
        self.connection = self.create_connection()

        if self.db_is_corrupt() or self.input_version_has_changed():
            self.create_lookup_db()
            self.populate_lookup_db()
        else:
            print('\tusing existing sqlite database for id resolution')

    def create_connection(self):
        cache_loc = self.cache_location()
        if not os.path.exists(cache_loc):
            os.makedirs(os.path.dirname(cache_loc), exist_ok=True)
        return sqlite3.connect(cache_loc)

    def db_is_corrupt(self):
        if not self.table_exists('matches'):
            print('\texisting database is missing table "matches"')
            return True
        if not self.table_exists('file_metadata'):
            print('\texisting database is missing table "file_metadata"')
            return True
        return False

    def table_exists(self, table):
        cur = self.connection.cursor()
        cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
        return cur.fetchone() is not None

    def input_version_has_changed(self) -> bool:
        cur = self.connection.cursor()
        cur.execute('SELECT version_key FROM file_metadata')
        sqlite_version_info = cur.fetchone()
        if sqlite_version_info is None:
            print('\texisting database is missing the version key')
            return True
        sqlite_version_info = sqlite_version_info[0]
        input_version_info = self.get_version_info()
        if sqlite_version_info != input_version_info:
            print(f'\texisting data version does not match input version')
            print(f'\t{sqlite_version_info}')
            print(f'\t{input_version_info}')
            return True
        return sqlite_version_info != input_version_info

    def create_lookup_db(self):
        print('\tcreating sqlite cache')
        cur = self.connection.cursor()

        cur.execute('DROP TABLE IF EXISTS matches')
        cur.execute('DROP TABLE IF EXISTS file_metadata')

        cur.execute('CREATE TABLE matches (id TEXT, match TEXT, type TEXT)')
        cur.execute('CREATE INDEX id_index ON matches (id)')
        cur.execute('CREATE INDEX match_index ON matches (match)')
        cur.execute('CREATE TABLE file_metadata (version_key TEXT)')

        self.connection.commit()

    def populate_lookup_db(self):
        matches = list(set([match for match in self.matching_ids()]))
        self.save_to_sqlite(matches)
        self.store_file_metadata()

    def store_file_metadata(self):
        cur = self.connection.cursor()
        cur.execute('DELETE FROM file_metadata')  # Clear any existing metadata
        cur.execute('INSERT INTO file_metadata (version_key) VALUES (?)', (self.get_version_info(),))
        self.connection.commit()

    def save_to_sqlite(self, matches: List[MatchingPair]):
        cur = self.connection.cursor()
        cur.executemany('INSERT INTO matches VALUES (?, ?, ?)',
                  [(match.id, match.match, match.type) for match in matches])
        self.connection.commit()

    def __del__(self):
        if self.connection is not None:
            self.connection.close()

    @abstractmethod
    def get_version_info(self) -> str:
        raise NotImplementedError('derived class must implement get_version_info')

    @abstractmethod
    def matching_ids(self) -> List[MatchingPair]:
        raise NotImplementedError('derived class must implement matching_ids')

    def resolve_internal(self, input_nodes: List[Node]) -> Dict[str, List[IdMatch]]:
        result_list = {}
        id_list = [node.id for node in input_nodes]
        cur = self.connection.cursor()
        max_vars = 50000
        id_chunks = [id_list[i:i + max_vars] for i in range(0, len(id_list), max_vars)]
        id_matches = []
        for chunk in id_chunks:
            cur.execute('SELECT id, match, type FROM matches WHERE match IN ({})'.format(','.join('?' * len(chunk))), tuple(chunk))
            id_matches.extend(cur.fetchall())

        if len(id_matches) > 0:
            resolved_id_list = set()
            for (resolved_id, input_id, type) in id_matches:
                resolved_id_list.add(resolved_id)
                match_type = 'exact' if resolved_id == input_id else type
                if input_id not in result_list:
                    result_list[input_id] = [IdMatch(input=input_id, match=resolved_id, equivalent_ids=[], context=[match_type])]
                else:
                    result_list[input_id].append(IdMatch(input=input_id, match=resolved_id, equivalent_ids=[], context=[match_type]))

            equiv_id_matches = []
            for chunk in [list(resolved_id_list)[i:i + max_vars] for i in range(0, len(resolved_id_list), max_vars)]:
                cur.execute("SELECT id, match FROM matches WHERE id IN ({})".format(','.join('?' * len(chunk))), tuple(chunk))
                equiv_id_matches.extend(cur.fetchall())

            match_map = {}
            for (resolved_id, matching_id) in equiv_id_matches:
                if resolved_id not in match_map:
                    match_map[resolved_id] = []
                match_map[resolved_id].append(matching_id)

            for match_list in result_list.values():
                for match in match_list:
                    match.equivalent_ids = match_map.get(match.match, [])

        return result_list
