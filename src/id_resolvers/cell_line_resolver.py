import os
import xml.etree.ElementTree as ET
from typing import List
from src.constants import Prefix
from src.id_resolvers.sqlite_cache_resolver import SqliteCacheResolver, MatchingPair
from src.models.node import EquivalentId


class CellosaurusCellLineResolver(SqliteCacheResolver):
    file_path: str

    def get_version_info(self) -> str:
        metadata = os.stat(self.file_path)
        return f"{self.file_path}\t{metadata.st_mtime}\t{metadata.st_size}"

    def __init__(self, file_path: str, **kwargs):
        self.file_path = file_path
        SqliteCacheResolver.__init__(self, **kwargs)

    def matching_ids(self) -> List[MatchingPair]:
        tree = ET.parse(self.file_path)
        root = tree.getroot()
        for node in root.findall('./cell-line-list/cell-line'):
            accession = node.find('./accession-list/accession[@type="primary"]').text
            id = EquivalentId(id=accession, type=Prefix.Cellosaurus).id_str()
            yield MatchingPair(id=id, match=id, type='exact')
            for xref in node.findall('./xref-list/xref[@category="Cell line databases/resources"]'):
                prefix = Prefix.parse(xref.get('database'))
                accession = xref.get('accession')
                match_id = EquivalentId(id=accession, type=prefix).id_str()
                yield MatchingPair(id=id, match=match_id, type=prefix.value)
