import subprocess
from dataclasses import dataclass, field
from typing import List, Dict

from src.models.datasource_version_info import DataSourceDetails


@dataclass
class CollectionMetadata:
    name: str
    total_count: int
    sources: List[DataSourceDetails] = field(default_factory=list)
    marginal_source_counts: Dict[str, int] = field(default_factory=dict)
    joint_source_counts: Dict[str, int] = field(default_factory=dict)

    def to_dict(self):
        return {
            "name": self.name,
            "total_count": self.total_count,
            "sources": [s.to_tsv() for s in self.sources],
            "marginal_source_counts": self.marginal_source_counts,
            "joint_source_counts": self.joint_source_counts
        }

    @staticmethod
    def from_dict(data):
        ret_obj = CollectionMetadata(
            name=data['name'],
            total_count=data['total_count'],
            sources=[DataSourceDetails.parse_tsv(source) for source in data['sources']],
            marginal_source_counts=data['marginal_source_counts'],
            joint_source_counts=data['joint_source_counts']
        )
        return ret_obj



@dataclass
class DatabaseMetadata:
    collections: List[CollectionMetadata]

    def to_dict(self):
        return [c.to_dict() for c in self.collections]

    @staticmethod
    def from_dict(collection_rows: List[any]):
        if collection_rows and len(collection_rows) > 0:
            return DatabaseMetadata(
                collections=[CollectionMetadata.from_dict(row) for row in collection_rows])
        return DatabaseMetadata(collections=[])


def get_git_metadata():
    def safe_run(cmd):
        try:
            return subprocess.check_output(cmd, stderr=subprocess.DEVNULL).decode().strip()
        except subprocess.CalledProcessError:
            return None

    return {
        "git_commit": safe_run(["git", "rev-parse", "HEAD"]),
        "git_branch": safe_run(["git", "rev-parse", "--abbrev-ref", "HEAD"]),
        "git_tag": safe_run(["git", "describe", "--tags", "--abbrev=0"]),
    }



