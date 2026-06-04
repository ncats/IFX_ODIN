from dataclasses import dataclass
from typing import Optional


@dataclass
class NcatsDataSourceInfo:
    dataSource: str
    dataSourceDescription: Optional[str] = None
    url: Optional[str] = None
    license: Optional[str] = None
    licenseURL: Optional[str] = None
    citation: Optional[str] = None


@dataclass
class NcatsDataSourceMapEntry:
    dataSource: str
    url: Optional[str] = None
    license: Optional[str] = None
    licenseURL: Optional[str] = None
    protein_id: Optional[int] = None
    protein_ifx_id: Optional[str] = None
    ncats_ligand_id: Optional[int] = None
    ligand_identifier: Optional[str] = None
    disease_name: Optional[str] = None
