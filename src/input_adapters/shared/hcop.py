import csv
import gzip
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Generator, Iterable, List, Optional, Set

from src.constants import Prefix
from src.models.node import EquivalentId

HCOP_TAXID_TO_SPECIES = {
    "4932": "Saccharomyces cerevisiae",
    "6239": "Caenorhabditis elegans",
    "7227": "Drosophila melanogaster",
    "7955": "Danio rerio",
    "8364": "Xenopus tropicalis",
    "9031": "Gallus gallus",
    "9258": "Ornithorhynchus anatinus",
    "9544": "Macaca mulatta",
    "9598": "Pan troglodytes",
    "9615": "Canis lupus familiaris",
    "9796": "Equus caballus",
    "9823": "Sus scrofa",
    "9913": "Bos taurus",
    "10090": "Mus musculus",
    "10116": "Rattus norvegicus",
    "13616": "Monodelphis domestica",
    "28377": "Anolis carolinensis",
}


@dataclass
class HCOPFilterConfig:
    accepted_species: Optional[Set[str]] = None
    drop_blank_ortholog_identity: bool = True


class HCOPRecordHelper:
    preferred_db_id_prefixes = {Prefix.MGI.value, Prefix.RGD.value}

    def __init__(self,
                 file_path: str,
                 accepted_species: Optional[Iterable[str]] = None,
                 drop_blank_ortholog_identity: bool = True):
        self.file_path = file_path
        self.filter_config = HCOPFilterConfig(
            accepted_species=None if accepted_species is None else {str(x) for x in accepted_species},
            drop_blank_ortholog_identity=drop_blank_ortholog_identity,
        )

    def iter_accepted_rows(self) -> Generator[Dict[str, str], None, None]:
        with self._open_reader() as reader:
            for row in reader:
                if self.row_is_accepted(row):
                    yield row

    def row_is_accepted(self, row: Dict[str, str]) -> bool:
        taxid = self.ortholog_species(row)
        if self.filter_config.accepted_species is not None and taxid not in self.filter_config.accepted_species:
            return False

        if self.filter_config.drop_blank_ortholog_identity:
            if self.ortholog_symbol(row) in ("", "-") and self.ortholog_name(row) in ("", "-"):
                return False

        return True

    @staticmethod
    def support_sources(row: Dict[str, str]) -> Set[str]:
        support = row.get("support") or ""
        return {part.strip() for part in support.split(",") if part.strip()}

    @staticmethod
    def ortholog_species(row: Dict[str, str]) -> str:
        return (row.get("ortholog_species") or "").strip()

    @staticmethod
    def ortholog_symbol(row: Dict[str, str]) -> str:
        return (row.get("ortholog_species_symbol") or "").strip()

    @staticmethod
    def ortholog_name(row: Dict[str, str]) -> str:
        return (row.get("ortholog_species_name") or "").strip()

    @staticmethod
    def ortholog_db_id(row: Dict[str, str]) -> Optional[str]:
        value = (row.get("ortholog_species_db_id") or "").strip()
        return value if value and value != "-" else None

    @staticmethod
    def ortholog_entrez_gene_id(row: Dict[str, str]) -> Optional[str]:
        value = (row.get("ortholog_species_entrez_gene") or "").strip()
        return value if value and value != "-" else None

    @staticmethod
    def ortholog_ensembl_gene_id(row: Dict[str, str]) -> Optional[str]:
        value = (row.get("ortholog_species_ensembl_gene") or "").strip()
        return value if value and value != "-" else None

    def ortholog_curies_from_row(self, row: Dict[str, str]) -> List[str]:
        curies: List[str] = []

        db_id = self.ortholog_db_id(row)
        if db_id and self._is_supported_prefixed_id(db_id):
            curies.append(db_id)

        geneid = self.ortholog_entrez_gene_id(row)
        if geneid:
            curies.append(EquivalentId(id=geneid, type=Prefix.NCBIGene).id_str())

        ensembl = self.ortholog_ensembl_gene_id(row)
        if ensembl:
            curies.append(EquivalentId(id=ensembl, type=Prefix.ENSEMBL).id_str())

        return list(dict.fromkeys(curies))

    def preferred_ortholog_curie(self, row: Dict[str, str]) -> Optional[str]:
        db_id = self.ortholog_db_id(row)
        if db_id and self._is_preferred_source_db_id(db_id):
            return db_id

        geneid = self.ortholog_entrez_gene_id(row)
        if geneid:
            return EquivalentId(id=geneid, type=Prefix.NCBIGene).id_str()

        ensembl = self.ortholog_ensembl_gene_id(row)
        if ensembl:
            return EquivalentId(id=ensembl, type=Prefix.ENSEMBL).id_str()

        if db_id and self._is_supported_prefixed_id(db_id):
            return db_id

        return None

    @staticmethod
    def preferred_human_gene_curie(row: Dict[str, str]) -> Optional[str]:
        geneid = (row.get("human_entrez_gene") or "").strip()
        if geneid and geneid != "-":
            return EquivalentId(id=geneid, type=Prefix.NCBIGene).id_str()

        ensembl = (row.get("human_ensembl_gene") or "").strip()
        if ensembl and ensembl != "-":
            return EquivalentId(id=ensembl, type=Prefix.ENSEMBL).id_str()

        symbol = (row.get("human_symbol") or "").strip()
        if symbol and symbol != "-":
            return EquivalentId(id=symbol, type=Prefix.Symbol).id_str()

        return None

    def _open_reader(self):
        path = Path(self.file_path)
        if path.suffix == ".gz":
            handle = gzip.open(path, "rt", newline="")
        else:
            handle = path.open("r", newline="")
        return _HCOPReaderContext(handle)

    @staticmethod
    def _is_supported_prefixed_id(curie: str) -> bool:
        if ":" not in curie:
            return False
        prefix = curie.split(":", 1)[0]
        return Prefix.parse(prefix) is not None

    @classmethod
    def _is_preferred_source_db_id(cls, curie: str) -> bool:
        if not cls._is_supported_prefixed_id(curie):
            return False
        prefix = curie.split(":", 1)[0]
        return prefix in cls.preferred_db_id_prefixes


class _HCOPReaderContext:
    def __init__(self, handle):
        self.handle = handle

    def __enter__(self):
        return csv.DictReader(self.handle, delimiter="\t")

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.handle.close()
