from pathlib import Path
from typing import Dict, Generator, Iterable, List, Optional, Set, Tuple, Union
import re
import zipfile

from rdflib import Graph, URIRef, RDF, RDFS

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.metabolite_harmonization import (
    MetaboliteIdentifier,
    MetaboliteIdentifierMappingDetail,
    MetaboliteIdentifierMappingEdge,
    MetaboliteName,
)


WP = "http://vocabularies.wikipathways.org/wp#"
PUBCHEM_COMPOUND_RDF_PREFIX = "http://rdf.ncbi.nlm.nih.gov/pubchem/compound/CID"
WIKIDATA_ENTITY_PREFIX = "http://www.wikidata.org/entity/"
IDENTIFIERS_ORG_PREFIX = "https://identifiers.org/"

IDENTIFIERS_ORG_PREFIXES = {
    "cas": "CAS",
    "chebi": "CHEBI",
    "chembl.compound": "ChEMBL.COMPOUND",
    "chemspider": "ChemSpider",
    "drugbank": "DRUGBANK",
    "hmdb": "HMDB",
    "inchikey": "InChIKey",
    "kegg.compound": "KEGG.COMPOUND",
    "kegg.drug": "KEGG.DRUG",
    "kegg.glycan": "KEGG.GLYCAN",
    "knapsack": "KNApSAcK",
    "lipidmaps": "LIPIDMAPS",
    "pharmgkb.drug": "PharmGKB.DRUG",
    "pid.pathway": "PID.PATHWAY",
    "pubchem.compound": "PUBCHEM.COMPOUND",
    "pubchem.substance": "PUBCHEM.SUBSTANCE",
    "reactome": "Reactome",
    "wikidata": "Wikidata",
}


class WikiPathwaysMetaboliteEquivalenceAdapter(InputAdapter):
    def __init__(
        self,
        data_source=None,
        rdf_zip_file: Optional[str] = None,
        max_files: Optional[int] = None,
        max_records: Optional[int] = None,
    ):
        if data_source is not None:
            rdf_zip_file = str(data_source.file())
            self.version_info = data_source.version_info()
        else:
            self.version_info = DatasourceVersionInfo(
                version=None,
                version_date=None,
                download_date=None,
            )
        if rdf_zip_file is None:
            raise ValueError("WikiPathwaysMetaboliteEquivalenceAdapter requires data_source or rdf_zip_file")
        self.rdf_zip_file = Path(rdf_zip_file)
        self.max_files = max_files
        self.max_records = max_records

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.WikiPathways

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def get_all(self) -> Generator[
        List[Union[MetaboliteIdentifier, MetaboliteIdentifierMappingEdge]],
        None,
        None,
    ]:
        yield from self._iter_node_batches()
        yield from self._iter_edge_batches()

    def _iter_node_batches(self) -> Generator[List[MetaboliteIdentifier], None, None]:
        batch: List[MetaboliteIdentifier] = []
        emitted_ids: Set[str] = set()
        emitted_labeled_records: Set[Tuple[str, Tuple[str, ...]]] = set()

        for record in self._iter_metabolite_records():
            source_id = record["source_id"]
            label_key = (source_id, tuple(record["labels"]))
            if label_key not in emitted_labeled_records:
                batch.append(self._source_node(record))
                emitted_labeled_records.add(label_key)
                emitted_ids.add(source_id)

            for _source_field, node_id in record["xrefs"]:
                if node_id == source_id or node_id in emitted_ids:
                    continue
                batch.append(MetaboliteIdentifier(id=node_id))
                emitted_ids.add(node_id)
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []

            if len(batch) >= self.batch_size:
                yield batch
                batch = []

        if batch:
            yield batch

    def _iter_edge_batches(self) -> Generator[List[MetaboliteIdentifierMappingEdge], None, None]:
        batch: List[MetaboliteIdentifierMappingEdge] = []
        emitted_details: Set[Tuple[str, str, str, str, str]] = set()

        for record in self._iter_metabolite_records():
            source_id = record["source_id"]
            for source_field, end_id in record["xrefs"]:
                if end_id == source_id:
                    continue
                detail_key = (source_id, end_id, "WikiPathways", source_field, source_id)
                if detail_key in emitted_details:
                    continue
                emitted_details.add(detail_key)
                batch.append(
                    MetaboliteIdentifierMappingEdge(
                        start_node=MetaboliteIdentifier(id=source_id),
                        end_node=MetaboliteIdentifier(id=end_id),
                        details=[
                            MetaboliteIdentifierMappingDetail(
                                source="WikiPathways",
                                source_field=source_field,
                                source_id=source_id,
                            )
                        ],
                    )
                )
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []

        if batch:
            yield batch

    def _iter_metabolite_records(self) -> Iterable[Dict]:
        count = 0
        with zipfile.ZipFile(self.rdf_zip_file) as archive:
            ttl_members = [
                info.filename
                for info in archive.infolist()
                if info.filename.endswith(".ttl")
            ]
            for file_idx, member in enumerate(ttl_members):
                if self.max_files is not None and file_idx >= self.max_files:
                    break
                graph = Graph()
                graph.parse(data=archive.read(member).decode("utf-8", "ignore"), format="turtle")
                for subject in graph.subjects(RDF.type, URIRef(WP + "Metabolite")):
                    record = self._parse_metabolite(graph, subject)
                    if record is None:
                        continue
                    yield record
                    count += 1
                    if self.max_records is not None and count >= self.max_records:
                        return

    @classmethod
    def _parse_metabolite(cls, graph: Graph, subject) -> Optional[Dict]:
        source_id = cls._normalize_uri(str(subject))
        if source_id is None:
            return None

        labels = cls._unique_clean_values(str(label) for label in graph.objects(subject, RDFS.label))
        xrefs = [("subject", source_id)]

        for predicate, obj in graph.predicate_objects(subject):
            predicate_uri = str(predicate)
            if not predicate_uri.startswith(WP + "bdb"):
                continue
            node_id = cls._normalize_uri(str(obj))
            if node_id is None:
                continue
            source_field = predicate_uri.rsplit("#", 1)[-1]
            xrefs.append((source_field, node_id))

        return {
            "source_id": source_id,
            "labels": labels,
            "xrefs": cls._unique_xrefs(xrefs),
        }

    @staticmethod
    def _source_node(record: Dict) -> MetaboliteIdentifier:
        names = [
            MetaboliteName(
                value=label,
                source="WikiPathways",
                source_field="rdfs:label",
            )
            for label in record["labels"]
        ]
        return MetaboliteIdentifier(id=record["source_id"], names=names)

    @classmethod
    def _normalize_uri(cls, uri: str) -> Optional[str]:
        uri = uri.strip()
        if uri.startswith(PUBCHEM_COMPOUND_RDF_PREFIX):
            return cls._prefixed_id("PUBCHEM.COMPOUND", uri[len(PUBCHEM_COMPOUND_RDF_PREFIX):])
        if uri.startswith(WIKIDATA_ENTITY_PREFIX):
            return cls._prefixed_id("Wikidata", uri[len(WIKIDATA_ENTITY_PREFIX):])
        if uri.startswith(IDENTIFIERS_ORG_PREFIX):
            suffix = uri[len(IDENTIFIERS_ORG_PREFIX):]
            if "/" not in suffix:
                return None
            family, value = suffix.split("/", 1)
            prefix = IDENTIFIERS_ORG_PREFIXES.get(family)
            if prefix is None:
                return None
            return cls._prefixed_id(prefix, value)
        return None

    @staticmethod
    def _prefixed_id(prefix: str, value: str) -> Optional[str]:
        value = value.strip()
        if not value:
            return None
        if prefix == "CHEBI" and value.upper().startswith("CHEBI:"):
            value = value.split(":", 1)[1]
        if prefix == "PUBCHEM.COMPOUND":
            value = re.sub(r"^CID", "", value, flags=re.IGNORECASE)
        return f"{prefix}:{value}"

    @staticmethod
    def _unique_clean_values(values: Iterable[Optional[str]]) -> List[str]:
        result = []
        seen = set()
        for value in values:
            if value is None:
                continue
            value = value.strip()
            if not value or value in seen:
                continue
            seen.add(value)
            result.append(value)
        return result

    @staticmethod
    def _unique_xrefs(values: Iterable[Tuple[str, str]]) -> List[Tuple[str, str]]:
        result = []
        seen = set()
        for source_field, node_id in values:
            key = (source_field, node_id)
            if key in seen:
                continue
            seen.add(key)
            result.append(key)
        return result
