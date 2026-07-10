from pathlib import Path
from typing import Dict, Generator, Iterable, List, Optional, Tuple, Union
import xml.etree.ElementTree as ET
import zipfile

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.metabolite_harmonization import (
    HmdbGoClassification,
    HmdbMetaboliteProteinAssociationDetail,
    HmdbMetaboliteProteinAssociationEdge,
    PathwayIdentifier,
    HmdbPfam,
    ProteinIdentifier,
    ProteinPathwayDetail,
    ProteinPathwayEdge,
    HmdbProteinReference,
    MetaboliteIdentifier,
)
from src.input_adapters.metabolite_harmonization.hmdb_pathways import _pathway_id, _pathway_node


HMDB_PROTEINS_XML_MEMBER = "hmdb_proteins.xml"


class HmdbProteinContextAdapter(InputAdapter):
    def __init__(
        self,
        data_source=None,
        hmdb_proteins_zip_file: Optional[str] = None,
        max_records: Optional[int] = None,
    ):
        if data_source is not None:
            hmdb_proteins_zip_file = str(data_source.file("hmdb_proteins.zip"))
            self.version_info = data_source.version_info()
        else:
            self.version_info = DatasourceVersionInfo(version=None, version_date=None, download_date=None)
        if hmdb_proteins_zip_file is None:
            raise ValueError("HmdbProteinContextAdapter requires data_source or hmdb_proteins_zip_file")
        self.hmdb_proteins_zip_file = Path(hmdb_proteins_zip_file)
        self.max_records = max_records

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.HMDB

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def get_all(self) -> Generator[
        List[Union[
            ProteinIdentifier,
            PathwayIdentifier,
            HmdbMetaboliteProteinAssociationEdge,
            ProteinPathwayEdge,
        ]],
        None,
        None,
    ]:
        yield from self._iter_node_batches()
        yield from self._iter_pathway_node_batches()
        yield from self._iter_edge_batches()
        yield from self._iter_protein_pathway_edge_batches()

    def _iter_node_batches(self) -> Generator[List[ProteinIdentifier], None, None]:
        batch: List[ProteinIdentifier] = []
        emitted: set[str] = set()
        for record in self._iter_protein_records():
            protein_id = self._uniprot_id(record.get("uniprot_id"))
            if protein_id is None or protein_id in emitted:
                continue
            emitted.add(protein_id)
            batch.append(self._protein_node(protein_id, record))
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def _iter_pathway_node_batches(self) -> Generator[List[PathwayIdentifier], None, None]:
        batch: List[PathwayIdentifier] = []
        emitted: set[str] = set()
        for record in self._iter_protein_records():
            for pathway in record.get("pathways", []):
                pathway_id = _pathway_id(pathway)
                if pathway_id is None or pathway_id in emitted:
                    continue
                emitted.add(pathway_id)
                batch.append(_pathway_node(pathway_id, pathway, source_field="pathways"))
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []
        if batch:
            yield batch

    def _iter_edge_batches(self) -> Generator[List[HmdbMetaboliteProteinAssociationEdge], None, None]:
        batch: List[HmdbMetaboliteProteinAssociationEdge] = []
        emitted: set[Tuple[str, str]] = set()
        for record in self._iter_protein_records():
            protein_id = self._uniprot_id(record.get("uniprot_id"))
            hmdb_protein_accession = self._clean_text(record.get("accession"))
            if protein_id is None or hmdb_protein_accession is None:
                continue
            protein_node = ProteinIdentifier(id=protein_id)
            references_by_metabolite = self._references_by_metabolite(record)
            for association in record.get("metabolite_associations", []):
                hmdb_metabolite_accession = self._clean_text(association.get("accession"))
                metabolite_id = self._hmdb_metabolite_id(hmdb_metabolite_accession)
                if metabolite_id is None:
                    continue
                edge_key = (metabolite_id, protein_id)
                if edge_key in emitted:
                    continue
                emitted.add(edge_key)
                references = references_by_metabolite.get(hmdb_metabolite_accession, [])
                batch.append(
                    HmdbMetaboliteProteinAssociationEdge(
                        start_node=MetaboliteIdentifier(id=metabolite_id),
                        end_node=protein_node,
                        details=[
                            HmdbMetaboliteProteinAssociationDetail(
                                source="HMDB",
                                source_field="metabolite_associations",
                                hmdb_metabolite_accession=hmdb_metabolite_accession,
                                hmdb_protein_accession=hmdb_protein_accession,
                                metabolite_name=self._clean_text(association.get("name")),
                                protein_type=self._clean_text(record.get("protein_type")),
                                references=references,
                            )
                        ],
                    )
                )
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []
        if batch:
            yield batch

    def _iter_protein_pathway_edge_batches(self) -> Generator[List[ProteinPathwayEdge], None, None]:
        batch: List[ProteinPathwayEdge] = []
        emitted: set[Tuple[str, str]] = set()
        for record in self._iter_protein_records():
            protein_id = self._uniprot_id(record.get("uniprot_id"))
            hmdb_protein_accession = self._clean_text(record.get("accession"))
            if protein_id is None or hmdb_protein_accession is None:
                continue
            for pathway in record.get("pathways", []):
                pathway_id = _pathway_id(pathway)
                if pathway_id is None:
                    continue
                edge_key = (protein_id, pathway_id)
                if edge_key in emitted:
                    continue
                emitted.add(edge_key)
                batch.append(
                    ProteinPathwayEdge(
                        start_node=ProteinIdentifier(id=protein_id),
                        end_node=PathwayIdentifier(id=pathway_id),
                        details=[
                            ProteinPathwayDetail(
                                source="HMDB",
                                source_field="pathways",
                                protein_id=protein_id,
                                hmdb_protein_accession=hmdb_protein_accession,
                                pathway_id=pathway_id,
                                pathway_name=pathway.get("name"),
                                pathway_category=pathway.get("category"),
                            )
                        ],
                    )
                )
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []
        if batch:
            yield batch

    def _iter_protein_records(self) -> Iterable[Dict]:
        count = 0
        with zipfile.ZipFile(self.hmdb_proteins_zip_file) as archive:
            with archive.open(HMDB_PROTEINS_XML_MEMBER) as handle:
                for _event, elem in ET.iterparse(handle, events=("end",)):
                    if self._local_name(elem.tag) != "protein":
                        continue
                    yield self._parse_protein(elem)
                    count += 1
                    elem.clear()
                    if self.max_records is not None and count >= self.max_records:
                        break

    @classmethod
    def _parse_protein(cls, elem) -> Dict:
        record = {
            "accession": None,
            "version": None,
            "creation_date": None,
            "update_date": None,
            "name": None,
            "secondary_accessions": [],
            "protein_type": None,
            "synonyms": [],
            "gene_name": None,
            "general_function": None,
            "specific_function": None,
            "pathways": [],
            "metabolite_associations": [],
            "go_classifications": [],
            "subcellular_locations": [],
            "gene_properties": {},
            "protein_properties": {},
            "genbank_protein_id": None,
            "uniprot_id": None,
            "uniprot_name": None,
            "pdb_ids": [],
            "genbank_gene_id": None,
            "genecard_id": None,
            "geneatlas_id": None,
            "hgnc_id": None,
            "general_references": [],
            "metabolite_references": [],
        }
        scalar_fields = {
            "version",
            "creation_date",
            "update_date",
            "accession",
            "name",
            "protein_type",
            "gene_name",
            "general_function",
            "specific_function",
            "genbank_protein_id",
            "uniprot_id",
            "uniprot_name",
            "genbank_gene_id",
            "genecard_id",
            "geneatlas_id",
            "hgnc_id",
        }
        for child in list(elem):
            tag = cls._local_name(child.tag)
            if tag in scalar_fields:
                record[tag] = cls._clean_text(child.text)
            elif tag == "secondary_accessions":
                record["secondary_accessions"] = cls._child_text_values(child, "accession")
            elif tag == "synonyms":
                record["synonyms"] = cls._child_text_values(child, "synonym")
            elif tag == "metabolite_associations":
                record["metabolite_associations"] = [
                    {
                        "accession": cls._child_text(metabolite, "accession"),
                        "name": cls._child_text(metabolite, "name"),
                    }
                    for metabolite in list(child)
                    if cls._local_name(metabolite.tag) == "metabolite"
                ]
            elif tag == "pathways":
                record["pathways"] = cls._parse_pathway_container(child)
            elif tag == "go_classifications":
                record["go_classifications"] = [
                    HmdbGoClassification(
                        category=cls._child_text(go_class, "category"),
                        description=cls._child_text(go_class, "description"),
                        go_id=cls._child_text(go_class, "go_id"),
                    )
                    for go_class in list(child)
                    if cls._local_name(go_class.tag) == "go_class"
                ]
            elif tag == "subcellular_locations":
                record["subcellular_locations"] = cls._child_text_values(child, "subcellular_location")
            elif tag == "protein_properties":
                record["protein_properties"] = cls._parse_protein_properties(child)
            elif tag == "pdb_ids":
                record["pdb_ids"] = cls._child_text_values(child, "pdb_id")
            elif tag == "general_references":
                record["general_references"] = cls._parse_references(child)
            elif tag == "metabolite_references":
                record["metabolite_references"] = cls._parse_metabolite_references(child)
        return record

    @classmethod
    def _parse_pathway_container(cls, elem) -> List[Dict]:
        pathways = []
        seen = set()
        for pathway in list(elem):
            if cls._local_name(pathway.tag) != "pathway":
                continue
            name = cls._child_text(pathway, "name")
            smpdb_id = cls._child_text(pathway, "smpdb_id")
            kegg_map_id = cls._child_text(pathway, "kegg_map_id")
            for pathway_type, source_id, category in (
                ("smpdb", smpdb_id, "smpdb3"),
                ("kegg", kegg_map_id, "kegg"),
            ):
                if not source_id:
                    continue
                key = (pathway_type, source_id)
                if key in seen:
                    continue
                seen.add(key)
                pathways.append(
                    {
                        "type": pathway_type,
                        "source_id": source_id,
                        "name": name,
                        "category": category,
                        "source_field": "pathways",
                    }
                )
        return pathways

    @classmethod
    def _protein_node(cls, protein_id: str, record: Dict) -> ProteinIdentifier:
        protein_properties = record.get("protein_properties") or {}
        return ProteinIdentifier(
            id=protein_id,
            hmdb_accession=cls._clean_text(record.get("accession")),
            secondary_accessions=record.get("secondary_accessions", []),
            name=cls._clean_text(record.get("name")),
            synonyms=record.get("synonyms", []),
            protein_type=cls._clean_text(record.get("protein_type")),
            gene_name=cls._clean_text(record.get("gene_name")),
            uniprot_name=cls._clean_text(record.get("uniprot_name")),
            general_function=cls._clean_text(record.get("general_function")),
            specific_function=cls._clean_text(record.get("specific_function")),
            genbank_protein_id=cls._clean_text(record.get("genbank_protein_id")),
            genbank_gene_id=cls._clean_text(record.get("genbank_gene_id")),
            genecard_id=cls._clean_text(record.get("genecard_id")),
            geneatlas_id=cls._clean_text(record.get("geneatlas_id")),
            hgnc_id=cls._clean_text(record.get("hgnc_id")),
            subcellular_locations=record.get("subcellular_locations", []),
            go_classifications=record.get("go_classifications", []),
            pfams=protein_properties.get("pfams", []),
            pdb_ids=record.get("pdb_ids", []),
            residue_number=protein_properties.get("residue_number"),
            molecular_weight=protein_properties.get("molecular_weight"),
            theoretical_pi=protein_properties.get("theoretical_pi"),
            transmembrane_regions=protein_properties.get("transmembrane_regions", []),
            signal_regions=protein_properties.get("signal_regions", []),
        )

    @classmethod
    def _parse_protein_properties(cls, elem) -> Dict:
        properties = {
            "residue_number": None,
            "molecular_weight": None,
            "theoretical_pi": None,
            "pfams": [],
            "transmembrane_regions": [],
            "signal_regions": [],
        }
        for child in list(elem):
            tag = cls._local_name(child.tag)
            if tag in {"residue_number", "molecular_weight", "theoretical_pi"}:
                properties[tag] = cls._clean_text(child.text)
            elif tag == "pfams":
                properties["pfams"] = [
                    HmdbPfam(
                        name=cls._child_text(pfam, "name"),
                        pfam_id=cls._child_text(pfam, "pfam_id"),
                    )
                    for pfam in list(child)
                    if cls._local_name(pfam.tag) == "pfam"
                ]
            elif tag in {"transmembrane_regions", "signal_regions"}:
                properties[tag] = cls._child_text_values(child, "region")
        return properties

    @classmethod
    def _parse_references(cls, elem) -> List[HmdbProteinReference]:
        references = []
        for reference in list(elem):
            if cls._local_name(reference.tag) != "reference":
                continue
            ref = HmdbProteinReference(
                pubmed_id=cls._child_text(reference, "pubmed_id"),
                reference_text=cls._child_text(reference, "reference_text"),
            )
            if ref.pubmed_id or ref.reference_text:
                references.append(ref)
        return references

    @classmethod
    def _parse_metabolite_references(cls, elem) -> List[Dict]:
        references = []
        for metabolite_reference in list(elem):
            if cls._local_name(metabolite_reference.tag) != "metabolite_reference":
                continue
            metabolite = cls._child(metabolite_reference, "metabolite")
            reference = cls._child(metabolite_reference, "reference")
            if metabolite is None or reference is None:
                continue
            metabolite_accession = cls._child_text(metabolite, "accession")
            ref = HmdbProteinReference(
                pubmed_id=cls._child_text(reference, "pubmed_id"),
                reference_text=cls._child_text(reference, "reference_text"),
            )
            if metabolite_accession and (ref.pubmed_id or ref.reference_text):
                references.append(
                    {
                        "metabolite_accession": metabolite_accession,
                        "reference": ref,
                    }
                )
        return references

    @staticmethod
    def _references_by_metabolite(record: Dict) -> Dict[str, List[HmdbProteinReference]]:
        references: Dict[str, List[HmdbProteinReference]] = {}
        seen: set[Tuple[str, Optional[str], Optional[str]]] = set()
        for item in record.get("metabolite_references", []):
            metabolite_accession = item["metabolite_accession"]
            reference = item["reference"]
            key = (metabolite_accession, reference.pubmed_id, reference.reference_text)
            if key in seen:
                continue
            seen.add(key)
            references.setdefault(metabolite_accession, []).append(reference)
        return references

    @classmethod
    def _uniprot_id(cls, value: Optional[str]) -> Optional[str]:
        value = cls._clean_text(value)
        if value is None:
            return None
        if value.upper().startswith("UNIPROTKB:"):
            return f"UniProtKB:{value.split(':', 1)[1].strip()}"
        return f"UniProtKB:{value}"

    @classmethod
    def _hmdb_metabolite_id(cls, value: Optional[str]) -> Optional[str]:
        value = cls._clean_text(value)
        if value is None:
            return None
        if value.upper().startswith("HMDB:"):
            return f"HMDB:{value.split(':', 1)[1].strip()}"
        return f"HMDB:{value}"

    @classmethod
    def _child_text_values(cls, elem, child_tag: str) -> List[str]:
        return cls._unique_clean_values(
            cls._clean_text(child.text)
            for child in list(elem)
            if cls._local_name(child.tag) == child_tag
        )

    @classmethod
    def _child_text(cls, elem, child_tag: str) -> Optional[str]:
        child = cls._child(elem, child_tag)
        if child is None:
            return None
        return cls._clean_text(child.text)

    @classmethod
    def _child(cls, elem, child_tag: str):
        for child in list(elem):
            if cls._local_name(child.tag) == child_tag:
                return child
        return None

    @staticmethod
    def _clean_text(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = value.strip()
        return value or None

    @classmethod
    def _unique_clean_values(cls, values: Iterable[Optional[str]]) -> List[str]:
        result = []
        seen = set()
        for value in values:
            value = cls._clean_text(value)
            if not value or value in seen:
                continue
            seen.add(value)
            result.append(value)
        return result

    @staticmethod
    def _local_name(tag: str) -> str:
        return tag.split("}", 1)[-1]
