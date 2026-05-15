from __future__ import annotations

import calendar
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Iterable, List, Optional


MONTH_ABBR_TO_NUM = {abbr: index for index, abbr in enumerate(calendar.month_abbr) if abbr}


@dataclass(frozen=True)
class ArticleRecord:
    pmid: int
    title: str
    journal: Optional[str]
    publication_date: Optional[str]
    pub_year: Optional[int]
    authors: Optional[str]
    abstract: Optional[str]
    pmc_id: Optional[str]
    doi: Optional[str]
    publication_status: Optional[str]
    publication_type: Optional[str]
    language: Optional[str]
    mesh_assignments: List["MeshAssignment"]


@dataclass(frozen=True)
class MeshAssignment:
    descriptor_ui: str
    descriptor_name: str
    descriptor_major_topic: bool
    qualifier_ui: Optional[str]
    qualifier_name: Optional[str]
    qualifier_major_topic: Optional[bool]


class PubMedParser:
    pubmed_article_selector = "PubmedArticle"
    deleted_article_selector = "DeleteCitation/PMID"
    article_selector = "MedlineCitation/Article"
    pmid_selector = "MedlineCitation/PMID"
    history_date_selector = "PubmedData/History/PubMedPubDate"
    article_title_selector = "ArticleTitle"
    journal_title_selector = "Journal/Title"
    journal_pub_date_selector = "Journal/JournalIssue/PubDate"
    abstract_text_selector = "Abstract/AbstractText"
    author_selector = "AuthorList/Author"
    article_id_selector = "PubmedData/ArticleIdList/ArticleId"
    publication_status_selector = "PubmedData/PublicationStatus"
    publication_type_selector = "PublicationTypeList/PublicationType"
    language_selector = "Language"
    mesh_heading_selector = "MedlineCitation/MeshHeadingList/MeshHeading"

    @classmethod
    def parse_articles(cls, xml_text: str) -> List[ArticleRecord]:
        root = ET.fromstring(cls._sanitize_xml(xml_text))
        records: List[ArticleRecord] = []
        for article_node in root.findall(cls.pubmed_article_selector):
            record = cls._parse_article_node(article_node)
            if record is not None:
                records.append(record)
        return records

    @classmethod
    def parse_deleted_pmids(cls, xml_text: str) -> List[int]:
        root = ET.fromstring(cls._sanitize_xml(xml_text))
        deleted_ids: List[int] = []
        for node in root.findall(cls.deleted_article_selector):
            raw_value = cls._flatten_text(node)
            if raw_value is None:
                continue
            raw_value = raw_value.strip()
            if raw_value.isdigit():
                deleted_ids.append(int(raw_value))
        return deleted_ids

    @classmethod
    def _parse_article_node(cls, pubmed_article_node: ET.Element) -> Optional[ArticleRecord]:
        article_node = pubmed_article_node.find(cls.article_selector)
        if article_node is None:
            return None

        pmid = cls._parse_pmid(pubmed_article_node.find(cls.pmid_selector))
        if pmid is None:
            return None

        title = cls._flatten_text(article_node.find(cls.article_title_selector)) or ""
        if not title:
            title = f"PMID {pmid}"

        journal = cls._flatten_text(article_node.find(cls.journal_title_selector))
        article_date = cls._parse_date_node(article_node.find(cls.journal_pub_date_selector))
        history_date = cls._parse_date_node(pubmed_article_node.find(cls.history_date_selector))
        publication_date = article_date or history_date
        pub_year = cls._derive_year(publication_date)
        authors = cls._format_authors(article_node.findall(cls.author_selector))
        abstract = cls._format_abstract(article_node.findall(cls.abstract_text_selector))
        pmc_id = cls._find_article_id(pubmed_article_node.findall(cls.article_id_selector), "pmc")
        doi = cls._find_article_id(pubmed_article_node.findall(cls.article_id_selector), "doi")
        publication_status = cls._flatten_text(pubmed_article_node.find(cls.publication_status_selector))
        publication_type = cls._join_unique_text(article_node.findall(cls.publication_type_selector))
        language = cls._join_unique_text(article_node.findall(cls.language_selector))
        mesh_assignments = cls._parse_mesh_assignments(pubmed_article_node.findall(cls.mesh_heading_selector))

        return ArticleRecord(
            pmid=pmid,
            title=title,
            journal=journal,
            publication_date=publication_date,
            pub_year=pub_year,
            authors=authors,
            abstract=abstract,
            pmc_id=pmc_id,
            doi=doi,
            publication_status=publication_status,
            publication_type=publication_type,
            language=language,
            mesh_assignments=mesh_assignments,
        )

    @staticmethod
    def _sanitize_xml(xml_text: str) -> str:
        return re.sub(r"</?su[bp]>|</?i>|</?b>", "", xml_text)

    @staticmethod
    def _flatten_text(node: Optional[ET.Element]) -> Optional[str]:
        if node is None:
            return None
        text = "".join(node.itertext()).strip()
        return text or None

    @classmethod
    def _join_unique_text(cls, nodes: Iterable[ET.Element]) -> Optional[str]:
        ordered_values: List[str] = []
        seen_values: set[str] = set()
        for node in nodes:
            value = cls._flatten_text(node)
            if value is None or value in seen_values:
                continue
            seen_values.add(value)
            ordered_values.append(value)
        if not ordered_values:
            return None
        return "; ".join(ordered_values)

    @classmethod
    def _find_article_id(cls, nodes: Iterable[ET.Element], id_type: str) -> Optional[str]:
        for node in nodes:
            if node.attrib.get("IdType") != id_type:
                continue
            value = cls._flatten_text(node)
            if value is not None:
                return value
        return None

    @classmethod
    def _parse_pmid(cls, node: Optional[ET.Element]) -> Optional[int]:
        raw_value = cls._flatten_text(node)
        if raw_value is None or not raw_value.isdigit():
            return None
        return int(raw_value)

    @classmethod
    def _parse_date_node(cls, node: Optional[ET.Element]) -> Optional[str]:
        if node is None:
            return None

        year = cls._flatten_text(node.find("Year"))
        if year and year.isdigit():
            month = cls._normalize_month(cls._flatten_text(node.find("Month")))
            day = cls._normalize_day(cls._flatten_text(node.find("Day")))
            if month is None:
                return year
            if day is None:
                return f"{year}-{month}"
            return f"{year}-{month}-{day}"

        medline_date = cls._flatten_text(node.find("MedlineDate"))
        if medline_date:
            return cls._parse_medline_date(medline_date)

        return None

    @staticmethod
    def _normalize_month(raw_month: Optional[str]) -> Optional[str]:
        if raw_month is None:
            return None
        month = raw_month.strip()
        if not month:
            return None
        if month.isdigit():
            return month.zfill(2)
        short_month = month[:3].title()
        month_number = MONTH_ABBR_TO_NUM.get(short_month)
        return str(month_number).zfill(2) if month_number else None

    @staticmethod
    def _normalize_day(raw_day: Optional[str]) -> Optional[str]:
        if raw_day is None:
            return None
        day = raw_day.strip()
        if not day or not day.isdigit():
            return None
        return day.zfill(2)

    @staticmethod
    def _parse_medline_date(raw_value: str) -> Optional[str]:
        raw_value = raw_value.strip()
        match = re.search(r"\b(\d{4})\b", raw_value)
        if not match:
            return None
        return match.group(1)

    @staticmethod
    def _derive_year(publication_date: Optional[str]) -> Optional[int]:
        if publication_date is None:
            return None
        match = re.match(r"^(\d{4})", publication_date)
        return int(match.group(1)) if match else None

    @classmethod
    def _parse_mesh_assignments(cls, mesh_heading_nodes: Iterable[ET.Element]) -> List[MeshAssignment]:
        assignments: List[MeshAssignment] = []
        for mesh_heading in mesh_heading_nodes:
            descriptor_node = mesh_heading.find("DescriptorName")
            descriptor_ui = descriptor_node.attrib.get("UI") if descriptor_node is not None else None
            descriptor_name = cls._flatten_text(descriptor_node)
            if not descriptor_ui or not descriptor_name:
                continue

            descriptor_major_topic = descriptor_node.attrib.get("MajorTopicYN") == "Y"
            qualifier_nodes = mesh_heading.findall("QualifierName")
            if not qualifier_nodes:
                assignments.append(
                    MeshAssignment(
                        descriptor_ui=descriptor_ui,
                        descriptor_name=descriptor_name,
                        descriptor_major_topic=descriptor_major_topic,
                        qualifier_ui=None,
                        qualifier_name=None,
                        qualifier_major_topic=None,
                    )
                )
                continue

            for qualifier_node in qualifier_nodes:
                qualifier_ui = qualifier_node.attrib.get("UI")
                qualifier_name = cls._flatten_text(qualifier_node)
                if not qualifier_ui or not qualifier_name:
                    continue
                assignments.append(
                    MeshAssignment(
                        descriptor_ui=descriptor_ui,
                        descriptor_name=descriptor_name,
                        descriptor_major_topic=descriptor_major_topic,
                        qualifier_ui=qualifier_ui,
                        qualifier_name=qualifier_name,
                        qualifier_major_topic=qualifier_node.attrib.get("MajorTopicYN") == "Y",
                    )
                )
        return assignments

    @classmethod
    def _format_abstract(cls, abstract_nodes: Iterable[ET.Element]) -> Optional[str]:
        parts: List[str] = []
        for node in abstract_nodes:
            label = node.attrib.get("Label")
            body = cls._flatten_text(node)
            if body is None:
                continue
            if label:
                parts.append(f"{label}: {body}")
            else:
                parts.append(body)
        if not parts:
            return None
        return "\n\n".join(parts)

    @classmethod
    def _format_authors(cls, author_nodes: Iterable[ET.Element]) -> Optional[str]:
        named_authors: List[str] = []
        collective_authors: List[str] = []

        for author in author_nodes:
            collective_name = cls._flatten_text(author.find("CollectiveName"))
            if collective_name:
                collective_authors.append(collective_name)
                continue

            forename = cls._flatten_text(author.find("ForeName"))
            last_name = cls._flatten_text(author.find("LastName"))
            if forename and last_name:
                named_authors.append(f"{forename} {last_name}")
            elif last_name:
                named_authors.append(last_name)
            elif forename:
                named_authors.append(forename)

        joined_named = ", ".join(named_authors)
        joined_collective = ", ".join(collective_authors)
        if joined_named and joined_collective:
            return f"{joined_named}; {joined_collective}"
        return joined_named or joined_collective or None
