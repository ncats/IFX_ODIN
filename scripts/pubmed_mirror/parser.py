from __future__ import annotations

import calendar
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
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

        return ArticleRecord(
            pmid=pmid,
            title=title,
            journal=journal,
            publication_date=publication_date,
            pub_year=pub_year,
            authors=authors,
            abstract=abstract,
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
