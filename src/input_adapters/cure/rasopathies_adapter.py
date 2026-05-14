import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Generator, List, Union

from src.constants import DataSourceName
from src.input_adapters.flat_file_adapter import FlatFileAdapter
from src.models.cure.pasc.condition import Condition
from src.models.cure.rasopathies.finding import Finding, PresentationFindingEdge
from src.models.cure.rasopathies.presentation import (
    CaseReportPresentationEdge,
    Presentation,
    PresentationConditionEdge,
)
from src.models.cure.shared.case_report import CaseReport
from src.models.cure.shared.patient import CaseReportPatientEdge, Patient
from src.models.cure.shared.reporter import CaseReportReporterEdge, Reporter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import Node, Relationship


class RasopathiesAdapter(FlatFileAdapter):
    def __init__(self, file_path: str):
        super().__init__(file_path=file_path)
        self._emitted_condition_ids: set[str] = set()
        self._finding_group_lookup, self._finding_default_lookup = self._load_finding_group_lookup()

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        batch: List[Union[Node, Relationship]] = []

        with open(self.file_path, mode="r") as handle:
            for line in handle:
                row = json.loads(line)
                if row.get("form_type") != "rasopathies":
                    continue

                case_report = self._build_case_report(row)
                batch.append(case_report)

                presentation = self._build_presentation(case_report.id)
                batch.extend([
                    presentation,
                    CaseReportPresentationEdge(
                        start_node=case_report,
                        end_node=presentation,
                    ),
                ])

                condition = self._build_condition(row)
                if condition is not None:
                    if condition.id not in self._emitted_condition_ids:
                        batch.append(condition)
                        self._emitted_condition_ids.add(condition.id)
                    batch.append(
                        PresentationConditionEdge(
                            start_node=presentation,
                            end_node=condition,
                        )
                    )

                for finding in self._build_findings(row, case_report.id):
                    batch.extend([
                        finding,
                        PresentationFindingEdge(
                            start_node=presentation,
                            end_node=finding,
                        ),
                    ])

                patient = self._build_patient(row, case_report.id)
                if patient is not None:
                    batch.extend([
                        patient,
                        CaseReportPatientEdge(
                            start_node=case_report,
                            end_node=patient,
                        ),
                    ])

                reporter = self._build_reporter(row, case_report.id)
                if reporter is not None:
                    batch.extend([
                        reporter,
                        CaseReportReporterEdge(
                            start_node=case_report,
                            end_node=reporter,
                        ),
                    ])

                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []

        if batch:
            yield batch

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.CURE

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version="manual-rasopathies",
            download_date=self.download_date,
        )

    def _build_case_report(self, row: dict) -> CaseReport:
        report = row.get("report") or {}
        extra_fields = report.get("extra_fields") or {}
        return CaseReport(
            id=row["id"],
            form_type=row.get("form_type"),
            report_type=row.get("report_type"),
            backend_report_type=self._normalize_optional_string(report.get("report_type")),
            status=row.get("status"),
            anonymous=row.get("anonymous"),
            created=self._parse_utc_datetime(row.get("created")),
            updated=self._parse_utc_datetime(row.get("updated")),
            comment_count=report.get("comment_count"),
            flagged=report.get("flagged"),
            reminder=report.get("reminder"),
            when_reminder=self._extract_value_list(row.get("when_reminder") or report.get("when_reminder")),
            previously_approved=extra_fields.get("previously_approved"),
        )

    @staticmethod
    def _build_presentation(case_report_id: str) -> Presentation:
        return Presentation(id=f"{case_report_id}:presentation")

    def _build_condition(self, row: dict) -> Condition | None:
        disease = (row.get("report") or {}).get("disease") or {}
        disease_name = self._normalize_optional_string(disease.get("name"))
        disease_slug = self._normalize_optional_string(disease.get("url_name"))
        disease_source_id = disease.get("id")
        if disease_name is None and disease_slug is None and disease_source_id is None:
            return None
        condition_id = (
            f"CURE-ID:Condition:{disease_source_id}"
            if disease_source_id is not None
            else f"CURE-ID:Condition:rasopathies:{self._slugify(disease_name or disease_slug or 'unknown')}"
        )
        return Condition(
            id=condition_id,
            name=disease_name,
            slug=disease_slug,
            source_id=disease_source_id,
        )

    def _build_findings(self, row: dict, case_report_id: str) -> List[Finding]:
        findings = []
        raw_findings = (row.get("report") or {}).get("findings") or []
        if not isinstance(raw_findings, list):
            return findings
        for index, item in enumerate(raw_findings):
            if not isinstance(item, dict):
                continue
            raw_value = self._normalize_optional_string(item.get("value"))
            group = self._normalize_optional_string(item.get("group"))
            label = self._normalize_optional_string(item.get("label"))
            text = self._normalize_optional_string(item.get("text"))
            selected = item.get("selected")
            default = item.get("default")
            if all(value is None for value in [raw_value, group, label, text]) and selected is None and default is None:
                continue
            if group is None and raw_value is not None:
                group = self._finding_group_lookup.get(raw_value)
            if default is None and raw_value is not None:
                default = self._finding_default_lookup.get(raw_value)

            split_values = self._split_other_finding_values(raw_value, text, label)
            for split_index, split_value in enumerate(split_values):
                findings.append(
                    Finding(
                        id=f"{case_report_id}:presentation:finding:{index}:{split_index}",
                        name=split_value,
                        group=group,
                        label=label,
                        text=None if split_value == raw_value else split_value,
                        raw_text=text if split_value != raw_value else None,
                        selected=selected if isinstance(selected, bool) else None,
                        default=default if isinstance(default, bool) else None,
                    )
                )
        return findings

    def _build_patient(self, row: dict, case_report_id: str) -> Patient | None:
        patient = (row.get("report") or {}).get("patient") or {}
        if not any(
            patient.get(key) not in (None, "", [], {})
            for key in ["age_group", "sex", "ethnicity", "country_treated", "race"]
        ):
            return None
        return Patient(
            id=f"{case_report_id}:patient",
            sex=self._normalize_optional_string(patient.get("sex")),
            age_group=self._normalize_optional_string(patient.get("age_group")),
            ethnicity=self._normalize_optional_string(patient.get("ethnicity")),
            country_treated=self._normalize_optional_string(patient.get("country_treated")),
            race=self._extract_value_list(patient.get("race")),
        )

    def _build_reporter(self, row: dict, case_report_id: str) -> Reporter | None:
        author = (row.get("report") or {}).get("author") or {}
        top_level_report_type = self._normalize_optional_string(row.get("report_type"))
        if not any(
            author.get(key) not in (None, "", [], {})
            for key in ["id", "qualification", "is_staff", "is_superuser"]
        ) and top_level_report_type is None:
            return None
        author_id = author.get("id")
        suffix = str(author_id) if author_id is not None else "reporter"
        reporter_type = f"{top_level_report_type}_reporter" if top_level_report_type else "reporter"
        return Reporter(
            id=f"{case_report_id}:author:{suffix}",
            reporter_type=reporter_type,
            qualification=self._normalize_optional_string(author.get("qualification")),
            is_staff=author.get("is_staff"),
            is_superuser=author.get("is_superuser"),
        )

    @staticmethod
    def _normalize_optional_string(value):
        if value is None:
            return None
        if isinstance(value, str):
            stripped = value.strip()
            return stripped or None
        return value

    def _extract_value_list(self, payload) -> List[str]:
        values: List[str] = []
        if not isinstance(payload, list):
            return values
        for item in payload:
            value = None
            if isinstance(item, dict):
                value = item.get("value")
            elif isinstance(item, str):
                value = item
            normalized = self._normalize_optional_string(value)
            if normalized is not None:
                values.append(normalized)
        return values

    @staticmethod
    def _parse_utc_datetime(value):
        if not value:
            return None
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).astimezone(timezone.utc).replace(tzinfo=None)

    @staticmethod
    def _slugify(value: str) -> str:
        return value.lower().replace(" ", "-").replace("/", "-").replace(":", "").replace(",", "")

    @staticmethod
    def _split_other_finding_values(raw_value, text, label) -> List[str]:
        if label != "Other":
            return [raw_value] if raw_value is not None else []
        source_text = text or raw_value
        if source_text is None:
            return []
        parts = [part.strip() for part in source_text.split(";")]
        parts = [part for part in parts if part]
        return parts or [source_text]

    @staticmethod
    def _load_finding_group_lookup() -> tuple[Dict[str, str], Dict[str, bool]]:
        form_path = Path("/Users/kelleherkj/IdeaProjects/project-cure-backend/server/apps/ui_forms/json_data/rasopathies.json")
        if not form_path.exists():
            return {}, {}

        try:
            form_spec = json.loads(form_path.read_text())
        except Exception:
            return {}, {}

        group_lookup: Dict[str, str] = {}
        default_lookup: Dict[str, bool] = {}
        for page_name, page_spec in form_spec.items():
            if not page_name.startswith("presentation-"):
                continue
            for control in page_spec.get("formControls") or []:
                if control.get("key") != "findings":
                    continue
                for group in control.get("groups") or []:
                    group_label = group.get("label")
                    is_default = bool(group.get("default"))
                    for option in group.get("options") or []:
                        if not isinstance(option, dict):
                            continue
                        value = option.get("value")
                        if not isinstance(value, str) or not value.strip():
                            continue
                        normalized = value.strip()
                        if group_label and normalized not in group_lookup:
                            group_lookup[normalized] = group_label
                        if normalized not in default_lookup:
                            default_lookup[normalized] = is_default
        return group_lookup, default_lookup
