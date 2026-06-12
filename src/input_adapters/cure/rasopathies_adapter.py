import json
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Generator, List, Union

from src.constants import DataSourceName
from src.input_adapters.cure.case_report_url import build_cure_case_report_url
from src.input_adapters.flat_file_adapter import FlatFileAdapter
from src.models.cure.pasc.condition import Condition
from src.models.cure.rasopathies.drug import Drug
from src.models.cure.rasopathies.drug_treatment import (
    DrugTreatment,
    DrugTreatmentAdverseEventEdge,
    DrugTreatmentDrugEdge,
    DrugTreatmentResponseEdge,
    ClinicalContextDrugTreatmentEdge,
    TreatmentResponse,
    TreatmentResponseFindingEdge,
)
from src.models.cure.rasopathies.finding import (
    Finding,
    FindingPhenotypeEdge,
    PerinatalContextFindingEdge,
    ClinicalContextFindingEdge,
)
from src.models.cure.rasopathies.genetics import (
    Diagnosis,
    DiagnosisConditionEdge,
    DiagnosisGeneEdge,
    DiagnosisGeneVariantEdge,
    GeneGeneVariantEdge,
    GeneVariant,
    ClinicalContextDiagnosisEdge,
)
from src.models.cure.rasopathies.perinatal_context import (
    PerinatalContext,
    PatientPerinatalContextEdge,
)
from src.models.cure.rasopathies.phenotype import Phenotype
from src.models.cure.rasopathies.clinical_context import (
    PatientClinicalContextEdge,
    ClinicalContext,
    ClinicalContextConditionEdge,
)
from src.models.cure.shared.case_report import CaseReport
from src.models.cure.shared.patient import CaseReportPatientEdge, Patient
from src.models.cure.shared.reporter import CaseReportReporterEdge, Reporter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.gene import Gene
from src.models.node import Node, Relationship


class RasopathiesAdapter(FlatFileAdapter):
    _VERSION_DATE_PATTERN = re.compile(r"_(\d{8})T\d{6}Z")

    def __init__(self, file_path: str = None, data_source=None):
        self.data_source = data_source
        if data_source is not None:
            file_path = str(data_source.file())
        if file_path is None:
            raise ValueError("RasopathiesAdapter requires file_path or data_source")
        super().__init__(file_path=file_path)
        self._finding_group_lookup, self._finding_default_lookup = self._load_finding_group_lookup()

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        batch: List[Union[Node, Relationship]] = []

        with open(self.file_path, mode="r") as handle:
            for line in handle:
                row = json.loads(line)
                if row.get("form_type") != "rasopathies":
                    continue
                if row.get("status") != "Approved":
                    continue

                case_report = self._build_case_report(row)
                batch.append(case_report)

                clinical_context = self._build_clinical_context(case_report.id)
                batch.append(clinical_context)

                condition = self._build_condition(row)
                if condition is not None:
                    batch.append(
                        ClinicalContextConditionEdge(
                            start_node=clinical_context,
                            end_node=condition,
                        )
                    )
                    batch.extend(self._build_genetics(row, case_report.id, clinical_context, condition))

                clinical_context_finding_lookup = {}
                for finding, finding_edge, phenotype_edge in self._build_findings(row, case_report.id, clinical_context):
                    batch.extend([
                        finding,
                        finding_edge,
                        phenotype_edge,
                    ])
                    lookup_key = self._finding_lookup_key(finding.source_value)
                    if lookup_key is not None and lookup_key not in clinical_context_finding_lookup:
                        clinical_context_finding_lookup[lookup_key] = finding

                patient = self._build_patient(row, case_report.id)
                if patient is not None:
                    batch.extend([
                        patient,
                        CaseReportPatientEdge(
                            start_node=case_report,
                            end_node=patient,
                        ),
                        PatientClinicalContextEdge(
                            start_node=patient,
                            end_node=clinical_context,
                        ),
                    ])
                    for treatment_entries in self._build_drug_treatments(row, case_report.id, clinical_context, clinical_context_finding_lookup):
                        batch.extend(treatment_entries)

                    perinatal_context = self._build_perinatal_context(row, case_report.id)
                    if perinatal_context is not None:
                        batch.extend([
                            perinatal_context,
                            PatientPerinatalContextEdge(
                                start_node=patient,
                                end_node=perinatal_context,
                            ),
                        ])
                        for fetal_finding, fetal_finding_edge, fetal_phenotype, fetal_phenotype_edge in self._build_fetal_finding_phenotypes(row, perinatal_context):
                            batch.extend([
                                fetal_finding,
                                fetal_finding_edge,
                                fetal_phenotype,
                                fetal_phenotype_edge,
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
        if self.data_source is not None:
            return self.data_source.version_info()
        return DatasourceVersionInfo(
            version=Path(self.file_path).stem,
            version_date=self._parse_version_date_from_file_name(),
            download_date=self.download_date,
        )

    def _parse_version_date_from_file_name(self) -> date | None:
        match = self._VERSION_DATE_PATTERN.search(Path(self.file_path).name)
        if match is None:
            return None
        return datetime.strptime(match.group(1), "%Y%m%d").date()

    def _build_case_report(self, row: dict) -> CaseReport:
        report = row.get("report") or {}
        extra_fields = report.get("extra_fields") or {}
        return CaseReport(
            id=row["id"],
            form_type=row.get("form_type"),
            report_type=row.get("report_type"),
            backend_report_type=self._normalize_optional_string(report.get("report_type")),
            case_report_url=build_cure_case_report_url(row.get("id"), row.get("form_type")),
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
    def _build_clinical_context(case_report_id: str) -> ClinicalContext:
        return ClinicalContext(id=f"{case_report_id}:clinical_context")

    def _build_condition(self, row: dict) -> Condition | None:
        disease = (row.get("report") or {}).get("disease") or {}
        disease_name = self._normalize_optional_string(disease.get("name"))
        if disease_name is None:
            return None
        return Condition(
            id=disease_name,
            name=disease_name,
        )

    def _build_genetics(
        self,
        row: dict,
        case_report_id: str,
        clinical_context: ClinicalContext,
        condition: Condition,
    ) -> List[Union[Node, Relationship]]:
        report = row.get("report") or {}
        gene_symbols = report.get("gene_sequencing") or []
        if not isinstance(gene_symbols, list):
            return []
        nucleotide_change = self._normalize_optional_string(report.get("nucleotide_change"))
        protein_change = self._normalize_optional_string(report.get("protein_change"))
        variant_label = protein_change or nucleotide_change
        diagnosis_methods = self._extract_value_list(report.get("how_diagnosis"))
        normalized_gene_symbols = [
            gene_symbol
            for gene_symbol in (
                self._normalize_optional_string(gene_symbol_raw)
                for gene_symbol_raw in gene_symbols
            )
            if gene_symbol is not None
        ]
        if not diagnosis_methods and not normalized_gene_symbols and variant_label is None:
            return []
        diagnosis = Diagnosis(
            id=f"{case_report_id}:diagnosis",
            diagnosis_methods=diagnosis_methods,
        )
        entries: List[Union[Node, Relationship]] = []
        entries.extend([
            diagnosis,
            ClinicalContextDiagnosisEdge(
                start_node=clinical_context,
                end_node=diagnosis,
            ),
            DiagnosisConditionEdge(
                start_node=diagnosis,
                end_node=condition,
            ),
        ])
        for gene_index, gene_symbol in enumerate(normalized_gene_symbols):
            gene = Gene(
                id=gene_symbol,
                symbol=gene_symbol,
            )
            entries.extend([
                gene,
                DiagnosisGeneEdge(
                    start_node=diagnosis,
                    end_node=gene,
                ),
            ])
            if variant_label is None:
                continue
            variant = GeneVariant(
                id=f"{case_report_id}:gene-variant:{gene_index}",
                source_gene_symbol=gene_symbol,
                nucleotide_change=nucleotide_change,
                protein_change=protein_change,
                variant_label=variant_label,
            )
            entries.extend([
                variant,
                DiagnosisGeneVariantEdge(
                    start_node=diagnosis,
                    end_node=variant,
                ),
                GeneGeneVariantEdge(
                    start_node=gene,
                    end_node=variant,
                ),
            ])
        return entries

    def _build_findings(self, row: dict, case_report_id: str, clinical_context: ClinicalContext) -> List[tuple[Finding, ClinicalContextFindingEdge, FindingPhenotypeEdge]]:
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
                finding = Finding(
                    id=f"{case_report_id}:clinical_context:finding:{index}:{split_index}",
                    finding_context="clinical_context",
                    source_value=split_value,
                    source_text=text or raw_value,
                    raw_text=text if split_value != raw_value else None,
                    group=group,
                    label=label,
                    selected=selected if isinstance(selected, bool) else None,
                    default=default if isinstance(default, bool) else None,
                )
                phenotype = Phenotype(
                    id=split_value,
                    name=split_value,
                )
                finding_edge = ClinicalContextFindingEdge(
                    start_node=clinical_context,
                    end_node=finding,
                )
                phenotype_edge = FindingPhenotypeEdge(
                    start_node=finding,
                    end_node=phenotype,
                )
                findings.append((finding, finding_edge, phenotype_edge))
        return findings

    def _build_drug_treatments(
        self,
        row: dict,
        case_report_id: str,
        clinical_context: ClinicalContext,
        clinical_context_finding_lookup: Dict[str, Finding],
    ) -> List[List[Union[Node, Relationship]]]:
        treatment_batches = []
        treatments = (row.get("report") or {}).get("treatments") or []
        if not isinstance(treatments, list):
            return treatment_batches

        for treatment_index, treatment_payload in enumerate(treatments):
            if not isinstance(treatment_payload, dict):
                continue
            treatment = self._build_drug_treatment(case_report_id, treatment_index, treatment_payload)
            treatment_entries: List[Union[Node, Relationship]] = [
                treatment,
                ClinicalContextDrugTreatmentEdge(
                    start_node=clinical_context,
                    end_node=treatment,
                ),
            ]

            drug = self._build_drug(treatment_payload)
            if drug is not None:
                treatment_entries.extend([
                    drug,
                    DrugTreatmentDrugEdge(
                        start_node=treatment,
                        end_node=drug,
                    ),
                ])

            treatment_entries.extend(
                self._build_treatment_responses(
                    case_report_id=case_report_id,
                    treatment_index=treatment_index,
                    treatment=treatment,
                    treatment_payload=treatment_payload,
                    clinical_context_finding_lookup=clinical_context_finding_lookup,
                )
            )
            treatment_entries.extend(
                self._build_treatment_adverse_events(
                    treatment=treatment,
                    treatment_payload=treatment_payload,
                )
            )
            treatment_batches.append(treatment_entries)
        return treatment_batches

    def _build_drug_treatment(self, case_report_id: str, treatment_index: int, treatment_payload: dict) -> DrugTreatment:
        initial_regimen = treatment_payload.get("treatment_initial_regimen") or {}
        current_regimen = treatment_payload.get("treatment_regimen") or {}
        duration = treatment_payload.get("treatment_duration") or {}
        additional_details = treatment_payload.get("treatment_additional_details") or {}
        additional_info = treatment_payload.get("treatment_additional_info") or {}
        return DrugTreatment(
            id=f"{case_report_id}:drug-treatment:{treatment_index}",
            source_treatment_index=treatment_index,
            initial_dose_amount=self._normalize_optional_string(initial_regimen.get("dose_amount")),
            initial_unit_of_measurement=self._normalize_optional_string(initial_regimen.get("unit_of_measurement")),
            initial_frequency=self._normalize_optional_string(initial_regimen.get("frequency")),
            initial_route=self._normalize_optional_string(initial_regimen.get("route")),
            current_dose_amount=self._normalize_optional_string(current_regimen.get("dose_amount")),
            current_unit_of_measurement=self._normalize_optional_string(current_regimen.get("unit_of_measurement")),
            current_frequency=self._normalize_optional_string(current_regimen.get("frequency")),
            current_route=self._normalize_optional_string(current_regimen.get("route")),
            current_dose_change=self._normalize_optional_string(current_regimen.get("dose_change")),
            duration_amount=self._normalize_optional_string(duration.get("duration_amount")),
            unit_of_measurement_duration=self._normalize_optional_string(duration.get("unit_of_measurement_duration")),
            treatment_begin=self._normalize_optional_string(duration.get("treatment_begin")),
            treatment_begin_month=self._normalize_optional_string(duration.get("treatment_begin_month")),
            treatment_end=self._normalize_optional_string(duration.get("treatment_end")),
            treatment_end_month=self._normalize_optional_string(duration.get("treatment_end_month")),
            treatment_on_going=self._normalize_optional_string(duration.get("treatment_on_going")),
            severity=self._extract_value_list(additional_details.get("severity")),
            additional_drug_info=self._normalize_optional_string(additional_info.get("additional_drug_info_non_ID")),
        )

    def _build_drug(self, treatment_payload: dict) -> Drug | None:
        drug_payload = treatment_payload.get("treatment_drug") or {}
        drug_name = self._normalize_optional_string(drug_payload.get("name"))
        if drug_name is None:
            return None
        source_id = drug_payload.get("id")
        return Drug(
            id=drug_name,
            name=drug_name,
            url=self._normalize_optional_string(drug_payload.get("url")),
            source_id=str(source_id) if source_id is not None else None,
        )

    def _build_treatment_responses(
        self,
        case_report_id: str,
        treatment_index: int,
        treatment: DrugTreatment,
        treatment_payload: dict,
        clinical_context_finding_lookup: Dict[str, Finding],
    ) -> List[Union[Node, Relationship]]:
        entries: List[Union[Node, Relationship]] = []
        treatment_time = treatment_payload.get("treatment_time") or {}
        time_to_improvement = self._normalize_optional_string(treatment_time.get("time_to_improvement"))
        primary_target = treatment_payload.get("treatment_primary_target") or {}
        primary_target_label = self._normalize_optional_string(primary_target.get("primary_drug_target"))
        if primary_target_label is not None:
            entries.extend(
                self._build_treatment_response_entries(
                    case_report_id=case_report_id,
                    treatment_index=treatment_index,
                    target_index=0,
                    target_role="primary",
                    target_label=primary_target_label,
                    outcome=self._normalize_optional_string(primary_target.get("outcome_primary_target")),
                    outcome_details=self._normalize_optional_string(primary_target.get("outcome_primary_target_details")),
                    time_to_improvement=time_to_improvement,
                    treatment=treatment,
                    clinical_context_finding_lookup=clinical_context_finding_lookup,
                )
            )

        secondary_target = treatment_payload.get("secondary_primary_target") or {}
        secondary_targets = secondary_target.get("secondary_drug_target") or []
        if not isinstance(secondary_targets, list):
            return entries
        for secondary_index, target_payload in enumerate(secondary_targets, start=1):
            if not isinstance(target_payload, dict):
                continue
            target_label = self._normalize_optional_string(target_payload.get("target"))
            if target_label is None:
                continue
            entries.extend(
                self._build_treatment_response_entries(
                    case_report_id=case_report_id,
                    treatment_index=treatment_index,
                    target_index=secondary_index,
                    target_role="secondary",
                    target_label=target_label,
                    outcome=self._normalize_optional_string(target_payload.get("outcome")),
                    outcome_details=self._normalize_optional_string(target_payload.get("outcome_details")),
                    time_to_improvement=time_to_improvement,
                    treatment=treatment,
                    clinical_context_finding_lookup=clinical_context_finding_lookup,
                )
            )
        return entries

    def _build_treatment_response_entries(
        self,
        case_report_id: str,
        treatment_index: int,
        target_index: int,
        target_role: str,
        target_label: str,
        outcome: str | None,
        outcome_details: str | None,
        time_to_improvement: str | None,
        treatment: DrugTreatment,
        clinical_context_finding_lookup: Dict[str, Finding],
    ) -> List[Union[Node, Relationship]]:
        response = TreatmentResponse(
            id=f"{case_report_id}:drug-treatment:{treatment_index}:response:{target_index}",
            source_treatment_index=treatment_index,
            source_target_index=target_index,
            target_role=target_role,
            outcome=outcome,
            outcome_details=outcome_details,
            time_to_improvement=time_to_improvement,
        )
        entries: List[Union[Node, Relationship]] = [
            response,
            DrugTreatmentResponseEdge(
                start_node=treatment,
                end_node=response,
            ),
        ]
        finding = clinical_context_finding_lookup.get(self._finding_lookup_key(target_label))
        if finding is None:
            finding = Finding(
                id=f"{case_report_id}:drug-treatment:{treatment_index}:target-finding:{target_index}",
                finding_context="treatment_target",
                source_value=target_label,
                source_text=target_label,
            )
            phenotype = Phenotype(
                id=target_label,
                name=target_label,
            )
            entries.extend([
                finding,
                FindingPhenotypeEdge(
                    start_node=finding,
                    end_node=phenotype,
                ),
            ])
        entries.append(
            TreatmentResponseFindingEdge(
                start_node=response,
                end_node=finding,
            )
        )
        return entries

    def _build_treatment_adverse_events(
        self,
        treatment: DrugTreatment,
        treatment_payload: dict,
    ) -> List[Union[Node, Relationship]]:
        adverse_event_payload = treatment_payload.get("treatment_adverse_events") or {}
        have_adverse_events = self._normalize_optional_string(adverse_event_payload.get("have_adverse_events"))
        adverse_event_labels = self._extract_value_list(adverse_event_payload.get("adverse_events_generic"))
        outcomes = self._extract_value_list(adverse_event_payload.get("adverse_events_outcome"))
        entries: List[Union[Node, Relationship]] = []
        for adverse_event_index, adverse_event_label in enumerate(adverse_event_labels):
            phenotype = Phenotype(
                id=adverse_event_label,
                name=adverse_event_label,
            )
            entries.extend([
                phenotype,
                DrugTreatmentAdverseEventEdge(
                    start_node=treatment,
                    end_node=phenotype,
                    source_adverse_event_index=adverse_event_index,
                    source_label=adverse_event_label,
                    have_adverse_events=have_adverse_events,
                    outcomes=list(outcomes),
                ),
            ])
        return entries

    def _build_patient(self, row: dict, case_report_id: str) -> Patient | None:
        patient = (row.get("report") or {}).get("patient") or {}
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

    def _build_perinatal_context(self, row: dict, case_report_id: str) -> PerinatalContext | None:
        report = row.get("report") or {}
        premature_birth = self._normalize_optional_string(report.get("premature_birth"))
        fetal_findings = self._extract_value_list(report.get("fetal_findings"))
        fetal_findings_details = self._extract_value_list(report.get("fetal_findings_details"))
        if premature_birth is None and not fetal_findings and not fetal_findings_details:
            return None
        return PerinatalContext(
            id=f"{case_report_id}:perinatal-context",
            premature_birth=premature_birth,
            fetal_findings=fetal_findings,
            fetal_findings_details=fetal_findings_details,
        )

    def _build_fetal_finding_phenotypes(self, row: dict, perinatal_context: PerinatalContext) -> List[tuple[Finding, PerinatalContextFindingEdge, Phenotype, FindingPhenotypeEdge]]:
        report = row.get("report") or {}
        findings = []
        for index, value in enumerate(self._extract_value_list(report.get("fetal_findings_details"))):
            finding = Finding(
                id=f"{perinatal_context.id}:finding:{index}",
                finding_context="perinatal",
                source_value=value,
                source_text=value,
            )
            phenotype = Phenotype(
                id=value,
                name=value,
            )
            findings.append((
                finding,
                PerinatalContextFindingEdge(
                    start_node=perinatal_context,
                    end_node=finding,
                ),
                phenotype,
                FindingPhenotypeEdge(
                    start_node=finding,
                    end_node=phenotype,
                )
            ))
        return findings

    @staticmethod
    def _normalize_optional_string(value):
        if value is None:
            return None
        if isinstance(value, str):
            stripped = value.strip()
            return stripped or None
        return value

    @classmethod
    def _finding_lookup_key(cls, value):
        normalized = cls._normalize_optional_string(value)
        if normalized is None:
            return None
        if not isinstance(normalized, str):
            normalized = str(normalized)
        return " ".join(normalized.split()).casefold()

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
            if not page_name.startswith("clinical_context-"):
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
