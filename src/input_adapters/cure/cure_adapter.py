import json
import hashlib
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Generator, List, Optional, Union

from src.constants import DataSourceName
from src.input_adapters.cure.case_report_url import build_cure_case_report_url
from src.input_adapters.flat_file_adapter import FlatFileAdapter
from src.models.cure.pasc.adverse_event import ExposureAdverseEventEdge
from src.models.cure.pasc.background_context import (
    BackgroundContext,
    BackgroundContextConditionEdge,
    BackgroundContextExposureEdge,
    PersonBackgroundContextEdge,
)
from src.models.cure.shared.case_report import CaseReport
from src.models.cure.pasc.condition import Condition
from src.models.cure.pasc.drug import Drug
from src.models.cure.pasc.episode import Episode, EpisodeConditionEdge, EpisodeEpisodeEdge, PersonEpisodeEdge
from src.models.cure.pasc.exposure import EpisodeExposureEdge, Exposure, ExposureDrugEdge
from src.models.cure.pasc.outcome import EpisodeOutcomeEdge, Outcome, OutcomePhenotypeEdge, TreatmentOutcomeEdge
from src.models.cure.pasc.phenotype import EpisodePhenotypeEdge, Phenotype
from src.models.cure.shared.patient import CaseReportPatientEdge, Patient
from src.models.cure.shared.reporter import CaseReportReporterEdge, Reporter
from src.models.cure.pasc.therapy import EpisodeTherapyEdge, Therapy
from src.models.cure.pasc.treatment import Treatment, TreatmentExposureEdge
from src.models.cure.pasc.vaccination import Vaccine, VaccinationEvent, VaccinationEventEpisodeEdge, VaccinationEventVaccineEdge
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import Node, Relationship


class CUREAdapter(FlatFileAdapter):
    _VERSION_DATE_PATTERN = re.compile(r"_(\d{8})T\d{6}Z")

    def __init__(self, file_path: str, form_type: str = "pasc"):
        super().__init__(file_path=file_path)
        self.form_type = form_type
        self._emitted_condition_ids = set()
        self._emitted_drug_ids = set()
        self._emitted_phenotype_ids = set()
        self._emitted_therapy_ids = set()
        self._emitted_vaccine_ids = set()

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        batch: List[Union[Node, Relationship]] = []

        with open(self.file_path, mode="r") as handle:
            for line in handle:
                row = json.loads(line)
                if row.get("form_type") != self.form_type:
                    continue
                if row.get("status") != "Approved":
                    continue

                case_report = self._build_case_report(row)
                patient = self._build_person(row)
                reporter = self._build_reporter(row, case_report.id)
                background_context = self._build_background_context(row, case_report.id)
                primary_episode = self._build_primary_episode(row, case_report.id)
                acute_episode = self._build_acute_episode(row, case_report.id)
                pregnancy_episode = self._build_pregnancy_episode(row, case_report.id)
                acute_complication_conditions = self._build_acute_complication_conditions(row)
                prior_conditions = self._build_prior_conditions(row)
                background_regular_medicine_exposures = self._build_background_regular_medicine_exposures(row, case_report.id)
                background_immunosuppressant_exposures = self._build_background_immunosuppressant_exposures(row, case_report.id)
                post_covid_conditions = self._build_post_covid_conditions(row)
                vaccination_history = self._build_vaccination_history(row, case_report.id)
                condition = self._build_condition(row)
                acute_condition = self._build_acute_condition()
                pregnancy_condition = self._build_pregnancy_condition()
                exposures = self._build_episode_exposures(row)
                acute_exposures = self._build_acute_episode_exposures(row, case_report.id) if acute_episode is not None else []
                pregnancy_exposures = self._build_pregnancy_episode_exposures(row, case_report.id) if pregnancy_episode is not None else []
                phenotypes = self._build_episode_phenotypes(row)
                therapies = self._build_episode_therapies(row)
                phenotype_map = self._build_phenotype_map(phenotypes)
                treatments = self._build_episode_treatments(row, case_report.id, exposures)
                treatment_map = {combo_key: treatment for combo_key, treatment, _ in treatments}
                outcomes = self._build_outcomes(row, case_report.id, treatment_map, phenotype_map)
                batch.extend([
                    case_report,
                    patient,
                    primary_episode,
                    CaseReportPatientEdge(start_node=case_report, end_node=patient),
                    PersonEpisodeEdge(start_node=patient, end_node=primary_episode),
                    EpisodeConditionEdge(start_node=primary_episode, end_node=condition, relationship_type="primary"),
                ])
                if reporter is not None:
                    batch.extend([
                        reporter,
                        CaseReportReporterEdge(start_node=case_report, end_node=reporter),
                    ])
                if background_context is not None:
                    batch.extend([
                        background_context,
                        PersonBackgroundContextEdge(start_node=patient, end_node=background_context),
                    ])
                if acute_episode is not None:
                    batch.extend([
                        acute_episode,
                        PersonEpisodeEdge(start_node=patient, end_node=acute_episode),
                        EpisodeConditionEdge(start_node=acute_episode, end_node=acute_condition, relationship_type="primary"),
                        EpisodeEpisodeEdge(start_node=acute_episode, end_node=primary_episode, relationship_type="precedes"),
                    ])
                if pregnancy_episode is not None:
                    batch.extend([
                        pregnancy_episode,
                        PersonEpisodeEdge(start_node=patient, end_node=pregnancy_episode),
                        EpisodeConditionEdge(start_node=pregnancy_episode, end_node=pregnancy_condition, relationship_type="primary"),
                        EpisodeEpisodeEdge(start_node=pregnancy_episode, end_node=primary_episode, relationship_type="overlaps"),
                    ])
                for prior_condition, prior_condition_edge in prior_conditions:
                    if prior_condition.id not in self._emitted_condition_ids:
                        batch.append(prior_condition)
                        self._emitted_condition_ids.add(prior_condition.id)
                    prior_condition_edge.start_node = background_context
                    prior_condition_edge.end_node = prior_condition
                    batch.append(prior_condition_edge)
                for exposure, drug, context_exposure_edge, exposure_drug_edge in background_regular_medicine_exposures:
                    batch.append(exposure)
                    if drug.id not in self._emitted_drug_ids:
                        batch.append(drug)
                        self._emitted_drug_ids.add(drug.id)
                    context_exposure_edge.start_node = background_context
                    context_exposure_edge.end_node = exposure
                    exposure_drug_edge.start_node = exposure
                    exposure_drug_edge.end_node = drug
                    batch.extend([context_exposure_edge, exposure_drug_edge])
                for exposure, drug, context_exposure_edge, exposure_drug_edge in background_immunosuppressant_exposures:
                    batch.append(exposure)
                    if drug.id not in self._emitted_drug_ids:
                        batch.append(drug)
                        self._emitted_drug_ids.add(drug.id)
                    context_exposure_edge.start_node = background_context
                    context_exposure_edge.end_node = exposure
                    exposure_drug_edge.start_node = exposure
                    exposure_drug_edge.end_node = drug
                    batch.extend([context_exposure_edge, exposure_drug_edge])
                for post_covid_condition, post_covid_condition_edge in post_covid_conditions:
                    if post_covid_condition.id not in self._emitted_condition_ids:
                        batch.append(post_covid_condition)
                        self._emitted_condition_ids.add(post_covid_condition.id)
                    post_covid_condition_edge.start_node = primary_episode
                    post_covid_condition_edge.end_node = post_covid_condition
                    batch.append(post_covid_condition_edge)
                if condition.id not in self._emitted_condition_ids:
                    batch.append(condition)
                    self._emitted_condition_ids.add(condition.id)
                if acute_episode is not None and acute_condition.id not in self._emitted_condition_ids:
                    batch.append(acute_condition)
                    self._emitted_condition_ids.add(acute_condition.id)
                if pregnancy_episode is not None and pregnancy_condition.id not in self._emitted_condition_ids:
                    batch.append(pregnancy_condition)
                    self._emitted_condition_ids.add(pregnancy_condition.id)
                if acute_episode is not None:
                    for complication_condition, complication_edge in acute_complication_conditions:
                        if complication_condition.id not in self._emitted_condition_ids:
                            batch.append(complication_condition)
                            self._emitted_condition_ids.add(complication_condition.id)
                        complication_edge.start_node = acute_episode
                        complication_edge.end_node = complication_condition
                        batch.append(complication_edge)
                if acute_episode is not None and vaccination_history is not None:
                    vaccination_event, event_episode_edge, vaccines, event_vaccine_edges = vaccination_history
                    batch.append(vaccination_event)
                    event_episode_edge.start_node = acute_episode
                    batch.append(event_episode_edge)
                    for vaccine, vaccine_edge in zip(vaccines, event_vaccine_edges):
                        if vaccine.id not in self._emitted_vaccine_ids:
                            batch.append(vaccine)
                            self._emitted_vaccine_ids.add(vaccine.id)
                        vaccine_edge.start_node = vaccination_event
                        vaccine_edge.end_node = vaccine
                        batch.append(vaccine_edge)
                for exposure, drug, adverse_events, episode_exposure_edge, exposure_drug_edge in exposures:
                    batch.append(exposure)
                    if drug.id not in self._emitted_drug_ids:
                        batch.append(drug)
                        self._emitted_drug_ids.add(drug.id)
                    episode_exposure_edge.start_node = primary_episode
                    episode_exposure_edge.end_node = exposure
                    exposure_drug_edge.start_node = exposure
                    exposure_drug_edge.end_node = drug
                    batch.extend([episode_exposure_edge, exposure_drug_edge])
                    for adverse_event, adverse_event_edge in adverse_events:
                        if adverse_event.id not in self._emitted_phenotype_ids:
                            batch.append(adverse_event)
                            self._emitted_phenotype_ids.add(adverse_event.id)
                        adverse_event_edge.start_node = exposure
                        adverse_event_edge.end_node = adverse_event
                        batch.append(adverse_event_edge)
                for exposure, drug, adverse_events, episode_exposure_edge, exposure_drug_edge in acute_exposures:
                    batch.append(exposure)
                    if drug.id not in self._emitted_drug_ids:
                        batch.append(drug)
                        self._emitted_drug_ids.add(drug.id)
                    episode_exposure_edge.start_node = acute_episode
                    episode_exposure_edge.end_node = exposure
                    exposure_drug_edge.start_node = exposure
                    exposure_drug_edge.end_node = drug
                    batch.extend([episode_exposure_edge, exposure_drug_edge])
                for exposure, drug, adverse_events, episode_exposure_edge, exposure_drug_edge in pregnancy_exposures:
                    batch.append(exposure)
                    if drug.id not in self._emitted_drug_ids:
                        batch.append(drug)
                        self._emitted_drug_ids.add(drug.id)
                    episode_exposure_edge.start_node = pregnancy_episode
                    episode_exposure_edge.end_node = exposure
                    exposure_drug_edge.start_node = exposure
                    exposure_drug_edge.end_node = drug
                    batch.extend([episode_exposure_edge, exposure_drug_edge])
                for _, treatment, treatment_exposure_edges in treatments:
                    batch.append(treatment)
                    for edge in treatment_exposure_edges:
                        edge.end_node = treatment
                        batch.append(edge)
                for phenotype, phenotype_edge in phenotypes:
                    if phenotype.id not in self._emitted_phenotype_ids:
                        batch.append(phenotype)
                        self._emitted_phenotype_ids.add(phenotype.id)
                    phenotype_edge.start_node = primary_episode
                    batch.append(phenotype_edge)
                for therapy, therapy_edge in therapies:
                    if therapy.id not in self._emitted_therapy_ids:
                        batch.append(therapy)
                        self._emitted_therapy_ids.add(therapy.id)
                    therapy_edge.start_node = primary_episode
                    batch.append(therapy_edge)
                for outcome, episode_outcome_edge, treatment_outcome_edge, outcome_phenotype_edge in outcomes:
                    batch.append(outcome)
                    episode_outcome_edge.start_node = primary_episode
                    episode_outcome_edge.end_node = outcome
                    batch.append(episode_outcome_edge)
                    if treatment_outcome_edge is not None:
                        batch.append(treatment_outcome_edge)
                    if outcome_phenotype_edge is not None:
                        batch.append(outcome_phenotype_edge)
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []

        if batch:
            yield batch

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.CURE

    def get_version(self) -> DatasourceVersionInfo:
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
            case_report_url=build_cure_case_report_url(row.get("id"), row.get("form_type")),
            status=row.get("status"),
            anonymous=row.get("anonymous"),
            created=self._parse_utc_datetime(row.get("created")),
            updated=self._parse_utc_datetime(row.get("updated")),
            percentage_completed=report.get("percentage_completed"),
            comment_count=report.get("comment_count"),
            outcome_computed=report.get("outcome_computed"),
            have_adverse_events_old=report.get("have_adverse_events_old"),
            research_prioritizing=self._normalize_optional_string(extra_fields.get("research_prioritizing")),
        )

    def _build_person(self, row: dict) -> Patient:
        patient = (row.get("report") or {}).get("patient") or {}
        return Patient(
            id=str(patient["id"]),
            sex=patient.get("sex"),
            gender=self._normalize_optional_string(patient.get("gender")),
            gender_same_as_sex=self._normalize_optional_string(patient.get("gender_same_as_sex")),
            age_group=patient.get("age_group"),
            ethnicity=self._normalize_optional_string(patient.get("ethnicity")),
            pregnant=self._normalize_optional_string(patient.get("pregnant")),
            country_treated=self._normalize_optional_string(patient.get("country_treated")),
            race=self._extract_race_list(patient),
        )

    def _build_reporter(self, row: dict, case_report_id: str) -> Optional[Reporter]:
        report = row.get("report") or {}
        author = report.get("author") or {}
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

    def _build_primary_episode(self, row: dict, case_report_id: str) -> Episode:
        report = row.get("report") or {}
        extra_fields = report.get("extra_fields") or {}
        return Episode(
            id=self._build_primary_episode_id(case_report_id),
            role="primary",
            problem_duration=self._normalize_optional_string(extra_fields.get("symptoms_duration")),
            additional_info=self._normalize_optional_string(report.get("additional_info")),
            drug_additional_details=self._normalize_optional_string(extra_fields.get("drug_additional_details")),
        )

    def _build_background_context(self, row: dict, case_report_id: str) -> Optional[BackgroundContext]:
        report = row.get("report") or {}
        patient = report.get("patient") or {}
        extra_fields = report.get("extra_fields") or {}
        has_prior_conditions = bool(patient.get("comorbidity") or [])
        has_regular_medicines = bool(extra_fields.get("regular_medicines") or [])
        has_immunosuppressant_details = bool(report.get("which_immunosuppressant_drugs") or [])
        immunosuppressant_drugs = self._normalize_optional_string(report.get("immunosuppressant_drugs"))
        has_immunosuppressant_context = immunosuppressant_drugs not in (None, "No")
        if not any([has_prior_conditions, has_regular_medicines, has_immunosuppressant_details, has_immunosuppressant_context]):
            return None
        return BackgroundContext(
            id=f"{case_report_id}:background-context",
            immunosuppressant_drugs=immunosuppressant_drugs if has_immunosuppressant_context else None,
        )

    def _build_acute_episode(self, row: dict, case_report_id: str) -> Optional[Episode]:
        report = row.get("report") or {}
        extra_fields = report.get("extra_fields") or {}
        diagnosis_methods = self._extract_value_list(report.get("how_diagnosis"))
        complications = self._extract_value_list(extra_fields.get("complications_acute_covid"))
        onset_month = self._normalize_optional_string(extra_fields.get("when_symptom_acute_covid_month"))
        onset_year = self._normalize_optional_string(extra_fields.get("when_symptom_acute_covid_year"))
        care_level = self._normalize_optional_string(extra_fields.get("care_acute_covid"))
        if not any([diagnosis_methods, complications, onset_month, onset_year, care_level]):
            return None
        return Episode(
            id=self._build_acute_episode_id(case_report_id),
            role="contextual",
            onset_month=onset_month,
            onset_year=onset_year,
            care_level=care_level,
            diagnosis_methods=diagnosis_methods,
        )

    def _build_pregnancy_episode(self, row: dict, case_report_id: str) -> Optional[Episode]:
        report = row.get("report") or {}
        extra_fields = report.get("extra_fields") or {}
        if self._normalize_optional_string(extra_fields.get("pregnant_during_lc")) != "Yes":
            return None
        pregnancy = (report.get("patient") or {}).get("pregnancy") or {}
        return Episode(
            id=self._build_pregnancy_episode_id(case_report_id),
            role="contextual",
            pregnancy_medications=self._normalize_optional_string(extra_fields.get("pregnancy_medications")),
            pregnancy_medication_names=self._extract_name_list(extra_fields.get("pregnancy_medications_details")),
            treatment_gestational_age=self._normalize_optional_string(pregnancy.get("treatment_gestational_age")),
            pregnancy_outcome=self._normalize_optional_string(pregnancy.get("outcome")),
            pregnancy_limited_access_to_treatment=self._normalize_optional_string(extra_fields.get("pregnancy_limited_access_pasc")),
            pregnancy_impacted_ability_to_care_for_newborn=self._normalize_optional_string(extra_fields.get("pregnancy_ability_to_care_for_newborn_pasc")),
        )

    def _build_condition(self, row: dict) -> Condition:
        condition = (row.get("report") or {}).get("disease") or {}
        source_id = condition.get("id")
        if source_id is None:
            raise ValueError("PASC primary disease is missing report.disease.id")
        return Condition(
            id=f"CURE-ID:Condition:{source_id}",
            name=self._normalize_optional_string(condition.get("name")),
            slug=self._normalize_optional_string(condition.get("url_name")),
            source_id=source_id,
        )

    @staticmethod
    def _build_acute_condition() -> Condition:
        return Condition(
            id="CURE-ID:Condition:acute-covid-19",
            name="Acute COVID-19",
            slug="acute-covid-19",
        )

    @staticmethod
    def _build_pregnancy_condition() -> Condition:
        return Condition(
            id="CURE-ID:Condition:pregnancy",
            name="Pregnancy",
            slug="pregnancy",
        )

    def _build_acute_complication_conditions(self, row: dict) -> List[tuple[Condition, EpisodeConditionEdge]]:
        complications = self._extract_value_list((((row.get("report") or {}).get("extra_fields") or {}).get("complications_acute_covid")))
        result = []
        for complication_name in complications:
            slug = self._slugify(complication_name)
            digest = hashlib.md5(complication_name.encode("utf-8")).hexdigest()
            condition = Condition(
                id=f"CURE-ID:Condition:acute-complication:{digest}",
                name=complication_name,
                slug=slug,
            )
            result.append((
                condition,
                EpisodeConditionEdge(start_node=None, end_node=None, relationship_type="complication"),
            ))
        return result

    def _build_prior_conditions(self, row: dict) -> List[tuple[Condition, BackgroundContextConditionEdge]]:
        patient = ((row.get("report") or {}).get("patient") or {})
        raw_conditions = patient.get("comorbidity") or []
        result = []
        for item in raw_conditions:
            if not isinstance(item, dict):
                continue
            condition_name = self._normalize_optional_string(item.get("value") or item.get("label") or item.get("name"))
            if not condition_name:
                continue
            source_id = item.get("id")
            source_id = source_id if isinstance(source_id, int) else None
            condition = self._build_contextual_condition(condition_name, source_id=source_id)
            result.append((
                condition,
                BackgroundContextConditionEdge(start_node=None, end_node=None, relationship_type="prior_comorbidity"),
            ))
        return result

    def _build_post_covid_conditions(self, row: dict) -> List[tuple[Condition, EpisodeConditionEdge]]:
        raw_conditions = (((row.get("report") or {}).get("extra_fields") or {}).get("comorbidities_after_pasc")) or []
        result = []
        for item in raw_conditions:
            if isinstance(item, dict):
                condition_name = self._normalize_optional_string(item.get("value") or item.get("label") or item.get("name"))
            else:
                condition_name = self._normalize_optional_string(item)
            if not condition_name:
                continue
            condition = self._build_contextual_condition(condition_name)
            result.append((condition, EpisodeConditionEdge(start_node=None, end_node=None, relationship_type="comorbidity")))
        return result

    def _build_vaccination_history(
        self,
        row: dict,
        case_report_id: str,
    ) -> Optional[tuple[VaccinationEvent, VaccinationEventEpisodeEdge, List[Vaccine], List[VaccinationEventVaccineEdge]]]:
        extra_fields = (((row.get("report") or {}).get("extra_fields")) or {})
        vaccinated_before_infection = self._normalize_optional_string(extra_fields.get("vaccine_status_generic"))
        dose_count_before_infection = self._normalize_optional_string(extra_fields.get("doses_of_vaccine"))
        relative_time_value = self._normalize_optional_string(extra_fields.get("time_from_vaccination_generic_units"))
        relative_time_unit = self._normalize_optional_string(extra_fields.get("time_from_vaccination_generic_measurements"))
        vaccine_names = self._extract_value_list(extra_fields.get("vaccine_received"))

        if not any([vaccinated_before_infection, dose_count_before_infection, relative_time_value, relative_time_unit, vaccine_names]):
            return None

        vaccination_event = VaccinationEvent(
            id=f"{case_report_id}:vaccination-history:acute-covid",
            vaccinated_before_infection=vaccinated_before_infection,
            dose_count_before_infection=dose_count_before_infection,
        )
        event_episode_edge = VaccinationEventEpisodeEdge(
            start_node=None,
            end_node=vaccination_event,
            relative_time_value=relative_time_value,
            relative_time_unit=relative_time_unit,
        )
        vaccines = []
        event_vaccine_edges = []
        for vaccine_name in vaccine_names:
            vaccine = Vaccine(
                id=self._build_vaccine_id(vaccine_name),
                name=vaccine_name,
                slug=self._slugify(vaccine_name),
            )
            vaccines.append(vaccine)
            event_vaccine_edges.append(VaccinationEventVaccineEdge(start_node=None, end_node=None))

        return vaccination_event, event_episode_edge, vaccines, event_vaccine_edges

    def _build_episode_exposures(self, row: dict) -> List[tuple[Exposure, Drug, List[tuple[Phenotype, ExposureAdverseEventEdge]], EpisodeExposureEdge, ExposureDrugEdge]]:
        regimens = ((row.get("report") or {}).get("regimens") or [])
        exposure_rows = []
        for regimen in regimens:
            source_regimen_id = regimen.get("id")
            if source_regimen_id is None:
                continue
            drug_payload = regimen.get("drug") or {}
            drug = self._build_drug(drug_payload)
            exposure = Exposure(
                id=f"CURE-ID:Exposure:{source_regimen_id}",
                source_regimen_id=source_regimen_id,
                created=self._parse_utc_datetime(regimen.get("created")),
                updated=self._parse_utc_datetime(regimen.get("updated")),
                is_initial_regimen=regimen.get("is_initial_regimen"),
                long_drug_name=self._normalize_optional_string(regimen.get("long_drug_str_lc")),
                dose_amount=self._normalize_optional_string(regimen.get("dose_amount")),
                unit_of_measurement=self._normalize_optional_string(regimen.get("unit_of_measurement")),
                frequency=self._normalize_optional_string(regimen.get("frequency")),
                route=self._normalize_optional_string(regimen.get("route")),
                treatment_begin=self._normalize_optional_string(regimen.get("treatment_begin")),
                treatment_begin_month=self._normalize_optional_string(regimen.get("treatment_begin_month")),
                treatment_end=self._normalize_optional_string(regimen.get("treatment_end")),
                treatment_end_month=self._normalize_optional_string(regimen.get("treatment_end_month")),
                treatment_on_going=self._normalize_optional_string(regimen.get("treatment_on_going")),
                duration_amount=self._normalize_optional_string(regimen.get("duration_amount")),
                unit_of_measurement_duration=self._normalize_optional_string(regimen.get("unit_of_measurement_duration")),
                have_adverse_events=self._normalize_optional_string(regimen.get("have_adverse_events")),
                adverse_events=self._extract_value_list(regimen.get("adverse_events_generic")),
                adverse_event_outcomes=self._extract_value_list(regimen.get("adverse_events_outcome")),
            )
            adverse_events = self._build_exposure_adverse_events(exposure)
            exposure_rows.append((
                exposure,
                drug,
                adverse_events,
                EpisodeExposureEdge(start_node=None, end_node=None),
                ExposureDrugEdge(start_node=None, end_node=None),
            ))
        return exposure_rows

    def _build_acute_episode_exposures(self, row: dict, case_report_id: str) -> List[tuple[Exposure, Drug, List[tuple[Phenotype, ExposureAdverseEventEdge]], EpisodeExposureEdge, ExposureDrugEdge]]:
        acute_drugs = (((row.get("report") or {}).get("extra_fields") or {}).get("drugs_acute_covid")) or []
        if isinstance(acute_drugs, str):
            acute_drugs = [{"name": acute_drugs}]
        exposure_rows = []
        for index, item in enumerate(acute_drugs):
            if isinstance(item, str):
                item = {"name": item}
            if not isinstance(item, dict):
                continue
            drug = self._build_drug(item, allow_missing_source_id=True)
            exposure = Exposure(
                id=f"{case_report_id}:episode:acute-covid:exposure:{index}",
                long_drug_name=self._normalize_optional_string(item.get("name")),
            )
            exposure_rows.append((
                exposure,
                drug,
                [],
                EpisodeExposureEdge(start_node=None, end_node=None),
                ExposureDrugEdge(start_node=None, end_node=None),
            ))
        return exposure_rows

    def _build_background_regular_medicine_exposures(
        self,
        row: dict,
        case_report_id: str,
    ) -> List[tuple[Exposure, Drug, BackgroundContextExposureEdge, ExposureDrugEdge]]:
        regular_medicines = (((row.get("report") or {}).get("extra_fields") or {}).get("regular_medicines")) or []
        return self._build_background_context_exposures(
            items=regular_medicines,
            case_report_id=case_report_id,
            exposure_type="regular-medicine",
            relationship_type="regular_medicine",
        )

    def _build_background_immunosuppressant_exposures(
        self,
        row: dict,
        case_report_id: str,
    ) -> List[tuple[Exposure, Drug, BackgroundContextExposureEdge, ExposureDrugEdge]]:
        immunosuppressant_drugs = ((row.get("report") or {}).get("which_immunosuppressant_drugs")) or []
        return self._build_background_context_exposures(
            items=immunosuppressant_drugs,
            case_report_id=case_report_id,
            exposure_type="immunosuppressant",
            relationship_type="immunosuppressant",
        )

    def _build_background_context_exposures(
        self,
        items: list,
        case_report_id: str,
        exposure_type: str,
        relationship_type: str,
    ) -> List[tuple[Exposure, Drug, BackgroundContextExposureEdge, ExposureDrugEdge]]:
        exposure_rows = []
        for index, item in enumerate(items):
            if isinstance(item, str):
                item = {"name": item}
            if not isinstance(item, dict):
                continue
            drug = self._build_drug(item, allow_missing_source_id=True)
            exposure = Exposure(
                id=f"{case_report_id}:background-context:exposure:{exposure_type}:{index}",
                long_drug_name=self._normalize_optional_string(item.get("name")),
            )
            exposure_rows.append((
                exposure,
                drug,
                BackgroundContextExposureEdge(
                    start_node=None,
                    end_node=None,
                    relationship_type=relationship_type,
                ),
                ExposureDrugEdge(start_node=None, end_node=None),
            ))
        return exposure_rows

    def _build_pregnancy_episode_exposures(self, row: dict, case_report_id: str) -> List[tuple[Exposure, Drug, List[tuple[Phenotype, ExposureAdverseEventEdge]], EpisodeExposureEdge, ExposureDrugEdge]]:
        pregnancy_drugs = (((row.get("report") or {}).get("extra_fields") or {}).get("pregnancy_medications_details")) or []
        exposure_rows = []
        for index, item in enumerate(pregnancy_drugs):
            if isinstance(item, str):
                item = {"name": item}
            if not isinstance(item, dict):
                continue
            drug = self._build_drug(item, allow_missing_source_id=True)
            exposure = Exposure(
                id=f"{case_report_id}:episode:pregnancy:exposure:{index}",
                long_drug_name=self._normalize_optional_string(item.get("name")),
            )
            exposure_rows.append((
                exposure,
                drug,
                [],
                EpisodeExposureEdge(start_node=None, end_node=None),
                ExposureDrugEdge(start_node=None, end_node=None),
            ))
        return exposure_rows

    def _build_exposure_adverse_events(self, exposure: Exposure) -> List[tuple[Phenotype, ExposureAdverseEventEdge]]:
        adverse_event_rows = []
        for adverse_event_name in exposure.adverse_events:
            adverse_event = Phenotype(
                id=self._build_phenotype_id(adverse_event_name),
                name=adverse_event_name,
            )
            adverse_event_rows.append((
                adverse_event,
                ExposureAdverseEventEdge(
                    start_node=None,
                    end_node=None,
                    outcomes=list(exposure.adverse_event_outcomes),
                ),
            ))
        return adverse_event_rows

    def _build_episode_treatments(
        self,
        row: dict,
        case_report_id: str,
        exposures: List[tuple[Exposure, Drug, EpisodeExposureEdge, ExposureDrugEdge]],
    ) -> List[tuple[tuple[str, ...], Treatment, List[TreatmentExposureEdge]]]:
        alias_map = {}
        for exposure, drug, _, _, _ in exposures:
            for alias in self._build_exposure_aliases(exposure, drug):
                alias_map[alias] = exposure

        outcomes = (((row.get("report") or {}).get("extra_fields") or {}).get("symptoms_outcome") or [])
        combo_map = {}
        combo_order = []
        for item in outcomes:
            if not isinstance(item, dict):
                continue
            raw_drug_names = [self._normalize_optional_string(name) for name in (item.get("long_covid_drugs") or [])]
            raw_drug_names = [name for name in raw_drug_names if name]
            if not raw_drug_names:
                continue
            combo_key = tuple(sorted(set(raw_drug_names)))
            if combo_key not in combo_map:
                combo_map[combo_key] = self._build_treatment(case_report_id, combo_key)
                combo_order.append(combo_key)

        treatments = []
        for combo_key in combo_order:
            treatment = combo_map[combo_key]
            matched_exposures = []
            unmatched_drug_names = []
            for raw_name in combo_key:
                alias = self._normalize_alias(raw_name)
                exposure = alias_map.get(alias)
                if exposure is not None and exposure.id not in {e.id for e in matched_exposures}:
                    matched_exposures.append(exposure)
                elif exposure is None:
                    unmatched_drug_names.append(raw_name)
            treatment.unmatched_drug_names = unmatched_drug_names
            treatment.has_unmatched_drug_names = bool(unmatched_drug_names)
            treatment_edges = [
                TreatmentExposureEdge(start_node=exposure, end_node=None)
                for exposure in matched_exposures
            ]
            treatments.append((
                combo_key,
                treatment,
                treatment_edges,
            ))
        return treatments

    def _build_outcomes(
        self,
        row: dict,
        case_report_id: str,
        treatment_map: dict[tuple[str, ...], Treatment],
        phenotype_map: dict[str, Phenotype],
    ) -> List[tuple[Outcome, EpisodeOutcomeEdge, Optional[TreatmentOutcomeEdge], Optional[OutcomePhenotypeEdge]]]:
        outcomes = (((row.get("report") or {}).get("extra_fields") or {}).get("symptoms_outcome") or [])
        result = []
        for index, item in enumerate(outcomes):
            if not isinstance(item, dict):
                continue
            raw_symptom_name = self._normalize_optional_string(item.get("long_covid_symptom"))
            combo_key = self._build_treatment_combo_key(item.get("long_covid_drugs") or [])
            phenotype = phenotype_map.get(self._normalize_alias(raw_symptom_name))
            outcome = Outcome(
                id=f"{case_report_id}:outcome:{index}",
                raw_symptom_name=raw_symptom_name,
                has_unmatched_phenotype=(phenotype is None),
                effect=self._normalize_optional_string(item.get("long_covid_outcome")),
                time_to_effect_amount=self._normalize_optional_string(item.get("duration_amount")),
                time_to_effect_units=self._normalize_optional_string(item.get("duration_units")),
            )
            treatment_outcome_edge = None
            if combo_key in treatment_map:
                treatment_outcome_edge = TreatmentOutcomeEdge(
                    start_node=treatment_map[combo_key],
                    end_node=outcome,
                )
            outcome_phenotype_edge = None
            if phenotype is not None:
                outcome_phenotype_edge = OutcomePhenotypeEdge(
                    start_node=outcome,
                    end_node=phenotype,
                )
            result.append((
                outcome,
                EpisodeOutcomeEdge(start_node=None, end_node=None),
                treatment_outcome_edge,
                outcome_phenotype_edge,
            ))
        return result

    def _build_drug(self, drug_payload: dict, allow_missing_source_id: bool = False) -> Drug:
        source_id = drug_payload.get("id")
        if source_id is None and not allow_missing_source_id:
            raise ValueError("PASC regimen is missing drug.id")
        rxnorm_id = drug_payload.get("rxnorm_id")
        name = self._normalize_optional_string(drug_payload.get("name"))
        if source_id is None:
            if not name:
                raise ValueError("Acute COVID drug entry is missing both id and name")
            digest = hashlib.md5(name.encode("utf-8")).hexdigest()
            drug_id = f"CURE-ID:Drug:custom:{digest}"
        else:
            drug_id = f"CURE-ID:Drug:{source_id}"
        return Drug(
            id=drug_id,
            name=name,
            url=self._normalize_optional_string(drug_payload.get("url")),
            source_id=source_id,
            rxnorm_id=str(rxnorm_id) if rxnorm_id not in (None, "") else None,
            category=self._normalize_optional_string(drug_payload.get("category")),
            fda_approved=drug_payload.get("fda_approved"),
        )

    def _build_contextual_condition(self, name: str, source_id: Optional[int] = None) -> Condition:
        return Condition(
            id=self._build_contextual_condition_id(name),
            name=name,
            slug=self._slugify(name),
            source_id=source_id,
        )

    def _build_treatment(self, case_report_id: str, combo_key: tuple[str, ...]) -> Treatment:
        combo_string = "||".join(combo_key)
        digest = hashlib.md5(combo_string.encode("utf-8")).hexdigest()
        return Treatment(
            id=f"{case_report_id}:treatment:{digest}",
            drug_names=list(combo_key),
        )

    @classmethod
    def _build_treatment_combo_key(cls, raw_drug_names: list) -> tuple[str, ...]:
        normalized_names = [cls._normalize_optional_string(name) for name in raw_drug_names]
        normalized_names = [name for name in normalized_names if name]
        return tuple(sorted(set(normalized_names)))

    @classmethod
    def _build_phenotype_map(cls, phenotypes: List[tuple[Phenotype, EpisodePhenotypeEdge]]) -> dict[str, Phenotype]:
        phenotype_map = {}
        for phenotype, _ in phenotypes:
            key = cls._normalize_alias(phenotype.name)
            if key:
                phenotype_map[key] = phenotype
        return phenotype_map

    def _build_episode_phenotypes(self, row: dict) -> List[tuple[Phenotype, EpisodePhenotypeEdge]]:
        symptoms = (((row.get("report") or {}).get("extra_fields") or {}).get("symptoms_severity") or [])
        phenotype_pairs = []
        for symptom in symptoms:
            if not isinstance(symptom, dict):
                continue
            name = self._normalize_optional_string(symptom.get("symptom"))
            if not name:
                continue
            phenotype = Phenotype(
                id=self._build_phenotype_id(name),
                name=name,
                short_name=self._normalize_optional_string(symptom.get("symptom_short")),
            )
            edge = EpisodePhenotypeEdge(
                start_node=None,
                end_node=phenotype,
                severity=self._normalize_optional_string(symptom.get("severity")),
            )
            phenotype_pairs.append((phenotype, edge))
        return phenotype_pairs

    def _build_episode_therapies(self, row: dict) -> List[tuple[Therapy, EpisodeTherapyEdge]]:
        raw_therapies = (((row.get("report") or {}).get("extra_fields") or {}).get("alternative_therapies")) or []
        therapy_pairs = []
        for therapy_name in self._extract_value_list(raw_therapies):
            therapy = Therapy(
                id=self._build_therapy_id(therapy_name),
                name=therapy_name,
                slug=self._slugify(therapy_name),
            )
            therapy_pairs.append((therapy, EpisodeTherapyEdge(start_node=None, end_node=therapy)))
        return therapy_pairs

    @staticmethod
    def _parse_utc_datetime(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)

    @staticmethod
    def _normalize_optional_string(value: Optional[str]) -> Optional[str]:
        if value in (None, ""):
            return None
        return value

    @classmethod
    def _extract_race_list(cls, patient: dict) -> List[str]:
        race = patient.get("race")
        if isinstance(race, list):
            return [value for value in race if value]

        races = patient.get("races") or []
        values = []
        for entry in races:
            if isinstance(entry, dict):
                value = cls._normalize_optional_string(entry.get("value"))
                if value:
                    values.append(value)
        return values

    @classmethod
    def _extract_value_list(cls, value: Optional[Union[list, str]]) -> List[str]:
        if isinstance(value, list):
            values = []
            for entry in value:
                if isinstance(entry, dict):
                    item_value = cls._normalize_optional_string(entry.get("value"))
                    if item_value:
                        values.append(item_value)
                elif isinstance(entry, str):
                    item_value = cls._normalize_optional_string(entry)
                    if item_value:
                        values.append(item_value)
            return values
        if isinstance(value, str):
            item_value = cls._normalize_optional_string(value)
            if item_value:
                return [item_value]
        return []

    @classmethod
    def _extract_name_list(cls, value: Optional[list]) -> List[str]:
        if not isinstance(value, list):
            return []
        values = []
        for entry in value:
            if isinstance(entry, dict):
                item_value = cls._normalize_optional_string(entry.get("name") or entry.get("value") or entry.get("label"))
            elif isinstance(entry, str):
                item_value = cls._normalize_optional_string(entry)
            else:
                item_value = None
            if item_value:
                values.append(item_value)
        return values

    @classmethod
    def _build_exposure_aliases(cls, exposure: Exposure, drug: Drug) -> set[str]:
        aliases = set()
        for value in [drug.name, exposure.long_drug_name]:
            alias = cls._normalize_alias(value)
            if alias:
                aliases.add(alias)
        return aliases

    @staticmethod
    def _normalize_alias(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        return " ".join(value.replace("\xa0", " ").strip().lower().split())

    @classmethod
    def _slugify(cls, value: str) -> Optional[str]:
        normalized = cls._normalize_alias(value)
        if not normalized:
            return None
        return normalized.replace(" ", "-")

    @staticmethod
    def _build_primary_episode_id(case_report_id: str) -> str:
        return f"{case_report_id}:episode:primary"

    @staticmethod
    def _build_acute_episode_id(case_report_id: str) -> str:
        return f"{case_report_id}:episode:acute-covid"

    @staticmethod
    def _build_pregnancy_episode_id(case_report_id: str) -> str:
        return f"{case_report_id}:episode:pregnancy"

    @staticmethod
    def _build_phenotype_id(name: str) -> str:
        digest = hashlib.md5(name.encode("utf-8")).hexdigest()
        return f"CURE-ID:Phenotype:{digest}"

    @staticmethod
    def _build_vaccine_id(name: str) -> str:
        digest = hashlib.md5(name.encode("utf-8")).hexdigest()
        return f"CURE-ID:Vaccine:{digest}"

    @staticmethod
    def _build_therapy_id(name: str) -> str:
        digest = hashlib.md5(name.encode("utf-8")).hexdigest()
        return f"CURE-ID:Therapy:{digest}"

    @staticmethod
    def _build_contextual_condition_id(name: str) -> str:
        digest = hashlib.md5(name.encode("utf-8")).hexdigest()
        return f"CURE-ID:Condition:contextual:{digest}"
