import csv
import json
from collections import Counter
from datetime import date

from src.id_resolvers.cure_id_label_resolver import CureIdLabelResolver
from src.input_adapters.cure.cure_adapter import CUREAdapter
from src.input_adapters.cure.rasopathies_adapter import RasopathiesAdapter
from src.interfaces.id_resolver import MultiMatchBehavior, NoMatchBehavior
from src.models.cure.rasopathies.drug_treatment import (
    DrugTreatmentAdverseEventEdge,
    DrugTreatmentDrugEdge,
    DrugTreatmentResponseEdge,
    ClinicalContextDrugTreatmentEdge,
    TreatmentResponse,
    TreatmentResponseFindingEdge,
)
from src.models.cure.rasopathies.finding import FindingPhenotypeEdge, ClinicalContextFindingEdge
from src.models.cure.rasopathies.genetics import (
    DiagnosisConditionEdge,
    DiagnosisGeneEdge,
    DiagnosisGeneVariantEdge,
    GeneGeneVariantEdge,
    GeneVariant,
    ClinicalContextDiagnosisEdge,
)
from src.models.cure.rasopathies.clinical_context import PatientClinicalContextEdge, ClinicalContextConditionEdge
from src.models.cure.pasc.adverse_event import ExposureAdverseEventEdge
from src.models.cure.pasc.phenotype import Phenotype as PascPhenotype
from src.models.cure.shared.case_report import CaseReport
from src.models.cure.shared.patient import Patient
from src.models.gene import Gene


CURE_REPORTS_FILE = "./input_files/manual/cure/reports_20260518T211409Z.jsonl"
CURE_TSV_FILE = "./input_files/manual/cure/cureid_data.tsv"


def _adapter_entries():
    adapter = RasopathiesAdapter(CURE_REPORTS_FILE)
    return [entry for batch in adapter.get_all() for entry in batch]


def test_rasopathies_adapter_emits_patient_for_each_case_report():
    entries = _adapter_entries()
    counts = Counter(entry.__class__.__name__ for entry in entries)

    assert counts["CaseReport"] == 13
    assert counts["Patient"] == 13
    assert counts["CaseReportPatientEdge"] == 13
    assert counts["PatientClinicalContextEdge"] == 13
    assert counts["CaseReportClinicalContextEdge"] == 0
    assert counts["Finding"] == 140
    assert counts["ClinicalContextFindingEdge"] == 131
    assert counts["PerinatalContextFindingEdge"] == 9
    assert counts["FindingPhenotypeEdge"] == 140
    assert counts["ClinicalContextPhenotypeEdge"] == 0
    assert counts["PerinatalContextPhenotypeEdge"] == 0
    assert counts["DrugTreatment"] == 28
    assert counts["ClinicalContextDrugTreatmentEdge"] == 28
    assert counts["Drug"] == 28
    assert counts["DrugTreatmentDrugEdge"] == 28
    assert counts["TreatmentResponse"] == 46
    assert counts["DrugTreatmentResponseEdge"] == 46
    assert counts["TreatmentResponseFindingEdge"] == 46
    assert counts["DrugTreatmentAdverseEventEdge"] == 7
    assert counts["Diagnosis"] == 12
    assert counts["ClinicalContextDiagnosisEdge"] == 12
    assert counts["DiagnosisConditionEdge"] == 12
    assert counts["DiagnosisGeneEdge"] == 12
    assert counts["DiagnosisGeneVariantEdge"] == 12
    assert counts["Gene"] == 12
    assert counts["GeneVariant"] == 12
    assert counts["GeneGeneVariantEdge"] == 12
    assert counts["ClinicalContextGeneEdge"] == 0
    assert counts["ClinicalContextGeneVariantEdge"] == 0
    assert counts["GeneConditionEdge"] == 0
    assert counts["GeneVariantConditionEdge"] == 0

    case_report = next(entry for entry in entries if isinstance(entry, CaseReport))
    assert case_report.case_report_url == (
        f"https://cure.ncats.io/explore/rasopathies/case-reports/case-details/{case_report.id}"
    )


def test_rasopathies_adapter_reads_version_date_from_file_name():
    version = RasopathiesAdapter(CURE_REPORTS_FILE).get_version()

    assert version.version == "reports_20260518T211409Z"
    assert version.version_date == date(2026, 5, 18)


def test_cure_pasc_adapter_reads_version_date_from_file_name():
    version = CUREAdapter(CURE_REPORTS_FILE, form_type="pasc").get_version()

    assert version.version == "reports_20260518T211409Z"
    assert version.version_date == date(2026, 5, 18)


def test_cure_id_label_resolver_applies_manual_yaml_style_label_map():
    resolver = CureIdLabelResolver(
        CURE_TSV_FILE,
        types=["Gene"],
        label_field_by_type={"Gene": "symbol"},
        manual_label_map={
            "Gene": {
                "Syngap": ["NCBIGene:8831"],
                "SYNGAP1": ["NCBIGene:8831"],
            }
        },
        no_match_behavior=NoMatchBehavior.Allow,
        multi_match_behavior=MultiMatchBehavior.All,
    )

    resolved = resolver.resolve_internal([
        Gene(id="Syngap", symbol="Syngap"),
        Gene(id="SYNGAP1", symbol="SYNGAP1"),
    ])

    assert [match.match for match in resolved["Syngap"]] == ["NCBIGene:8831"]
    assert [match.match for match in resolved["SYNGAP1"]] == ["NCBIGene:8831"]


def test_rasopathies_adapter_only_emits_approved_cases(tmp_path):
    with open(CURE_REPORTS_FILE, "r", encoding="utf-8") as handle:
        for line in handle:
            approved_row = json.loads(line)
            if approved_row.get("form_type") == "rasopathies":
                break
        else:
            raise AssertionError("expected at least one rasopathies row")
    rejected_row = dict(approved_row)
    rejected_row["id"] = "not-approved-rasopathies-case"
    rejected_row["status"] = "Rejected"

    test_file = tmp_path / "reports_20260518T211409Z.jsonl"
    test_file.write_text(
        "\n".join(json.dumps(row) for row in [approved_row, rejected_row]) + "\n",
        encoding="utf-8",
    )

    entries = [
        entry
        for batch in RasopathiesAdapter(str(test_file)).get_all()
        for entry in batch
    ]

    assert {
        entry.id
        for entry in entries
        if isinstance(entry, CaseReport)
    } == {approved_row["id"]}


def test_cure_pasc_adapter_only_emits_approved_cases(tmp_path):
    with open(CURE_REPORTS_FILE, "r", encoding="utf-8") as handle:
        for line in handle:
            approved_row = json.loads(line)
            if approved_row.get("form_type") == "pasc":
                break
        else:
            raise AssertionError("expected at least one pasc row")
    rejected_row = dict(approved_row)
    rejected_row["id"] = "not-approved-pasc-case"
    rejected_row["status"] = "Rejected"

    test_file = tmp_path / "reports_20260518T211409Z.jsonl"
    test_file.write_text(
        "\n".join(json.dumps(row) for row in [approved_row, rejected_row]) + "\n",
        encoding="utf-8",
    )

    entries = [
        entry
        for batch in CUREAdapter(str(test_file), form_type="pasc").get_all()
        for entry in batch
    ]

    assert {
        entry.id
        for entry in entries
        if isinstance(entry, CaseReport)
    } == {approved_row["id"]}


def test_cure_pasc_case_reports_include_original_cure_id_url():
    adapter = CUREAdapter(CURE_REPORTS_FILE, form_type="pasc")
    case_report = next(
        entry
        for batch in adapter.get_all()
        for entry in batch
        if isinstance(entry, CaseReport)
    )

    assert case_report.case_report_url == (
        f"https://cure.ncats.io/explore/long-covid/case-reports/case-details/{case_report.id}"
    )


def test_cure_pasc_adverse_events_are_modeled_as_phenotypes():
    entries = [
        entry
        for batch in CUREAdapter(CURE_REPORTS_FILE, form_type="pasc").get_all()
        for entry in batch
    ]
    counts = Counter(entry.__class__.__name__ for entry in entries)

    assert counts["AdverseEvent"] == 0
    assert counts["ExposureAdverseEventEdge"] == 1228
    assert all(
        isinstance(edge.end_node, PascPhenotype)
        for edge in entries
        if isinstance(edge, ExposureAdverseEventEdge)
    )


def test_rasopathies_adapter_keeps_empty_demographic_patient_anchor():
    entries = _adapter_entries()
    patient = next(
        entry
        for entry in entries
        if isinstance(entry, Patient)
        and entry.id == "86a9442a-f603-47b4-9c7b-ce4ba7140301:patient"
    )

    assert patient.sex is None
    assert patient.age_group is None
    assert patient.ethnicity is None
    assert patient.country_treated is None
    assert patient.race == []


def test_rasopathies_clinical_context_findings_cover_tsv_phenotype_pairs():
    resolver_map = {
        "Phenotype": CureIdLabelResolver(
            CURE_TSV_FILE,
            types=["Phenotype"],
            no_match_behavior=NoMatchBehavior.Allow,
            multi_match_behavior=MultiMatchBehavior.All,
        )
    }
    adapter = RasopathiesAdapter(CURE_REPORTS_FILE)
    entries = [
        entry
        for batch in adapter.get_resolved_and_provenanced_list(resolver_map)
        for entry in batch
    ]
    finding_to_report = {
        edge.end_node.id: edge.start_node.id.replace(":clinical_context", "")
        for edge in entries
        if isinstance(edge, ClinicalContextFindingEdge)
    }
    graph_pairs = {
        (finding_to_report[edge.start_node.id], edge.end_node.id)
        for edge in entries
        if isinstance(edge, FindingPhenotypeEdge)
        and edge.start_node.id in finding_to_report
    }

    with open(CURE_TSV_FILE, newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        tsv_pairs = {
            (row["report_id"], row["object_final_curie"])
            for row in reader
            if row["predicate_raw"] == "has_phenotype_of"
        }

    assert len(tsv_pairs) == 137
    assert len(graph_pairs) == 145
    assert tsv_pairs <= graph_pairs


def test_rasopathies_treatment_responses_cover_tsv_drug_phenotype_pairs():
    resolver_map = {
        "Drug": CureIdLabelResolver(
            CURE_TSV_FILE,
            types=["Drug"],
            no_match_behavior=NoMatchBehavior.Allow,
            multi_match_behavior=MultiMatchBehavior.All,
        ),
        "Phenotype": CureIdLabelResolver(
            CURE_TSV_FILE,
            types=["Phenotype"],
            no_match_behavior=NoMatchBehavior.Allow,
            multi_match_behavior=MultiMatchBehavior.All,
        ),
    }
    adapter = RasopathiesAdapter(CURE_REPORTS_FILE)
    entries = [
        entry
        for batch in adapter.get_resolved_and_provenanced_list(resolver_map)
        for entry in batch
    ]
    clinical_context_to_report = {
        edge.end_node.id: edge.start_node.id.replace(":patient", "")
        for edge in entries
        if isinstance(edge, PatientClinicalContextEdge)
    }
    treatment_to_report = {
        edge.end_node.id: clinical_context_to_report[edge.start_node.id]
        for edge in entries
        if isinstance(edge, ClinicalContextDrugTreatmentEdge)
    }
    treatment_to_drug = {
        edge.start_node.id: edge.end_node.id
        for edge in entries
        if isinstance(edge, DrugTreatmentDrugEdge)
    }
    response_to_treatment = {
        edge.end_node.id: edge.start_node.id
        for edge in entries
        if isinstance(edge, DrugTreatmentResponseEdge)
    }
    response_to_outcome = {
        entry.id: entry.outcome or ""
        for entry in entries
        if isinstance(entry, TreatmentResponse)
    }
    response_to_finding = {
        edge.start_node.id: edge.end_node.id
        for edge in entries
        if isinstance(edge, TreatmentResponseFindingEdge)
    }
    finding_to_phenotypes = {}
    for edge in entries:
        if isinstance(edge, FindingPhenotypeEdge):
            finding_to_phenotypes.setdefault(edge.start_node.id, set()).add(edge.end_node.id)

    graph_pairs = set()
    for response_id, finding_id in response_to_finding.items():
        treatment_id = response_to_treatment[response_id]
        report_id = treatment_to_report[treatment_id]
        drug_id = treatment_to_drug[treatment_id]
        for phenotype_id in finding_to_phenotypes.get(finding_id, []):
            graph_pairs.add((report_id, drug_id, phenotype_id))

    with open(CURE_TSV_FILE, newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        tsv_pairs = {
            (row["report_id"], row["subject_final_curie"], row["object_final_curie"])
            for row in reader
            if row["predicate_raw"] == "applied_to_treat"
            and row["object_type"] == "PhenotypicFeature"
        }

    assert len(tsv_pairs) == 40
    assert len(graph_pairs) == 49
    assert tsv_pairs <= graph_pairs


def test_rasopathies_drug_treatments_cover_tsv_drug_condition_pairs():
    resolver_map = {
        "Condition": CureIdLabelResolver(
            CURE_TSV_FILE,
            types=["Condition"],
            no_match_behavior=NoMatchBehavior.Allow,
            multi_match_behavior=MultiMatchBehavior.All,
        ),
        "Drug": CureIdLabelResolver(
            CURE_TSV_FILE,
            types=["Drug"],
            no_match_behavior=NoMatchBehavior.Allow,
            multi_match_behavior=MultiMatchBehavior.All,
        ),
    }
    adapter = RasopathiesAdapter(CURE_REPORTS_FILE)
    entries = [
        entry
        for batch in adapter.get_resolved_and_provenanced_list(resolver_map)
        for entry in batch
    ]
    clinical_context_to_report = {
        edge.end_node.id: edge.start_node.id.replace(":patient", "")
        for edge in entries
        if isinstance(edge, PatientClinicalContextEdge)
    }
    treatment_to_report = {
        edge.end_node.id: clinical_context_to_report[edge.start_node.id]
        for edge in entries
        if isinstance(edge, ClinicalContextDrugTreatmentEdge)
    }
    treatment_to_drug = {
        edge.start_node.id: edge.end_node.id
        for edge in entries
        if isinstance(edge, DrugTreatmentDrugEdge)
    }
    report_to_conditions = {}
    for edge in entries:
        if isinstance(edge, ClinicalContextConditionEdge):
            report_id = clinical_context_to_report[edge.start_node.id]
            report_to_conditions.setdefault(report_id, set()).add(edge.end_node.id)

    graph_triples = set()
    for treatment_id, drug_id in treatment_to_drug.items():
        report_id = treatment_to_report[treatment_id]
        for condition_id in report_to_conditions.get(report_id, set()):
            graph_triples.add((report_id, drug_id, condition_id))

    with open(CURE_TSV_FILE, newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        tsv_triples = {
            (row["report_id"], row["subject_final_curie"], row["object_final_curie"])
            for row in reader
            if row["predicate_raw"] == "applied_to_treat"
            and row["object_type"] == "Disease"
        }

    assert len(tsv_triples) == 23
    assert len(graph_triples) == 28
    assert tsv_triples <= graph_triples


def test_rasopathies_adverse_events_cover_tsv_has_adverse_events_triples():
    resolver_map = {
        "Drug": CureIdLabelResolver(
            CURE_TSV_FILE,
            types=["Drug"],
            no_match_behavior=NoMatchBehavior.Allow,
            multi_match_behavior=MultiMatchBehavior.All,
        ),
        "Phenotype": CureIdLabelResolver(
            CURE_TSV_FILE,
            types=["Phenotype"],
            no_match_behavior=NoMatchBehavior.Allow,
            multi_match_behavior=MultiMatchBehavior.All,
        ),
    }
    adapter = RasopathiesAdapter(CURE_REPORTS_FILE)
    entries = [
        entry
        for batch in adapter.get_resolved_and_provenanced_list(resolver_map)
        for entry in batch
    ]
    clinical_context_to_report = {
        edge.end_node.id: edge.start_node.id.replace(":patient", "")
        for edge in entries
        if isinstance(edge, PatientClinicalContextEdge)
    }
    treatment_to_report = {
        edge.end_node.id: clinical_context_to_report[edge.start_node.id]
        for edge in entries
        if isinstance(edge, ClinicalContextDrugTreatmentEdge)
    }
    treatment_to_drug = {
        edge.start_node.id: edge.end_node.id
        for edge in entries
        if isinstance(edge, DrugTreatmentDrugEdge)
    }

    graph_triples = {
        (
            treatment_to_report[edge.start_node.id],
            treatment_to_drug[edge.start_node.id],
            edge.end_node.id,
            "; ".join(edge.outcomes or []),
        )
        for edge in entries
        if isinstance(edge, DrugTreatmentAdverseEventEdge)
    }

    with open(CURE_TSV_FILE, newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        tsv_triples = {
            (row["report_id"], row["subject_final_curie"], row["object_final_curie"], row["outcome"])
            for row in reader
            if row["predicate_raw"] == "has_adverse_events"
        }

    assert len(tsv_triples) == 7
    assert len(graph_triples) == 8
    assert tsv_triples <= graph_triples


def test_rasopathies_genetics_cover_tsv_genetic_predicates():
    resolver_map = {
        "Condition": CureIdLabelResolver(
            CURE_TSV_FILE,
            types=["Condition"],
            no_match_behavior=NoMatchBehavior.Allow,
            multi_match_behavior=MultiMatchBehavior.All,
        ),
        "Gene": CureIdLabelResolver(
            CURE_TSV_FILE,
            types=["Gene"],
            label_field_by_type={"Gene": "symbol"},
            no_match_behavior=NoMatchBehavior.Allow,
            multi_match_behavior=MultiMatchBehavior.All,
        ),
    }
    adapter = RasopathiesAdapter(CURE_REPORTS_FILE)
    entries = [
        entry
        for batch in adapter.get_resolved_and_provenanced_list(resolver_map)
        for entry in batch
    ]
    clinical_context_to_report = {
        edge.end_node.id: edge.start_node.id.replace(":patient", "")
        for edge in entries
        if isinstance(edge, PatientClinicalContextEdge)
    }
    diagnosis_to_report = {
        edge.end_node.id: clinical_context_to_report[edge.start_node.id]
        for edge in entries
        if isinstance(edge, ClinicalContextDiagnosisEdge)
    }
    diagnosis_to_conditions = {}
    for edge in entries:
        if isinstance(edge, DiagnosisConditionEdge):
            diagnosis_to_conditions.setdefault(edge.start_node.id, set()).add(edge.end_node.id)
    diagnosis_gene_pairs = {
        (edge.start_node.id, edge.end_node.id)
        for edge in entries
        if isinstance(edge, DiagnosisGeneEdge)
    }
    variant_to_diagnosis = {
        edge.end_node.id: edge.start_node.id
        for edge in entries
        if isinstance(edge, DiagnosisGeneVariantEdge)
    }
    variant_labels = {
        entry.id: entry.variant_label
        for entry in entries
        if isinstance(entry, GeneVariant)
    }

    graph_gene_condition = {
        (diagnosis_to_report[diagnosis_id], gene_id, condition_id)
        for diagnosis_id, gene_id in diagnosis_gene_pairs
        for condition_id in diagnosis_to_conditions.get(diagnosis_id, set())
    }

    graph_gene_variant = {
        (
            diagnosis_to_report[variant_to_diagnosis[edge.end_node.id]],
            edge.start_node.id,
            variant_labels[edge.end_node.id],
        )
        for edge in entries
        if isinstance(edge, GeneGeneVariantEdge)
    }
    graph_variant_condition = {
        (diagnosis_to_report[diagnosis_id], variant_labels[variant_id], condition_id)
        for variant_id, diagnosis_id in variant_to_diagnosis.items()
        for condition_id in diagnosis_to_conditions.get(diagnosis_id, set())
    }

    tsv_by_predicate = {}
    with open(CURE_TSV_FILE, newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            if row["predicate_raw"] not in {
                "gene_associated_with_condition",
                "has_sequence_variant",
                "genetically_associated_with",
            }:
                continue
            tsv_by_predicate.setdefault(row["predicate_raw"], set()).add((
                row["report_id"],
                row["subject_final_curie"] or row["subject_label_original"],
                row["object_final_curie"] or row["object_label_original"],
            ))

    assert tsv_by_predicate["gene_associated_with_condition"] <= graph_gene_condition
    assert tsv_by_predicate["has_sequence_variant"] <= graph_gene_variant
    assert tsv_by_predicate["genetically_associated_with"] <= graph_variant_condition


def test_rasopathies_graph_reconstructs_tsv_association_set():
    resolver_map = {
        "Condition": CureIdLabelResolver(
            CURE_TSV_FILE,
            types=["Condition"],
            no_match_behavior=NoMatchBehavior.Allow,
            multi_match_behavior=MultiMatchBehavior.All,
        ),
        "Drug": CureIdLabelResolver(
            CURE_TSV_FILE,
            types=["Drug"],
            no_match_behavior=NoMatchBehavior.Allow,
            multi_match_behavior=MultiMatchBehavior.All,
        ),
        "Gene": CureIdLabelResolver(
            CURE_TSV_FILE,
            types=["Gene"],
            label_field_by_type={"Gene": "symbol"},
            no_match_behavior=NoMatchBehavior.Allow,
            multi_match_behavior=MultiMatchBehavior.All,
        ),
        "Phenotype": CureIdLabelResolver(
            CURE_TSV_FILE,
            types=["Phenotype"],
            no_match_behavior=NoMatchBehavior.Allow,
            multi_match_behavior=MultiMatchBehavior.All,
        ),
    }
    adapter = RasopathiesAdapter(CURE_REPORTS_FILE)
    entries = [
        entry
        for batch in adapter.get_resolved_and_provenanced_list(resolver_map)
        for entry in batch
    ]

    clinical_context_to_report = {
        edge.end_node.id: edge.start_node.id.replace(":patient", "")
        for edge in entries
        if isinstance(edge, PatientClinicalContextEdge)
    }
    report_to_conditions = {}
    clinical_context_to_conditions = {}
    for edge in entries:
        if isinstance(edge, ClinicalContextConditionEdge):
            clinical_context_to_conditions.setdefault(edge.start_node.id, set()).add(edge.end_node.id)
            report_id = clinical_context_to_report[edge.start_node.id]
            report_to_conditions.setdefault(report_id, set()).add(edge.end_node.id)

    finding_to_clinical_context = {
        edge.end_node.id: edge.start_node.id
        for edge in entries
        if isinstance(edge, ClinicalContextFindingEdge)
    }
    finding_to_phenotypes = {}
    for edge in entries:
        if isinstance(edge, FindingPhenotypeEdge):
            finding_to_phenotypes.setdefault(edge.start_node.id, set()).add(edge.end_node.id)

    treatment_to_report = {
        edge.end_node.id: clinical_context_to_report[edge.start_node.id]
        for edge in entries
        if isinstance(edge, ClinicalContextDrugTreatmentEdge)
    }
    treatment_to_drug = {
        edge.start_node.id: edge.end_node.id
        for edge in entries
        if isinstance(edge, DrugTreatmentDrugEdge)
    }
    response_to_treatment = {
        edge.end_node.id: edge.start_node.id
        for edge in entries
        if isinstance(edge, DrugTreatmentResponseEdge)
    }
    response_to_outcome = {
        entry.id: entry.outcome or ""
        for entry in entries
        if isinstance(entry, TreatmentResponse)
    }
    response_to_finding = {
        edge.start_node.id: edge.end_node.id
        for edge in entries
        if isinstance(edge, TreatmentResponseFindingEdge)
    }

    diagnosis_to_report = {
        edge.end_node.id: clinical_context_to_report[edge.start_node.id]
        for edge in entries
        if isinstance(edge, ClinicalContextDiagnosisEdge)
    }
    diagnosis_to_conditions = {}
    for edge in entries:
        if isinstance(edge, DiagnosisConditionEdge):
            diagnosis_to_conditions.setdefault(edge.start_node.id, set()).add(edge.end_node.id)
    diagnosis_gene_pairs = {
        (edge.start_node.id, edge.end_node.id)
        for edge in entries
        if isinstance(edge, DiagnosisGeneEdge)
    }
    variant_to_diagnosis = {
        edge.end_node.id: edge.start_node.id
        for edge in entries
        if isinstance(edge, DiagnosisGeneVariantEdge)
    }
    variant_labels = {
        entry.id: entry.variant_label
        for entry in entries
        if isinstance(entry, GeneVariant)
    }

    graph_rows = set()

    for finding_id, clinical_context_id in finding_to_clinical_context.items():
        report_id = clinical_context_to_report[clinical_context_id]
        for condition_id in clinical_context_to_conditions.get(clinical_context_id, set()):
            for phenotype_id in finding_to_phenotypes.get(finding_id, set()):
                graph_rows.add((
                    report_id,
                    "Disease",
                    "has_phenotype_of",
                    "PhenotypicFeature",
                    condition_id,
                    phenotype_id,
                    "",
                ))

    for response_id, finding_id in response_to_finding.items():
        treatment_id = response_to_treatment[response_id]
        report_id = treatment_to_report[treatment_id]
        drug_id = treatment_to_drug[treatment_id]
        for phenotype_id in finding_to_phenotypes.get(finding_id, set()):
                graph_rows.add((
                    report_id,
                    "Drug",
                    "applied_to_treat",
                    "PhenotypicFeature",
                    drug_id,
                    phenotype_id,
                    response_to_outcome[response_id],
                ))

    for treatment_id, drug_id in treatment_to_drug.items():
        report_id = treatment_to_report[treatment_id]
        for condition_id in report_to_conditions.get(report_id, set()):
            graph_rows.add((
                report_id,
                "Drug",
                "applied_to_treat",
                "Disease",
                drug_id,
                condition_id,
                "",
            ))

    for edge in entries:
        if isinstance(edge, DrugTreatmentAdverseEventEdge):
            graph_rows.add((
                treatment_to_report[edge.start_node.id],
                "Drug",
                "has_adverse_events",
                "AdverseEvent",
                treatment_to_drug[edge.start_node.id],
                edge.end_node.id,
                "; ".join(edge.outcomes or []),
            ))

    for diagnosis_id, gene_id in diagnosis_gene_pairs:
        report_id = diagnosis_to_report[diagnosis_id]
        for condition_id in diagnosis_to_conditions.get(diagnosis_id, set()):
            graph_rows.add((
                report_id,
                "Gene",
                "gene_associated_with_condition",
                "Disease",
                gene_id,
                condition_id,
                "",
            ))

    for edge in entries:
        if isinstance(edge, GeneGeneVariantEdge):
            graph_rows.add((
                diagnosis_to_report[variant_to_diagnosis[edge.end_node.id]],
                "Gene",
                "has_sequence_variant",
                "SequenceVariant",
                edge.start_node.id,
                variant_labels[edge.end_node.id],
                "",
            ))

    for variant_id, diagnosis_id in variant_to_diagnosis.items():
        report_id = diagnosis_to_report[diagnosis_id]
        for condition_id in diagnosis_to_conditions.get(diagnosis_id, set()):
            graph_rows.add((
                report_id,
                "SequenceVariant",
                "genetically_associated_with",
                "Disease",
                variant_labels[variant_id],
                condition_id,
                "",
            ))

    with open(CURE_TSV_FILE, newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        tsv_rows = {
            (
                row["report_id"],
                row["subject_type"],
                row["predicate_raw"],
                row["object_type"],
                row["subject_final_curie"] or row["subject_label_original"],
                row["object_final_curie"] or row["object_label_original"],
                row["outcome"],
            )
            for row in reader
        }

    assert len(tsv_rows) == 237
    assert len(graph_rows) == 266
    assert tsv_rows <= graph_rows
