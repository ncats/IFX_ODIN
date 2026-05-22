import csv
from collections import Counter

from src.id_resolvers.cure_id_label_resolver import CureIdLabelResolver
from src.input_adapters.cure.cure_adapter import CUREAdapter
from src.input_adapters.cure.rasopathies_adapter import RasopathiesAdapter
from src.interfaces.id_resolver import MultiMatchBehavior, NoMatchBehavior
from src.models.cure.rasopathies.drug_treatment import (
    DrugTreatmentAdverseEventEdge,
    DrugTreatmentDrugEdge,
    DrugTreatmentResponseEdge,
    PatientDrugTreatmentEdge,
    TreatmentResponse,
    TreatmentResponseFindingEdge,
)
from src.models.cure.rasopathies.finding import FindingPhenotypeEdge, PresentationFindingEdge
from src.models.cure.rasopathies.genetics import (
    DiagnosisConditionEdge,
    DiagnosisGeneEdge,
    DiagnosisGeneVariantEdge,
    GeneGeneVariantEdge,
    GeneVariant,
    PresentationDiagnosisEdge,
)
from src.models.cure.rasopathies.presentation import PatientPresentationEdge, PresentationConditionEdge
from src.models.cure.pasc.adverse_event import ExposureAdverseEventEdge
from src.models.cure.pasc.phenotype import Phenotype as PascPhenotype
from src.models.cure.shared.case_report import CaseReport
from src.models.cure.shared.patient import Patient


def _adapter_entries():
    adapter = RasopathiesAdapter("./input_files/manual/cure/reports.jsonl")
    return [entry for batch in adapter.get_all() for entry in batch]


def test_rasopathies_adapter_emits_patient_for_each_case_report():
    entries = _adapter_entries()
    counts = Counter(entry.__class__.__name__ for entry in entries)

    assert counts["CaseReport"] == 11
    assert counts["Patient"] == 11
    assert counts["CaseReportPatientEdge"] == 11
    assert counts["PatientPresentationEdge"] == 11
    assert counts["CaseReportPresentationEdge"] == 0
    assert counts["Finding"] == 134
    assert counts["PresentationFindingEdge"] == 125
    assert counts["PerinatalContextFindingEdge"] == 9
    assert counts["FindingPhenotypeEdge"] == 134
    assert counts["PresentationPhenotypeEdge"] == 0
    assert counts["PerinatalContextPhenotypeEdge"] == 0
    assert counts["DrugTreatment"] == 23
    assert counts["PatientDrugTreatmentEdge"] == 23
    assert counts["Drug"] == 23
    assert counts["DrugTreatmentDrugEdge"] == 23
    assert counts["TreatmentResponse"] == 39
    assert counts["DrugTreatmentResponseEdge"] == 39
    assert counts["TreatmentResponseFindingEdge"] == 39
    assert counts["DrugTreatmentAdverseEventEdge"] == 6
    assert counts["Diagnosis"] == 10
    assert counts["PresentationDiagnosisEdge"] == 10
    assert counts["DiagnosisConditionEdge"] == 10
    assert counts["DiagnosisGeneEdge"] == 10
    assert counts["DiagnosisGeneVariantEdge"] == 10
    assert counts["Gene"] == 10
    assert counts["GeneVariant"] == 10
    assert counts["GeneGeneVariantEdge"] == 10
    assert counts["PresentationGeneEdge"] == 0
    assert counts["PresentationGeneVariantEdge"] == 0
    assert counts["GeneConditionEdge"] == 0
    assert counts["GeneVariantConditionEdge"] == 0

    case_report = next(entry for entry in entries if isinstance(entry, CaseReport))
    assert case_report.case_report_url == (
        f"https://cure.ncats.io/explore/rasopathies/case-reports/case-details/{case_report.id}"
    )


def test_cure_pasc_case_reports_include_original_cure_id_url():
    adapter = CUREAdapter("./input_files/manual/cure/reports.jsonl", form_type="pasc")
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
        for batch in CUREAdapter("./input_files/manual/cure/reports.jsonl", form_type="pasc").get_all()
        for entry in batch
    ]
    counts = Counter(entry.__class__.__name__ for entry in entries)

    assert counts["AdverseEvent"] == 0
    assert counts["ExposureAdverseEventEdge"] == 1224
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


def test_rasopathies_presentation_findings_cover_tsv_phenotype_pairs():
    resolver_map = {
        "Phenotype": CureIdLabelResolver(
            "./input_files/manual/cure/cureid_data.tsv",
            types=["Phenotype"],
            no_match_behavior=NoMatchBehavior.Allow,
            multi_match_behavior=MultiMatchBehavior.All,
        )
    }
    adapter = RasopathiesAdapter("./input_files/manual/cure/reports.jsonl")
    entries = [
        entry
        for batch in adapter.get_resolved_and_provenanced_list(resolver_map)
        for entry in batch
    ]
    finding_to_report = {
        edge.end_node.id: edge.start_node.id.replace(":presentation", "")
        for edge in entries
        if isinstance(edge, PresentationFindingEdge)
    }
    graph_pairs = {
        (finding_to_report[edge.start_node.id], edge.end_node.id)
        for edge in entries
        if isinstance(edge, FindingPhenotypeEdge)
        and edge.start_node.id in finding_to_report
    }

    with open("./input_files/manual/cure/cureid_data.tsv", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        tsv_pairs = {
            (row["report_id"], row["object_final_curie"])
            for row in reader
            if row["predicate_raw"] == "has_phenotype_of"
        }

    assert len(graph_pairs) == 137
    assert graph_pairs == tsv_pairs


def test_rasopathies_treatment_responses_cover_tsv_drug_phenotype_pairs():
    resolver_map = {
        "Drug": CureIdLabelResolver(
            "./input_files/manual/cure/cureid_data.tsv",
            types=["Drug"],
            no_match_behavior=NoMatchBehavior.Allow,
            multi_match_behavior=MultiMatchBehavior.All,
        ),
        "Phenotype": CureIdLabelResolver(
            "./input_files/manual/cure/cureid_data.tsv",
            types=["Phenotype"],
            no_match_behavior=NoMatchBehavior.Allow,
            multi_match_behavior=MultiMatchBehavior.All,
        ),
    }
    adapter = RasopathiesAdapter("./input_files/manual/cure/reports.jsonl")
    entries = [
        entry
        for batch in adapter.get_resolved_and_provenanced_list(resolver_map)
        for entry in batch
    ]
    treatment_to_report = {
        edge.end_node.id: edge.start_node.id.replace(":patient", "")
        for edge in entries
        if isinstance(edge, PatientDrugTreatmentEdge)
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

    with open("./input_files/manual/cure/cureid_data.tsv", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        tsv_pairs = {
            (row["report_id"], row["subject_final_curie"], row["object_final_curie"])
            for row in reader
            if row["predicate_raw"] == "applied_to_treat"
            and row["object_type"] == "PhenotypicFeature"
        }

    assert len(graph_pairs) == 40
    assert graph_pairs == tsv_pairs


def test_rasopathies_drug_treatments_cover_tsv_drug_condition_pairs():
    resolver_map = {
        "Condition": CureIdLabelResolver(
            "./input_files/manual/cure/cureid_data.tsv",
            types=["Condition"],
            no_match_behavior=NoMatchBehavior.Allow,
            multi_match_behavior=MultiMatchBehavior.All,
        ),
        "Drug": CureIdLabelResolver(
            "./input_files/manual/cure/cureid_data.tsv",
            types=["Drug"],
            no_match_behavior=NoMatchBehavior.Allow,
            multi_match_behavior=MultiMatchBehavior.All,
        ),
    }
    adapter = RasopathiesAdapter("./input_files/manual/cure/reports.jsonl")
    entries = [
        entry
        for batch in adapter.get_resolved_and_provenanced_list(resolver_map)
        for entry in batch
    ]
    treatment_to_report = {
        edge.end_node.id: edge.start_node.id.replace(":patient", "")
        for edge in entries
        if isinstance(edge, PatientDrugTreatmentEdge)
    }
    treatment_to_drug = {
        edge.start_node.id: edge.end_node.id
        for edge in entries
        if isinstance(edge, DrugTreatmentDrugEdge)
    }
    presentation_to_report = {
        edge.end_node.id: edge.start_node.id.replace(":patient", "")
        for edge in entries
        if isinstance(edge, PatientPresentationEdge)
    }
    report_to_conditions = {}
    for edge in entries:
        if isinstance(edge, PresentationConditionEdge):
            report_id = presentation_to_report[edge.start_node.id]
            report_to_conditions.setdefault(report_id, set()).add(edge.end_node.id)

    graph_triples = set()
    for treatment_id, drug_id in treatment_to_drug.items():
        report_id = treatment_to_report[treatment_id]
        for condition_id in report_to_conditions.get(report_id, set()):
            graph_triples.add((report_id, drug_id, condition_id))

    with open("./input_files/manual/cure/cureid_data.tsv", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        tsv_triples = {
            (row["report_id"], row["subject_final_curie"], row["object_final_curie"])
            for row in reader
            if row["predicate_raw"] == "applied_to_treat"
            and row["object_type"] == "Disease"
        }

    assert len(graph_triples) == 23
    assert graph_triples == tsv_triples


def test_rasopathies_adverse_events_cover_tsv_has_adverse_events_triples():
    resolver_map = {
        "Drug": CureIdLabelResolver(
            "./input_files/manual/cure/cureid_data.tsv",
            types=["Drug"],
            no_match_behavior=NoMatchBehavior.Allow,
            multi_match_behavior=MultiMatchBehavior.All,
        ),
        "Phenotype": CureIdLabelResolver(
            "./input_files/manual/cure/cureid_data.tsv",
            types=["Phenotype"],
            no_match_behavior=NoMatchBehavior.Allow,
            multi_match_behavior=MultiMatchBehavior.All,
        ),
    }
    adapter = RasopathiesAdapter("./input_files/manual/cure/reports.jsonl")
    entries = [
        entry
        for batch in adapter.get_resolved_and_provenanced_list(resolver_map)
        for entry in batch
    ]
    treatment_to_report = {
        edge.end_node.id: edge.start_node.id.replace(":patient", "")
        for edge in entries
        if isinstance(edge, PatientDrugTreatmentEdge)
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

    with open("./input_files/manual/cure/cureid_data.tsv", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        tsv_triples = {
            (row["report_id"], row["subject_final_curie"], row["object_final_curie"], row["outcome"])
            for row in reader
            if row["predicate_raw"] == "has_adverse_events"
        }

    assert len(graph_triples) == 7
    assert graph_triples == tsv_triples


def test_rasopathies_genetics_cover_tsv_genetic_predicates():
    resolver_map = {
        "Condition": CureIdLabelResolver(
            "./input_files/manual/cure/cureid_data.tsv",
            types=["Condition"],
            no_match_behavior=NoMatchBehavior.Allow,
            multi_match_behavior=MultiMatchBehavior.All,
        ),
        "Gene": CureIdLabelResolver(
            "./input_files/manual/cure/cureid_data.tsv",
            types=["Gene"],
            label_field_by_type={"Gene": "symbol"},
            no_match_behavior=NoMatchBehavior.Allow,
            multi_match_behavior=MultiMatchBehavior.All,
        ),
    }
    adapter = RasopathiesAdapter("./input_files/manual/cure/reports.jsonl")
    entries = [
        entry
        for batch in adapter.get_resolved_and_provenanced_list(resolver_map)
        for entry in batch
    ]
    presentation_to_report = {
        edge.end_node.id: edge.start_node.id.replace(":patient", "")
        for edge in entries
        if isinstance(edge, PatientPresentationEdge)
    }
    diagnosis_to_report = {
        edge.end_node.id: presentation_to_report[edge.start_node.id]
        for edge in entries
        if isinstance(edge, PresentationDiagnosisEdge)
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
    with open("./input_files/manual/cure/cureid_data.tsv", newline="") as handle:
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

    assert graph_gene_condition == tsv_by_predicate["gene_associated_with_condition"]
    assert graph_gene_variant == tsv_by_predicate["has_sequence_variant"]
    assert graph_variant_condition == tsv_by_predicate["genetically_associated_with"]


def test_rasopathies_graph_reconstructs_tsv_association_set():
    resolver_map = {
        "Condition": CureIdLabelResolver(
            "./input_files/manual/cure/cureid_data.tsv",
            types=["Condition"],
            no_match_behavior=NoMatchBehavior.Allow,
            multi_match_behavior=MultiMatchBehavior.All,
        ),
        "Drug": CureIdLabelResolver(
            "./input_files/manual/cure/cureid_data.tsv",
            types=["Drug"],
            no_match_behavior=NoMatchBehavior.Allow,
            multi_match_behavior=MultiMatchBehavior.All,
        ),
        "Gene": CureIdLabelResolver(
            "./input_files/manual/cure/cureid_data.tsv",
            types=["Gene"],
            label_field_by_type={"Gene": "symbol"},
            no_match_behavior=NoMatchBehavior.Allow,
            multi_match_behavior=MultiMatchBehavior.All,
        ),
        "Phenotype": CureIdLabelResolver(
            "./input_files/manual/cure/cureid_data.tsv",
            types=["Phenotype"],
            no_match_behavior=NoMatchBehavior.Allow,
            multi_match_behavior=MultiMatchBehavior.All,
        ),
    }
    adapter = RasopathiesAdapter("./input_files/manual/cure/reports.jsonl")
    entries = [
        entry
        for batch in adapter.get_resolved_and_provenanced_list(resolver_map)
        for entry in batch
    ]

    presentation_to_report = {
        edge.end_node.id: edge.start_node.id.replace(":patient", "")
        for edge in entries
        if isinstance(edge, PatientPresentationEdge)
    }
    report_to_conditions = {}
    presentation_to_conditions = {}
    for edge in entries:
        if isinstance(edge, PresentationConditionEdge):
            presentation_to_conditions.setdefault(edge.start_node.id, set()).add(edge.end_node.id)
            report_id = presentation_to_report[edge.start_node.id]
            report_to_conditions.setdefault(report_id, set()).add(edge.end_node.id)

    finding_to_presentation = {
        edge.end_node.id: edge.start_node.id
        for edge in entries
        if isinstance(edge, PresentationFindingEdge)
    }
    finding_to_phenotypes = {}
    for edge in entries:
        if isinstance(edge, FindingPhenotypeEdge):
            finding_to_phenotypes.setdefault(edge.start_node.id, set()).add(edge.end_node.id)

    treatment_to_report = {
        edge.end_node.id: edge.start_node.id.replace(":patient", "")
        for edge in entries
        if isinstance(edge, PatientDrugTreatmentEdge)
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
        edge.end_node.id: presentation_to_report[edge.start_node.id]
        for edge in entries
        if isinstance(edge, PresentationDiagnosisEdge)
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

    for finding_id, presentation_id in finding_to_presentation.items():
        report_id = presentation_to_report[presentation_id]
        for condition_id in presentation_to_conditions.get(presentation_id, set()):
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

    with open("./input_files/manual/cure/cureid_data.tsv", newline="") as handle:
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
    assert len(graph_rows) == 237
    assert graph_rows == tsv_rows
