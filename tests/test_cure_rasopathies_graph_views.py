import yaml


def test_rasopathies_graph_views_only_include_final_exports():
    with open("./src/use_cases/cure/cure_rasopathies.yaml", "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    graph_views = {view["id"]: view for view in config["graph_views"]}

    assert set(graph_views) == {
        "rasopathies_condition_has_phenotype",
        "rasopathies_drug_applied_to_treat_condition",
        "rasopathies_drug_applied_to_treat_phenotype",
        "rasopathies_drug_has_adverse_event",
        "rasopathies_gene_associated_with_condition",
        "rasopathies_gene_has_sequence_variant",
        "rasopathies_sequence_variant_genetically_associated_with_condition",
        "rasopathies_translator_version_info",
    }


def test_rasopathies_resolver_config_includes_temporary_manual_gene_map():
    with open("./src/use_cases/cure/cure_rasopathies.yaml", "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    resolver = config["resolvers"][0]
    manual_gene_map = resolver["kwargs"]["manual_label_map"]["Gene"]
    manual_drug_map = resolver["kwargs"]["manual_label_map"]["Drug"]

    assert manual_gene_map["Syngap"] == ["NCBIGene:8831"]
    assert manual_gene_map["SYNGAP1"] == ["NCBIGene:8831"]
    assert manual_drug_map["Epidiolex"] == ["RXCUI:2058901"]
    assert manual_drug_map["Tanganil"] == ["CHEBI:17786"]
    assert manual_drug_map["Sertraline"] == ["RXCUI:36437"]
    assert manual_drug_map["Sertraline 25 Mg Oral Tablet [Zoloft]"] == ["RXCUI:36437"]
    assert manual_drug_map["Clobazam"] == ["RXCUI:21241"]


def test_rasopathies_translator_version_info_graph_view_shape():
    with open("./src/use_cases/cure/cure_rasopathies.yaml", "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    graph_views = {view["id"]: view for view in config["graph_views"]}
    view = graph_views["rasopathies_translator_version_info"]
    query = view["query"]

    assert view["output_format"] == "jsonl"
    assert view["query_language"] == "aql"
    assert "columns" not in view
    assert 'label: "Translator: RAS"' in query
    assert "source_versions" in query
    assert "version_date" in query
    assert "download_date" in query
    assert 'form_type: "rasopathies"' in query
    assert 'status: "Approved"' in query
    assert "case_report_count" in query
    assert "case_report_ids" not in query
    assert "association_view_ids" in query
    assert "manual-cureid-curated-concepts" not in query
    assert "reports_20260518T211409Z" not in query
    assert "biolink" not in query.lower()


def test_rasopathies_condition_has_phenotype_graph_view_shape():
    with open("./src/use_cases/cure/cure_rasopathies.yaml", "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    graph_views = {view["id"]: view for view in config["graph_views"]}
    view = graph_views["rasopathies_condition_has_phenotype"]
    query = view["query"]

    assert view["output_format"] == "jsonl"
    assert view["query_language"] == "aql"
    assert "columns" not in view
    assert "FOR condition IN `Condition`" in query
    assert "INBOUND condition `ClinicalContextConditionEdge`" in query
    assert "OUTBOUND clinical_context `ClinicalContextFindingEdge`" in query
    assert "OUTBOUND finding `FindingPhenotypeEdge`" in query
    assert "INBOUND clinical_context `PatientClinicalContextEdge`" in query
    assert "INBOUND patient `CaseReportPatientEdge`" in query
    assert 'id: "has_phenotype"' in query
    assert 'label: "has phenotype"' in query
    assert "patient_count" in query
    assert "case_report_count" in query
    assert "SORT patient_count DESC, case_report_count DESC" in query
    assert "evidence" in query
    assert "source_value" in query
    assert "source_text" in query
    assert "group" in query
    assert "biolink" not in query.lower()


def test_rasopathies_drug_applied_to_treat_condition_graph_view_shape():
    with open("./src/use_cases/cure/cure_rasopathies.yaml", "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    graph_views = {view["id"]: view for view in config["graph_views"]}
    view = graph_views["rasopathies_drug_applied_to_treat_condition"]
    query = view["query"]

    assert view["output_format"] == "jsonl"
    assert view["query_language"] == "aql"
    assert "columns" not in view
    assert "FOR drug IN `Drug`" in query
    assert "INBOUND drug `DrugTreatmentDrugEdge`" in query
    assert "INBOUND drug_treatment `ClinicalContextDrugTreatmentEdge`" in query
    assert "OUTBOUND clinical_context `ClinicalContextConditionEdge`" in query
    assert "INBOUND clinical_context `PatientClinicalContextEdge`" in query
    assert "INBOUND patient `CaseReportPatientEdge`" in query
    assert 'id: "applied_to_treat"' in query
    assert 'label: "applied to treat"' in query
    assert "patient_count" in query
    assert "case_report_count" in query
    assert "SORT patient_count DESC, case_report_count DESC" in query
    assert "evidence" in query
    assert "initial_dose_amount" in query
    assert "initial_unit_of_measurement" in query
    assert "initial_frequency" in query
    assert "initial_route" in query
    assert "current_dose_amount" in query
    assert "current_unit_of_measurement" in query
    assert "current_frequency" in query
    assert "current_route" in query
    assert "current_dose_change" in query
    assert "duration_amount" in query
    assert "unit_of_measurement_duration" in query
    assert "treatment_begin" in query
    assert "treatment_begin_month" in query
    assert "treatment_end" in query
    assert "treatment_end_month" in query
    assert "treatment_on_going" in query
    assert "severity" in query
    assert "additional_drug_info" in query
    assert "sex" in query
    assert "age_group" in query
    assert "ethnicity" in query
    assert "country_treated" in query
    assert "race" in query
    assert "case_report_url" in query
    assert "biolink" not in query.lower()


def test_rasopathies_drug_applied_to_treat_phenotype_graph_view_shape():
    with open("./src/use_cases/cure/cure_rasopathies.yaml", "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    graph_views = {view["id"]: view for view in config["graph_views"]}
    view = graph_views["rasopathies_drug_applied_to_treat_phenotype"]
    query = view["query"]

    assert view["output_format"] == "jsonl"
    assert view["query_language"] == "aql"
    assert "columns" not in view
    assert "FOR drug IN `Drug`" in query
    assert "INBOUND drug `DrugTreatmentDrugEdge`" in query
    assert "OUTBOUND drug_treatment `DrugTreatmentResponseEdge`" in query
    assert "OUTBOUND treatment_response `TreatmentResponseFindingEdge`" in query
    assert "OUTBOUND finding `FindingPhenotypeEdge`" in query
    assert "INBOUND drug_treatment `ClinicalContextDrugTreatmentEdge`" in query
    assert "INBOUND clinical_context `PatientClinicalContextEdge`" in query
    assert "INBOUND patient `CaseReportPatientEdge`" in query
    assert 'id: "applied_to_treat"' in query
    assert 'label: "applied to treat"' in query
    assert "patient_count" in query
    assert "case_report_count" in query
    assert "SORT patient_count DESC, case_report_count DESC" in query
    assert "outcomes" in query
    assert "evidence" in query
    assert "phenotype" in query
    assert "target_role" in query
    assert "outcome" in query
    assert "outcome_details" in query
    assert "time_to_improvement" in query
    assert "source_value" in query
    assert "source_text" in query
    assert "raw_text" not in query
    assert "finding_context" not in query
    assert "selected" not in query
    assert "default" not in query
    assert "biolink" not in query.lower()


def test_rasopathies_gene_associated_with_condition_graph_view_shape():
    with open("./src/use_cases/cure/cure_rasopathies.yaml", "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    graph_views = {view["id"]: view for view in config["graph_views"]}
    view = graph_views["rasopathies_gene_associated_with_condition"]
    query = view["query"]

    assert view["output_format"] == "jsonl"
    assert view["query_language"] == "aql"
    assert "columns" not in view
    assert "FOR gene IN `Gene`" in query
    assert "INBOUND gene `DiagnosisGeneEdge`" in query
    assert "OUTBOUND diagnosis `DiagnosisConditionEdge`" in query
    assert "INBOUND diagnosis `ClinicalContextDiagnosisEdge`" in query
    assert "INBOUND clinical_context `PatientClinicalContextEdge`" in query
    assert "INBOUND patient `CaseReportPatientEdge`" in query
    assert 'id: "gene_associated_with_condition"' in query
    assert 'label: "gene associated with condition"' in query
    assert "patient_count" in query
    assert "case_report_count" in query
    assert "SORT patient_count DESC, case_report_count DESC" in query
    assert "evidence" in query
    assert "symbol: gene_doc.symbol" in query
    assert "diagnosis_methods" in query
    assert "case_report_url" in query
    assert "biolink" not in query.lower()


def test_rasopathies_drug_has_adverse_event_graph_view_shape():
    with open("./src/use_cases/cure/cure_rasopathies.yaml", "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    graph_views = {view["id"]: view for view in config["graph_views"]}
    view = graph_views["rasopathies_drug_has_adverse_event"]
    query = view["query"]

    assert view["output_format"] == "jsonl"
    assert view["query_language"] == "aql"
    assert "columns" not in view
    assert "FOR drug IN `Drug`" in query
    assert "INBOUND drug `DrugTreatmentDrugEdge`" in query
    assert "OUTBOUND drug_treatment `DrugTreatmentAdverseEventEdge`" in query
    assert "INBOUND drug_treatment `ClinicalContextDrugTreatmentEdge`" in query
    assert "INBOUND clinical_context `PatientClinicalContextEdge`" in query
    assert "INBOUND patient `CaseReportPatientEdge`" in query
    assert 'id: "has_adverse_event"' in query
    assert 'label: "has adverse event"' in query
    assert "patient_count" in query
    assert "case_report_count" in query
    assert "SORT patient_count DESC, case_report_count DESC" in query
    assert "outcomes" in query
    assert "source_label" in query
    assert "have_adverse_events" in query
    assert "evidence" in query
    assert "case_report_url" in query
    assert "biolink" not in query.lower()


def test_rasopathies_gene_has_sequence_variant_graph_view_shape():
    with open("./src/use_cases/cure/cure_rasopathies.yaml", "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    graph_views = {view["id"]: view for view in config["graph_views"]}
    view = graph_views["rasopathies_gene_has_sequence_variant"]
    query = view["query"]

    assert view["output_format"] == "jsonl"
    assert view["query_language"] == "aql"
    assert "columns" not in view
    assert "FOR gene IN `Gene`" in query
    assert "OUTBOUND gene `GeneGeneVariantEdge`" in query
    assert "INBOUND gene_variant `DiagnosisGeneVariantEdge`" in query
    assert "INBOUND diagnosis `ClinicalContextDiagnosisEdge`" in query
    assert "INBOUND clinical_context `PatientClinicalContextEdge`" in query
    assert "INBOUND patient `CaseReportPatientEdge`" in query
    assert 'id: "has_sequence_variant"' in query
    assert 'label: "has sequence variant"' in query
    assert "source_gene_symbol" in query
    assert "nucleotide_change" in query
    assert "protein_change" in query
    assert "variant_label" in query
    assert "diagnosis_methods" in query
    assert "patient_count" in query
    assert "case_report_count" in query
    assert "SORT patient_count DESC, case_report_count DESC" in query
    assert "biolink" not in query.lower()


def test_rasopathies_sequence_variant_genetically_associated_with_condition_graph_view_shape():
    with open("./src/use_cases/cure/cure_rasopathies.yaml", "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    graph_views = {view["id"]: view for view in config["graph_views"]}
    view = graph_views["rasopathies_sequence_variant_genetically_associated_with_condition"]
    query = view["query"]

    assert view["output_format"] == "jsonl"
    assert view["query_language"] == "aql"
    assert "columns" not in view
    assert "FOR gene_variant IN `GeneVariant`" in query
    assert "INBOUND gene_variant `DiagnosisGeneVariantEdge`" in query
    assert "OUTBOUND diagnosis `DiagnosisConditionEdge`" in query
    assert "INBOUND diagnosis `ClinicalContextDiagnosisEdge`" in query
    assert "INBOUND clinical_context `PatientClinicalContextEdge`" in query
    assert "INBOUND patient `CaseReportPatientEdge`" in query
    assert 'id: "genetically_associated_with"' in query
    assert 'label: "genetically associated with"' in query
    assert "source_gene_symbol" in query
    assert "nucleotide_change" in query
    assert "protein_change" in query
    assert "variant_label" in query
    assert "diagnosis_methods" in query
    assert "patient_count" in query
    assert "case_report_count" in query
    assert "SORT patient_count DESC, case_report_count DESC" in query
    assert "biolink" not in query.lower()
