## PASC Conceptual Model

This is a conceptual ER diagram for the `pasc` form family. It is not a direct rendering of `schema.sql`; it reflects the domain concepts and edges discussed from the form structure and exported report data. For PASC specifically, the model is shown in the unconsolidated form with distinct acute COVID, Long COVID, and pregnancy episodes.

```mermaid
flowchart TD
    CR["**CaseReport**
id
form_type
status"]

    R["**Reporter**
role"]

    P["**Person**
sex
age_group
race
ethnicity"]

    CE_ACUTE["**Episode**
Condition: Acute COVID-19
episode_role: antecedent"]

    CE_LC["**Episode**
Condition: Long COVID
episode_role: current"]

    CE_PREG["**Episode**
Condition: Pregnancy
episode_role: contextual"]

    TS["**Treatment**
treatment_class"]

    AE["**AdverseEvent**
name"]

    V["**VaccinationEvent**
vaccine_name
doses
relative_timing"]

    CR -->|submitted_by| R
    CR -->|describes| P
    P -->|has_episode| CE_ACUTE
    P -->|has_episode| CE_LC
    P -->|has_episode| CE_PREG

    subgraph AcuteCOVIDBranch["Acute COVID Branch"]
        PH_ACUTE["**Phenotype**
name
severity
duration"]
        CE_ACUTE -->|has_phenotype| PH_ACUTE
        DX_ACUTE["**Exposure**
route
frequency
dose
ongoing"]
        D_ACUTE["**Drug**
name"]
        CE_ACUTE -->|has_exposure| DX_ACUTE
        DX_ACUTE -->|drug| D_ACUTE
    end

    subgraph LongCOVIDBranch["Long COVID Branch"]
        PH_LC["**Phenotype**
name
severity
duration"]
        CE_LC -->|has_phenotype| PH_LC
        TA["**Outcome**
effect_label
effect_direction
time_to_effect"]
        PH_LC -->|assessed_by| TA
        TS -->|evaluated_by| TA
        DX_LC["**Exposure**
route
frequency
dose
ongoing"]
        D_LC["**Drug**
name"]
        CE_LC -->|has_exposure| DX_LC
        DX_LC -->|is_part_of| TS
        DX_LC -->|drug| D_LC
        DX_LC -->|caused| AE
    end

    subgraph PregnancyBranch["Pregnancy Branch"]
        PH_PREG["**Phenotype**
name
severity
duration"]
        CE_PREG -->|has_phenotype| PH_PREG
        DX_PREG["**Exposure**
route
frequency
dose
ongoing"]
        D_PREG["**Drug**
name"]
        CE_PREG -->|has_exposure| DX_PREG
        DX_PREG -->|drug| D_PREG
    end
    CE_LC -->|develops_after| CE_ACUTE
    CE_PREG -->|overlaps| CE_LC
    V -->|before| CE_ACUTE

    classDef core fill:#fee2e2,stroke:#dc2626,stroke-width:3px,color:#7f1d1d;

    class PH_LC,TS,TA core;
```