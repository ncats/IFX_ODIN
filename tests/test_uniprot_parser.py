from src.shared.uniprot_parser import UniProtParser


def test_get_full_name_prefers_recommended_name():
    row = {
        "proteinDescription": {
            "recommendedName": {
                "fullName": {"value": "Recommended Name"}
            },
            "submissionNames": [
                {"fullName": {"value": "Submission Name"}}
            ],
        }
    }

    assert UniProtParser.get_full_name(row) == "Recommended Name"


def test_get_full_name_falls_back_to_submission_name():
    row = {
        "proteinDescription": {
            "submissionNames": [
                {"fullName": {"value": "Submission Name"}}
            ]
        }
    }

    assert UniProtParser.get_full_name(row) == "Submission Name"


def test_get_full_name_falls_back_to_alternative_name():
    row = {
        "proteinDescription": {
            "alternativeNames": [
                {"fullName": {"value": "Alternative Name"}}
            ]
        }
    }

    assert UniProtParser.get_full_name(row) == "Alternative Name"


def test_get_full_name_returns_none_when_no_name_fields_present():
    row = {"proteinDescription": {}}

    assert UniProtParser.get_full_name(row) is None


def test_parse_aliases_handles_missing_recommended_name():
    row = {
        "primaryAccession": "P12345",
        "uniProtkbId": "ABC_HUMAN",
        "proteinDescription": {
            "submissionNames": [
                {"fullName": {"value": "Submission Name"}}
            ]
        },
    }

    aliases = UniProtParser.parse_aliases(row)
    alias_pairs = {(a.type, a.term) for a in aliases}

    assert ("full name", "Submission Name") in alias_pairs
