from src.output_adapters.mysql_output_adapter import MySQLOutputAdapter
from src.shared.sqlalchemy_tables.pharos_tables_new import TDL_info


def test_serialize_rows_fills_missing_optional_columns_with_none():
    rows = MySQLOutputAdapter._serialize_rows([
        TDL_info(itype="Ab Count", protein_id=1, integer_value=75),
        TDL_info(itype="JensenLab PubMed Score", protein_id=1, number_value=25.269722),
    ])

    assert rows == [
        {
            "itype": "Ab Count",
            "protein_id": 1,
            "integer_value": 75,
            "number_value": None,
        },
        {
            "itype": "JensenLab PubMed Score",
            "protein_id": 1,
            "integer_value": None,
            "number_value": 25.269722,
        },
    ]
