from src.output_adapters.sql_converters.tcrd import TCRDOutputConverter
from src.shared.sqlalchemy_tables.pharos_tables_new import DOParent


class _FakeQuery:
    def __init__(self, rows):
        self._rows = rows

    def join(self, *_args, **_kwargs):
        return self

    def all(self):
        return self._rows


class _FakeSession:
    def query(self, *columns):
        key = tuple(getattr(column, "name", str(column)) for column in columns)
        if key == ("id", "ifx_id"):
            return _FakeQuery([
                (123, "IFX123"),
            ])
        if key == ("id", "uniprot"):
            return _FakeQuery([
                (123, "Q9Y345"),
            ])
        if key == ("id", "identifier"):
            return _FakeQuery([
                (77, "LIGAND:EXISTING"),
            ])
        if key == ("name",):
            return _FakeQuery([])
        if key == ("mondoid",):
            return _FakeQuery([])
        raise AssertionError(f"Unexpected query columns: {key}")


def test_tcrd_output_converter_preloads_protein_ids():
    converter = TCRDOutputConverter()

    converter.preload_id_mappings(_FakeSession())

    assert converter.id_mapping["protein"] == {"IFX123": 123}
    assert converter.id_mapping["target_by_uniprot"] == {"Q9Y345": 123}
    assert converter.id_mapping["ligand"] == {"LIGAND:EXISTING": 77}


def test_drgc_resource_converter_uses_target_mapping_by_uniprot():
    converter = TCRDOutputConverter()
    converter._target_id_by_uniprot = {"Q9Y345": 123}

    row = converter.drgc_resource_converter({
        "id": "https://repo.metadatacenter.org/template-instances/example",
        "uniprot_id": "Q9Y345",
        "rssid": "https://repo.metadatacenter.org/template-instances/example",
        "resource_type": "Genetic Construct",
        "json": "{\"resource\": []}",
        "legacy_target_id": 999,
        "provenance": "Pharos 3.19\t3.19\t2024-02-15\tNone",
    })

    assert row.target_id == 123
    assert row.rssid == "https://repo.metadatacenter.org/template-instances/example"
    assert row.resource_type == "Genetic Construct"


def test_drgc_resource_converter_skips_missing_destination_target():
    converter = TCRDOutputConverter()

    row = converter.drgc_resource_converter({
        "id": "rssid",
        "uniprot_id": "Q5JXX5",
        "rssid": "rssid",
        "resource_type": "Mouse",
        "json": "{}",
        "legacy_target_id": 1234,
        "provenance": "Pharos 3.19\t3.19\t2024-02-15\tNone",
    })

    assert row is None


def test_protein_converter_seeds_drgc_target_mapping_for_same_run():
    converter = TCRDOutputConverter()
    converter.id_mapping["protein"] = {"IFX123": 123}

    converter.protein_converter({
        "id": "IFX123",
        "xref": [],
        "gene_name": "SC6A5_HUMAN",
        "name": "Sodium- and chloride-dependent glycine transporter 2",
        "uniprot_id": "Q9Y345",
        "symbol": "SLC6A5",
        "sequence": "MSEQ",
        "dtoid": None,
        "dtoclass": None,
        "preferred_symbol": "SLC6A5",
        "provenance": "test",
    })

    row = converter.drgc_resource_converter({
        "id": "https://repo.metadatacenter.org/template-instances/example",
        "uniprot_id": "Q9Y345",
        "rssid": "https://repo.metadatacenter.org/template-instances/example",
        "resource_type": "Genetic Construct",
        "json": "{\"resource\": []}",
        "legacy_target_id": 999,
        "provenance": "Pharos 3.19\t3.19\t2024-02-15\tNone",
    })

    assert row.target_id == 123


def test_ligand_converter_assigns_integer_id_from_mapping():
    converter = TCRDOutputConverter()
    converter.id_mapping["ligand"] = {"LIGAND:EXISTING": 77}

    row = converter.ligand_converter({
        "id": "LIGAND:EXISTING",
        "name": "Existing ligand",
        "xref": [],
        "provenance": "test",
    })

    assert row.id == 77
    assert row.identifier == "LIGAND:EXISTING"


def test_protein_converter_uses_short_graph_name_for_legacy_description():
    converter = TCRDOutputConverter()
    converter.id_mapping["protein"] = {"IFX123": 123}

    row = converter.protein_converter({
        "id": "IFX123",
        "xref": ["NCBIGene:1234", "ENSEMBL:ENSP00000123456"],
        "gene_name": "SC6A5_HUMAN",
        "name": "Sodium- and chloride-dependent glycine transporter 2",
        "description": "Long UniProt free text that should not land in legacy protein.description",
        "uniprot_id": "Q9Y345",
        "symbol": "SLC6A5",
        "sequence": "MSEQ",
        "dtoid": None,
        "dtoclass": None,
        "preferred_symbol": "SLC6A5",
        "provenance": "test",
    })

    assert row.name == "SC6A5_HUMAN"
    assert row.description == "Sodium- and chloride-dependent glycine transporter 2"


def test_tdl_info_converter_exports_uniprot_function_as_legacy_uniprot_function():
    converter = TCRDOutputConverter()
    converter.id_mapping["protein"] = {"IFX123": 123}

    rows = converter.tdl_info_converter({
        "id": "IFX123",
        "uniprot_function": "Long UniProt-derived descriptive text",
        "description": "Graph description fallback",
        "provenance": "test",
    })

    by_type = {row.itype: row for row in rows}
    assert by_type["UniProt Function"].protein_id == 123
    assert by_type["UniProt Function"].string_value == "Long UniProt-derived descriptive text"


def test_ligand_edge_converter_uses_same_integer_ligand_id():
    converter = TCRDOutputConverter()
    converter.id_mapping["protein"] = {"IFX123": 123}
    converter.id_mapping["ligand"] = {"LIGAND:EXISTING": 77}

    rows = converter.ligand_edge_converter({
        "start_id": "IFX123",
        "end_id": "LIGAND:EXISTING",
        "details": [{
            "act_value": 1.5,
            "act_type": "IC50",
            "action_type": "inhibitor",
            "reference": "ref",
            "act_pmids": ["12345"],
        }],
        "provenance": "test",
    })

    assert len(rows) == 1
    assert rows[0].ncats_ligand_id == 77
    assert rows[0].target_id == 123


def test_pathway_converter_keeps_pwtype_without_lookup_table():
    converter = TCRDOutputConverter()
    converter.id_mapping["protein"] = {"IFX123": 123}

    row = converter.pathway_converter({
        "start_id": "IFX123",
        "end_node": {
            "type": "Reactome",
            "source_id": "R-HSA-199420",
            "name": "Generic Transcription Pathway",
            "url": "https://reactome.org/content/detail/R-HSA-199420",
        },
        "provenance": "Reactome\t95\t2025-11-27\t2026-03-23",
    })

    assert row.target_id == 123
    assert row.protein_id == 123
    assert row.pwtype == "Reactome"
    assert row.id_in_source == "R-HSA-199420"


def test_ppi_converter_emits_reciprocal_rows_with_max_score_and_legacy_stringdb_label():
    converter = TCRDOutputConverter()
    converter.id_mapping["protein"] = {
        "IFX123": 123,
        "IFX456": 456,
    }

    rows = converter.ppi_converter({
        "start_id": "IFX456",
        "end_id": "IFX123",
        "sources": ["STRING\t12.0\t2023-05-16\t2026-04-16"],
        "score": [475, 477],
        "provenance": "STRING\t12.0\t2023-05-16\t2026-04-16",
    })

    assert len(rows) == 2
    assert {(row.protein_id, row.other_id) for row in rows} == {(123, 456), (456, 123)}
    assert all(row.ppitypes == "StringDB" for row in rows)
    assert all(row.score == 477 for row in rows)


def test_ppi_converter_joins_multiple_source_labels():
    converter = TCRDOutputConverter()
    converter.id_mapping["protein"] = {
        "IFX123": 123,
        "IFX456": 456,
    }

    rows = converter.ppi_converter({
        "start_id": "IFX123",
        "end_id": "IFX456",
        "sources": [
            "Reactome\t95\t2025-11-27\t2026-03-23",
            "STRING\t12.0\t2023-05-16\t2026-04-16",
        ],
        "score": [800],
        "provenance": "STRING\t12.0\t2023-05-16\t2026-04-16",
    })

    assert len(rows) == 2
    assert all(row.ppitypes == "Reactome,StringDB" for row in rows)
    assert all(row.score == 800 for row in rows)


def test_ppi_converter_maps_reactome_pmids_and_interaction_type_downstream():
    converter = TCRDOutputConverter()
    converter.id_mapping["protein"] = {
        "IFX123": 123,
        "IFX456": 456,
    }

    rows = converter.ppi_converter({
        "start_id": "IFX123",
        "end_id": "IFX456",
        "sources": ["Reactome\t96\t2026-03-24\t2026-04-24"],
        "pmids": [24243840, 11163199],
        "contexts": ["reactome:R-HSA-2428940"],
        "interaction_type": ["physical association"],
        "provenance": "Reactome\t96\t2026-03-24\t2026-04-24",
    })

    assert len(rows) == 2
    assert all(row.evidence == "24243840|11163199" for row in rows)
    assert all(row.interaction_type == "physical association" for row in rows)


def test_gtex_converter_branches_gtex_details_from_shared_expression_edge():
    converter = TCRDOutputConverter()
    converter.id_mapping["protein"] = {"IFX123": 123}

    rows = converter.gtex_converter({
        "start_id": "IFX123",
        "details": [
            {
                "source": "GTEx",
                "tissue": "Liver",
                "source_tissue_id": "UBERON:0002107",
                "number_value": 10.0,
                "source_rank": 0.7,
            },
            {
                "source": "GTEx",
                "tissue": "Liver",
                "source_tissue_id": "UBERON:0002107",
                "sex": "male",
                "number_value": 11.0,
                "source_rank": 0.8,
            },
            {
                "source": "GTEx",
                "tissue": "Liver",
                "source_tissue_id": "UBERON:0002107",
                "sex": "female",
                "number_value": 9.0,
                "source_rank": 0.6,
            },
            {
                "source": "HPM Protein",
                "tissue": "Liver",
                "source_tissue_id": "UBERON:0002107",
                "number_value": 1.0,
            },
        ],
        "provenance": "GTEx\tv1\t2026-01-01\t2026-01-02",
    })

    assert len(rows) == 1
    row = rows[0]
    assert row.protein_id == 123
    assert row.tissue == "Liver"
    assert row.uberon_id == "UBERON:0002107"
    assert row.tpm == 10.0
    assert row.tpm_male == 11.0
    assert row.tpm_female == 9.0


def test_mondo_xref_converter_marks_exact_matches():
    converter = TCRDOutputConverter()

    rows = converter.mondo_xref_converter({
        "id": "MONDO:0000001",
        "mondo_xrefs": ["DOID:4", "ICD9:799.9"],
        "exact_matches": ["DOID:4"],
        "provenance": "Mondo\tv2026-03-03\t2026-03-03\t2026-03-10",
    })

    assert len(rows) == 2
    by_xref = {row.xref: row for row in rows}
    assert by_xref["DOID:4"].db == "DOID"
    assert by_xref["DOID:4"].value == "4"
    assert by_xref["DOID:4"].equiv_to is True
    assert by_xref["ICD9:799.9"].equiv_to is False


def test_keyword_xref_converter_writes_keyword_id_to_value_and_label_to_xtra():
    converter = TCRDOutputConverter()
    converter.id_mapping["protein"] = {"IFX123": 123}

    row = converter.keyword_xref_converter({
        "start_id": "IFX123",
        "end_node": {
            "id": "keyword:uniprot:technical term:reference proteome",
            "source_id": "KW-1185",
            "value": "Reference proteome",
            "category": "Technical term",
        },
        "provenance": "UniProt\t2026_01\t2026-01-28\t2026-03-16",
    })

    assert row.protein_id == 123
    assert row.xtype == "UniProt Keyword"
    assert row.value == "KW-1185"
    assert row.xtra == "Reference proteome"


def test_do_parent_uses_composite_primary_key():
    pk_columns = tuple(column.name for column in DOParent.__table__.primary_key.columns)

    assert pk_columns == ("doid", "parent_id")


def test_p2p_converter_dedupes_across_calls():
    converter = TCRDOutputConverter()
    converter.id_mapping["protein"] = {"IFX123": 123}

    obj = {
        "start_id": "IFX123",
        "gene_id": 6857,
        "pmids": ["20222955", "20222955"],
        "provenance": "Pharos 4.0 CSV\t1.0\tNone\t2024-10-03",
    }

    first_rows = converter.p2p_converter(obj)
    second_rows = converter.p2p_converter(obj)

    assert len(first_rows) == 1
    assert len(second_rows) == 0
    assert first_rows[0].protein_id == 123
    assert str(first_rows[0].pubmed_id) == "20222955"
    assert first_rows[0].gene_id == 6857
    assert first_rows[0].source == "NCBI"


def test_ncats_disease_converter_only_keeps_loaded_mondo_ids():
    converter = TCRDOutputConverter()

    missing = converter.ncats_disease_converter({
        "id": "MONDO:0957221",
        "name": "Spastic paraplegia 70, autosomal recessive",
        "provenance": "Mondo\tv2026-03-03\t2026-03-03\t2026-03-10",
    })
    assert missing.mondoid is None

    converter._known_mondo_ids.add("MONDO:0000001")
    known = converter.ncats_disease_converter({
        "id": "MONDO:0000001",
        "name": "disease",
        "provenance": "Mondo\tv2026-03-03\t2026-03-03\t2026-03-10",
    })
    assert known.mondoid == "MONDO:0000001"


def test_disease_converter_prefers_detail_source_id_and_keeps_resolved_mondoid():
    converter = TCRDOutputConverter()
    converter.id_mapping["protein"] = {"IFX123": 123}
    converter.id_mapping["ncats_disease"] = {"MONDO:0000001": 456}
    converter._known_mondo_ids.add("MONDO:0000001")

    rows = converter.disease_converter({
        "start_id": "IFX123",
        "end_id": "MONDO:0000001",
        "end_node": {"name": "Example disease"},
        "details": [{
            "source": "JensenLab Text Mining",
            "source_id": "DOID:1234",
            "confidence": 3.2,
            "zscore": 6.5,
            "url": "https://example.org/jensen",
        }],
        "provenance": "JensenLab DISEASES\tNone\t2026-03-18\t2026-04-02",
    })

    assert len(rows) == 1
    row = rows[0]
    assert row.protein_id == 123
    assert row.dtype == "JensenLab Text Mining"
    assert row.did == "DOID:1234"
    assert row.mondoid == "MONDO:0000001"
    assert row.conf == 3.2
    assert row.zscore == 6.5
    assert row.reference == "https://example.org/jensen"


def test_ncats_d2da_converter_keys_links_off_resolved_disease_id():
    converter = TCRDOutputConverter()
    converter.id_mapping["protein"] = {"IFX123": 123}
    converter.id_mapping["ncats_disease"] = {"MONDO:0000001": 456}
    converter._known_mondo_ids.add("MONDO:0000001")

    obj = {
        "start_id": "IFX123",
        "end_id": "MONDO:0000001",
        "end_node": {"name": "Example disease"},
        "details": [{
            "source": "JensenLab Text Mining",
            "source_id": "DOID:1234",
            "confidence": 3.2,
            "zscore": 6.5,
            "url": "https://example.org/jensen",
        }],
        "provenance": "JensenLab DISEASES\tNone\t2026-03-18\t2026-04-02",
    }

    disease_rows = converter.disease_converter(obj)
    link_rows = converter.ncats_d2da_converter(obj)

    assert len(disease_rows) == 1
    assert len(link_rows) == 1
    assert link_rows[0].ncats_disease_id == 456
    assert link_rows[0].disease_assoc_id == disease_rows[0].id
