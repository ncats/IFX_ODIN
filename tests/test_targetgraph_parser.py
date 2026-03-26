from src.shared.targetgraph_parser import TargetGraphGeneParser, TargetGraphTranscriptParser


def test_targetgraph_gene_parser_reads_tsv(tmp_path):
    file_path = tmp_path / "gene_ids.tsv"
    file_path.write_text(
        "\t".join(
            [
                "ncats_gene_id",
                "createdAt",
                "updatedAt",
                "consolidated_symbol",
                "consolidated_description",
            ]
        )
        + "\n"
        + "\t".join(
            [
                "IFXGene:ABC1234",
                "2025-07-08T12:25:29.336300",
                "2026-03-15T09:53:28.798010",
                "MT-RNR1",
                "16S ribosomal RNA",
            ]
        )
        + "\n"
    )

    rows = list(TargetGraphGeneParser(file_path=str(file_path)).all_rows())

    assert rows == [
        {
            "ncats_gene_id": "IFXGene:ABC1234",
            "createdAt": "2025-07-08T12:25:29.336300",
            "updatedAt": "2026-03-15T09:53:28.798010",
            "consolidated_symbol": "MT-RNR1",
            "consolidated_description": "16S ribosomal RNA",
        }
    ]


def test_targetgraph_transcript_parser_handles_missing_optional_fields(tmp_path):
    row = {
        "ncats_transcript_id": "IFXTranscript:XYZ9876",
        "ensembl_transcript_id": "ENST00000387314",
        "ensembl_transcript_id_version": "ENST00000387314.1",
        "ensembl_gene_id": "ENSG00000210049",
        "ensembl_transcript_type": "Mt_tRNA",
        "ensembl_trans_length": "71",
        "ensembl_transcript_tsl": "tslNA",
        "ensembl_canonical": "1.0",
        "ensembl_refseq_MANEselect": "",
        "refseq_rna_id": "",
        "refseq_ncbi_id": "",
        "refseq_status": "",
        "createdAt": "2025-07-08T12:49:11.462992",
        "updatedAt": "2025-07-08T12:49:11.462992",
    }

    file_path = tmp_path / "transcript_ids.tsv"
    file_path.write_text("ncats_transcript_id\nIFXTranscript:XYZ9876\n")

    parser = TargetGraphTranscriptParser(file_path=str(file_path))

    assert parser.get_transcript_location(row) is None
    equivalent_ids = parser.get_equivalent_ids(row)

    assert [eq.id_str() for eq in equivalent_ids] == ["ENSEMBL:ENST00000387314"]


def test_targetgraph_gene_location_ignores_ambiguous_multi_strand():
    row = {
        "consolidated_location": "1|16|17",
        "ensembl_strand": "-1.0|1.0",
    }

    location = TargetGraphGeneParser.get_gene_location(row)

    assert location is not None
    assert location.location == "1|16|17"
    assert location.chromosome == 1
    assert location.strand is None


def test_targetgraph_mapping_ratio_ignores_ambiguous_multi_value():
    row = {"Total_Mapping_Ratio": "0.0833333333333333|0.25"}

    assert TargetGraphGeneParser.get_mapping_ratio(row) is None
