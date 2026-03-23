import zipfile

from src.input_adapters.reactome.reactome_pathways import ReactomePathwayAdapter


def test_reactome_pathway_adapter_reads_id_from_second_gmt_column(tmp_path):
    gmt_zip_path = tmp_path / "ReactomePathways.gmt.zip"
    version_path = tmp_path / "reactome_version.tsv"

    with zipfile.ZipFile(gmt_zip_path, "w") as archive:
        archive.writestr(
            "ReactomePathways.gmt",
            "\n".join([
                "2-LTR circle formation\tR-HSA-164843\tBANF1\tHMGA1",
                "Mouse pathway\tR-MMU-12345\tGeneA",
            ])
        )

    version_path.write_text("version\tversion_date\n95\t2025-11-27\n", encoding="utf-8")

    adapter = ReactomePathwayAdapter(
        gmt_file_path=str(gmt_zip_path),
        version_file_path=str(version_path),
    )

    pathways = next(adapter.get_all())

    assert len(pathways) == 1
    assert pathways[0].id == "R-HSA-164843"
    assert pathways[0].name == "2-LTR circle formation"
    assert pathways[0].source_id == "R-HSA-164843"
