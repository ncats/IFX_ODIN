from src.input_adapters.reactome.reactome_ppi import ReactomePPIAdapter


def test_reactome_ppi_adapter_filters_non_protein_rows_and_keeps_context_and_pmids(tmp_path):
    data_path = tmp_path / "reactome.homo_sapiens.interactions.tab-delimited.txt"
    version_path = tmp_path / "reactome_version.tsv"

    data_path.write_text(
        "\n".join(
            [
                "# Interactor 1 uniprot id\tInteractor 1 Ensembl gene id\tInteractor 1 Entrez Gene id\tInteractor 2 uniprot id\tInteractor 2 Ensembl gene id\tInteractor 2 Entrez Gene id\tInteraction type\tInteraction context\tPubmed references",
                "uniprotkb:P08123\t-\t-\tuniprotkb:P02452\t-\t-\tphysical association\treactome:R-HSA-2428940\t24243840",
                "ChEBI:29035\t-\t-\tuniprotkb:P02452\t-\t-\tphysical association\treactome:R-HSA-123\t111",
            ]
        ),
        encoding="utf-8",
    )
    version_path.write_text("version\tversion_date\n96\t2026-03-24\n", encoding="utf-8")

    adapter = ReactomePPIAdapter(file_path=str(data_path), version_file_path=str(version_path))
    edges = [edge for batch in adapter.get_all() for edge in batch]

    assert len(edges) == 1
    assert edges[0].start_node.id == "UniProtKB:P02452"
    assert edges[0].end_node.id == "UniProtKB:P08123"
    assert edges[0].interaction_type == ["physical association"]
    assert edges[0].contexts == ["reactome:R-HSA-2428940"]
    assert edges[0].pmids == [24243840]
    assert edges[0].sources == []


def test_reactome_ppi_adapter_skips_self_pairs_and_honors_max_rows(tmp_path):
    data_path = tmp_path / "reactome.homo_sapiens.interactions.tab-delimited.txt"

    data_path.write_text(
        "\n".join(
            [
                "# Interactor 1 uniprot id\tInteractor 1 Ensembl gene id\tInteractor 1 Entrez Gene id\tInteractor 2 uniprot id\tInteractor 2 Ensembl gene id\tInteractor 2 Entrez Gene id\tInteraction type\tInteraction context\tPubmed references",
                "uniprotkb:P08123\t-\t-\tuniprotkb:P08123\t-\t-\tphysical association\treactome:R-HSA-1\t1",
                "uniprotkb:P08123\t-\t-\tuniprotkb:P02452\t-\t-\tphysical association\treactome:R-HSA-2\t2|3",
                "uniprotkb:P01160\t-\t-\tuniprotkb:P06727\t-\t-\tenzymatic reaction\treactome:R-HSA-3\t4",
            ]
        ),
        encoding="utf-8",
    )

    adapter = ReactomePPIAdapter(file_path=str(data_path), max_rows=1)
    edges = [edge for batch in adapter.get_all() for edge in batch]

    assert len(edges) == 1
    assert edges[0].pmids == [2, 3]
    assert edges[0].contexts == ["reactome:R-HSA-2"]


def test_reactome_ppi_adapter_dedupes_pair_plus_type_and_accumulates_contexts_and_pmids(tmp_path):
    data_path = tmp_path / "reactome.homo_sapiens.interactions.tab-delimited.txt"

    data_path.write_text(
        "\n".join(
            [
                "# Interactor 1 uniprot id\tInteractor 1 Ensembl gene id\tInteractor 1 Entrez Gene id\tInteractor 2 uniprot id\tInteractor 2 Ensembl gene id\tInteractor 2 Entrez Gene id\tInteraction type\tInteraction context\tPubmed references",
                "uniprotkb:P08123\t-\t-\tuniprotkb:P02452\t-\t-\tphysical association\treactome:R-HSA-1\t1|2",
                "uniprotkb:P02452\t-\t-\tuniprotkb:P08123\t-\t-\tphysical association\treactome:R-HSA-2\t2|3",
            ]
        ),
        encoding="utf-8",
    )

    adapter = ReactomePPIAdapter(file_path=str(data_path))
    edges = [edge for batch in adapter.get_all() for edge in batch]

    assert len(edges) == 1
    assert edges[0].contexts == ["reactome:R-HSA-1", "reactome:R-HSA-2"]
    assert edges[0].pmids == [1, 2, 3]
