import gzip
from datetime import date
from dataclasses import fields

from src.interfaces.id_resolver import IdMatch, IdResolver
from src.input_adapters.ncbi.gene_summary import NCBIGeneSummaryAdapter
from src.models.gene import Gene
from src.models.protein import Protein


def _write_gene_summary(path):
    with gzip.open(path, "wt", encoding="utf-8", newline="") as handle:
        handle.write("#tax_id\tGeneID\tSource\tSummary\n")
        handle.write("10090\t1\tRefSeq\tMouse summary should be skipped.\n")
        handle.write("9606\t1\tRefSeq\tHuman summary one.\n")
        handle.write("9606\t2\tOMIM\tHuman summary two.\n")
        handle.write("9606\t3\tRefSeq\t\n")


def test_ncbi_gene_summary_adapter_emits_human_gene_summaries(tmp_path):
    summary_path = tmp_path / "gene_summary.gz"
    version_path = tmp_path / "version.tsv"
    _write_gene_summary(summary_path)
    version_path.write_text(
        "version\tversion_date\tdownload_date\n"
        "\t2026-05-19\t2026-05-20\n",
        encoding="utf-8",
    )

    adapter = NCBIGeneSummaryAdapter(
        gene_summary_file_path=str(summary_path),
        version_file_path=str(version_path),
    )

    genes = [gene for batch in adapter.get_all() for gene in batch]

    assert [gene.id for gene in genes] == ["NCBIGene:1", "NCBIGene:2"]
    assert [gene.ncbi_gene_summary for gene in genes] == ["Human summary one.", "Human summary two."]
    assert all(isinstance(gene, Gene) for gene in genes)


def test_ncbi_gene_summary_adapter_reads_version_metadata(tmp_path):
    summary_path = tmp_path / "gene_summary.gz"
    version_path = tmp_path / "version.tsv"
    _write_gene_summary(summary_path)
    version_path.write_text(
        "version\tversion_date\tdownload_date\n"
        "latest\t2026-05-19\t2026-05-20\n",
        encoding="utf-8",
    )

    version = NCBIGeneSummaryAdapter(
        gene_summary_file_path=str(summary_path),
        version_file_path=str(version_path),
    ).get_version()

    assert version.version == "latest"
    assert version.version_date == date(2026, 5, 19)
    assert version.download_date == date(2026, 5, 20)


def test_ncbi_gene_summary_field_is_available_on_gene_and_protein():
    gene_fields = {field.name for field in fields(Gene)}
    protein_fields = {field.name for field in fields(Protein)}

    assert "ncbi_gene_summary" in gene_fields
    assert "ncbi_gene_summary" in protein_fields


def test_ncbi_gene_summary_gene_payload_can_retype_to_protein():
    resolver = _GeneToProteinResolver(types=["Gene"], canonical_class=Protein)
    gene = Gene(id="NCBIGene:1", ncbi_gene_summary="Human summary one.")

    entity_map = resolver.resolve_nodes([gene], allow_retype=True)
    proteins = resolver.parse_flat_node_list_from_map(entity_map)

    assert len(proteins) == 1
    assert isinstance(proteins[0], Protein)
    assert proteins[0].id == "IFXProtein:ABC"
    assert proteins[0].ncbi_gene_summary == "Human summary one."


class _GeneToProteinResolver(IdResolver):
    def resolve_internal(self, input_nodes):
        return {
            node.id: [IdMatch(input=node.id, match="IFXProtein:ABC", equivalent_ids=[node.id])]
            for node in input_nodes
        }
