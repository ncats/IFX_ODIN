from src.input_adapters.pharos_source_tcrd.word_count import WordCountAdapter
from src.models.word_count import WordCount
from src.output_adapters.sql_converters.tcrd import TCRDOutputConverter


def test_words_in_abstract_uses_legacy_document_frequency_tokenizer():
    words = WordCountAdapter.words_in_abstract(
        "TP53 TP53 p53 IL-2 alpha/beta 3abc4 A x C."
    )

    assert words == {"tp53", "p53", "il-2", "alpha/beta", "3abc4"}


def test_word_count_converter_emits_mysql_word_count_row():
    converter = TCRDOutputConverter()

    row = converter.word_count_converter(WordCount(id="tp53", word="tp53", count=17).__dict__)

    assert row.word == "tp53"
    assert row.count == 17
