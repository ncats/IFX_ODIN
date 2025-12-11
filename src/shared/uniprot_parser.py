from dataclasses import dataclass

from src.constants import Prefix


@dataclass
class Alias:
    type: str
    term: str


class UniProtParser:
    @staticmethod
    def find_matches(uniprot_obj, list_field, data_field, match_string):
        if list_field in uniprot_obj:
            return filter(lambda match: match[data_field] == match_string, uniprot_obj[list_field])
        return iter([])

    @staticmethod
    def find_comments(uniprot_obj, comment_type):
        return UniProtParser.find_matches(uniprot_obj, 'comments', 'commentType', comment_type)

    @staticmethod
    def find_first_comment(uniprot_obj, comment_type):
        first = next(UniProtParser.find_comments(uniprot_obj, comment_type), None)
        if first is not None and len(first) > 0:
            return first['texts'][0]['value']
        return None

    @staticmethod
    def find_cross_refs(uniprot_obj, ref_type):
        return UniProtParser.find_matches(uniprot_obj, 'uniProtKBCrossReferences', 'database', ref_type)

    @staticmethod
    def get_uniprot_id(uniprot_obj):
        return uniprot_obj['primaryAccession']

    @staticmethod
    def get_isoforms(uniprot_obj):
        ap_comments = UniProtParser.find_comments(uniprot_obj, 'ALTERNATIVE PRODUCTS')
        isoforms = []
        for alternative_product in ap_comments:
            for isoform in alternative_product.get('isoforms', []):
                if isoform['isoformSequenceStatus'] != 'Described':
                    continue
                isoforms.append({
                    'id': isoform['isoformIds'][0],
                    'name': isoform['name']['value'],
                })
        if len(isoforms) > 0:
            return isoforms
        return None

    @staticmethod
    def get_primary_accession(uniprot_obj):
        return f"{Prefix.UniProtKB}:" + UniProtParser.get_uniprot_id(uniprot_obj)

    @staticmethod
    def get_gene_name(uniprot_obj):
        return uniprot_obj['uniProtkbId']

    @staticmethod
    def get_full_name(uniprot_obj):
        return uniprot_obj['proteinDescription']['recommendedName']['fullName']['value']

    @staticmethod
    def get_description(uniprot_obj):
        return UniProtParser.find_first_comment(uniprot_obj, "FUNCTION")

    @staticmethod
    def get_symbols(uniprot_obj):
        if 'genes' not in uniprot_obj:
            return None
        symbols = []
        for g in uniprot_obj['genes']:
            if 'geneName' in g:
                symbols.append(g['geneName']['value'])
        if len(symbols) == 0:
            return None
        return symbols

    @staticmethod
    def get_sequence(uniprot_obj):
        return uniprot_obj['sequence']['value']

    @staticmethod
    def get_secondary_accessions(uniprot_obj):
        if 'secondaryAccessions' in uniprot_obj:
            return uniprot_obj['secondaryAccessions']
        return None

    @staticmethod
    def parse_aliases(uniprot_obj):
        aliases = []
        UniProtParser.append_to_list(aliases, Alias(type='primary accession', term=UniProtParser.get_primary_accession(uniprot_obj)))
        if 'secondaryAccessions' in uniprot_obj:
            for id_val in uniprot_obj['secondaryAccessions']:
                UniProtParser.append_to_list(aliases, Alias('secondary accession', f"{Prefix.UniProtKB}:" + id_val))
        UniProtParser.append_to_list(aliases, Alias('uniprot kb', uniprot_obj['uniProtkbId']))
        UniProtParser.append_to_list(aliases,
                                     Alias('full name',
                                            uniprot_obj['proteinDescription']['recommendedName']['fullName']['value']))
        if 'shortNames' in uniprot_obj['proteinDescription']['recommendedName']:
            for obj in uniprot_obj['proteinDescription']['recommendedName']['shortNames']:
                UniProtParser.append_to_list(aliases, Alias('short name', obj['value']))
        if 'genes' in uniprot_obj and len(uniprot_obj['genes']) > 0:
            for gene in uniprot_obj['genes']:
                if 'geneName' in gene:
                    UniProtParser.append_to_list(aliases, Alias('symbol', gene['geneName']['value']))
                if 'synonyms' in gene and len(gene['synonyms']) > 0:
                    for synonym in gene['synonyms']:
                        UniProtParser.append_to_list(aliases, Alias('synonym', synonym['value']))
        ensembl_objs = UniProtParser.find_matches(uniprot_obj, 'uniProtKBCrossReferences', 'database', 'Ensembl')
        for match in ensembl_objs:
            UniProtParser.append_to_list(aliases, Alias('Ensembl', Prefix.ENSEMBL + ":" + UniProtParser.trim_version(match['id'])))
            if 'properties' in match:
                for prop in match['properties']:
                    UniProtParser.append_to_list(aliases, Alias('Ensembl', Prefix.ENSEMBL + ":" + UniProtParser.trim_version(prop['value'])))
        string_objs = UniProtParser.find_matches(uniprot_obj, 'uniProtKBCrossReferences', 'database', 'STRING')
        for match in string_objs:
            UniProtParser.append_to_list(aliases, Alias('STRING', UniProtParser.trim_species(match['id'])))
        refseq_objs = UniProtParser.find_matches(uniprot_obj, Prefix.ENSEMBL + ":" + 'uniProtKBCrossReferences', 'database', 'RefSeq')
        for match in refseq_objs:
            UniProtParser.append_to_list(aliases, Alias('RefSeq', Prefix.RefSeq + ":" + UniProtParser.trim_version(match['id'])))
        return aliases

    @staticmethod
    def append_to_list(aliases, new_alias):
        found = list(filter(lambda each: new_alias.type == each.type and new_alias.term == each.term, aliases))
        if len(found) == 0:
            aliases.append(new_alias)

    @staticmethod
    def trim_version(ensembl_id):
        return ensembl_id.split('.')[0]

    @staticmethod
    def trim_species(string_id):
        return string_id.split('.')[1]

    @staticmethod
    def parse_go_term(go_term_json):
        go_id = go_term_json['id']
        term = next(UniProtParser.find_matches(go_term_json, 'properties', 'key', 'GoTerm'))
        evidence = next(UniProtParser.find_matches(go_term_json, 'properties', 'key', 'GoEvidenceType'))
        pieces = term['value'].split(':')
        go_type = pieces[0]
        go_term = pieces[1]
        pieces = evidence['value'].split(':')
        eco_term = pieces[0]
        eco_assigned_by = pieces[1]

        return go_id, go_type, go_term, eco_term, eco_assigned_by
