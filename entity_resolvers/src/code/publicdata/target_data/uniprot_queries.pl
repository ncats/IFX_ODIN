#107_uniprot_sequences_and_mark_which_is_cannonical_for_human: List all human UniProtKB entries and their sequences, marking if the sequence listed is the cannonical sequence of the matching entry.

PREFIX taxon: <http://purl.uniprot.org/taxonomy/>
PREFIX up: <http://purl.uniprot.org/core/>
SELECT ?entry ?sequence ?isCanonical
WHERE {
  # We don't want to look into the UniParc graph which will 
  # confuse matters
  GRAPH <http://sparql.uniprot.org/uniprot> {
      # we need the UniProt entries that are human
      ?entry a up:Protein ;
	up:organism taxon:9606 ;
      	up:sequence ?sequence .
      # If the sequence is a "Simple_Sequence" it is likely to be the 
      # cannonical sequence
      OPTIONAL {
       	?sequence a up:Simple_Sequence .
        BIND(true AS ?likelyIsCanonical)
      }
      # unless we are dealing with an external isoform
      # see https://www.uniprot.org/help/canonical_and_isoforms
      OPTIONAL {
       	FILTER(?likelyIsCanonical)
        ?sequence a up:External_Sequence .
        BIND(true AS ?isComplicated)
      }
      # If it is an external isoform it's id would not match the 
      # entry primary accession
      BIND(IF(?isComplicated, STRENDS(STR(?entry), STRBEFORE(SUBSTR(STR(?sequence), 34),'-')),?likelyIsCanonical) AS ?isCanonical)
  }
}


#78_genetic_disease_related_proteins: List all UniProtKB proteins annotated to be related to a genetic disease.
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX up: <http://purl.uniprot.org/core/>
SELECT
  ?uniprot ?disease ?diseaseComment ?mim
WHERE
{
  GRAPH <http://sparql.uniprot.org/uniprot> {
    ?uniprot a up:Protein ;
       up:annotation ?diseaseAnnotation .
    ?diseaseAnnotation up:disease ?disease .
  }
  GRAPH <http://sparql.uniprot.org/diseases> {
    ?disease a up:Disease ;
             rdfs:comment ?diseaseComment .
    OPTIONAL {
      ?disease rdfs:seeAlso ?mim .
       ?mim up:database <http://purl.uniprot.org/database/MIM> .
    }
  }
}