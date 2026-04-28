# SPARQL Starter Queries

These are starter queries for the IFX RDF export derived from RaMP.

Assumptions:

- namespace prefix `ifx:` points at the exported ontology namespace
- `rdfs:label` is used for primary names
- `skos:altLabel` is used for synonyms
- `skos:exactMatch` is used for xrefs
- resources use `rdf:type`
- simple edge predicates are exposed directly
- edge payload resources may exist as separate statement nodes

See also:

- `designs/rdf/ramp_rdf_ontology.ttl`
- `designs/rdf/ramp_rdf_handoff_note.md`

## Prefixes

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX ifx: <https://ifx.ncats.nih.gov/ontology/>
```

## 1. Count All Metabolites

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ifx: <https://ifx.ncats.nih.gov/ontology/>

SELECT (COUNT(*) AS ?n)
WHERE {
  ?metabolite rdf:type ifx:Metabolite .
}
```

## 2. Show A Few Metabolites With Names

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ifx: <https://ifx.ncats.nih.gov/ontology/>

SELECT ?metabolite ?name
WHERE {
  ?metabolite rdf:type ifx:Metabolite ;
              rdfs:label ?name .
}
LIMIT 25
```

## 3. Show Metabolites By Name Prefix

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ifx: <https://ifx.ncats.nih.gov/ontology/>

SELECT ?metabolite ?name
WHERE {
  ?metabolite rdf:type ifx:Metabolite ;
              rdfs:label ?name .
  FILTER(STRSTARTS(LCASE(STR(?name)), "3"))
}
LIMIT 25
```

## 4. Count Pathways

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ifx: <https://ifx.ncats.nih.gov/ontology/>

SELECT (COUNT(*) AS ?n)
WHERE {
  ?pathway rdf:type ifx:Pathway .
}
```

## 5. Metabolite To Pathway Join

Assumes direct edge predicate `ifx:hasPathway`.

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ifx: <https://ifx.ncats.nih.gov/ontology/>

SELECT ?metabolite ?metName ?pathway ?pathwayName
WHERE {
  ?metabolite rdf:type ifx:Metabolite ;
              ifx:hasPathway ?pathway .
  OPTIONAL { ?metabolite rdfs:label ?metName }
  OPTIONAL { ?pathway rdfs:label ?pathwayName }
}
LIMIT 50
```

## 6. Metabolite To Reaction Join

Assumes direct edge predicate `ifx:hasReaction`.

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ifx: <https://ifx.ncats.nih.gov/ontology/>

SELECT ?metabolite ?metName ?reaction ?reactionLabel
WHERE {
  ?metabolite rdf:type ifx:Metabolite ;
              ifx:hasReaction ?reaction .
  OPTIONAL { ?metabolite rdfs:label ?metName }
  OPTIONAL { ?reaction rdfs:label ?reactionLabel }
}
LIMIT 50
```

## 7. Count Edge Payload Statement Nodes

Useful if you export reified statement resources for edges with metadata.

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ifx: <https://ifx.ncats.nih.gov/ontology/>

SELECT ?edgeType (COUNT(*) AS ?n)
WHERE {
  ?stmt rdf:type ?edgeType ;
        ifx:subject ?s ;
        ifx:predicate ?p ;
        ifx:object ?o .
}
GROUP BY ?edgeType
ORDER BY DESC(?n)
```

## 8. Inspect Edge Metadata

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ifx: <https://ifx.ncats.nih.gov/ontology/>

SELECT ?stmt ?s ?p ?o ?source ?role
WHERE {
  ?stmt ifx:subject ?s ;
        ifx:predicate ?p ;
        ifx:object ?o .
  OPTIONAL { ?stmt ifx:source ?source }
  OPTIONAL { ?stmt ifx:substrate_product ?role }
}
LIMIT 25
```

## 9. Find Resources By Source Tag

Good for sanity-checking merged provenance.

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ifx: <https://ifx.ncats.nih.gov/ontology/>

SELECT ?resource ?source
WHERE {
  ?resource ifx:sources ?source .
  FILTER(CONTAINS(LCASE(STR(?source)), "ramp"))
}
LIMIT 50
```

## 10. Find Metabolite By Exact RaMP Identifier

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ifx: <https://ifx.ncats.nih.gov/ontology/>

SELECT ?resource
WHERE {
  ?resource ifx:id "RAMP_C_000265500" .
}
```

## 11. Prefix Search On Metabolite Name

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ifx: <https://ifx.ncats.nih.gov/ontology/>

SELECT ?metabolite ?name
WHERE {
  ?metabolite rdf:type ifx:Metabolite ;
              rdfs:label ?name .
  FILTER(STRSTARTS(LCASE(STR(?name)), "3"))
}
LIMIT 50
```

## 12. Top Pathways By Number Of Metabolites

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX ifx: <https://ifx.ncats.nih.gov/ontology/>

SELECT ?pathway ?pathwayName (COUNT(DISTINCT ?metabolite) AS ?metaboliteCount)
WHERE {
  ?metabolite ifx:hasPathway ?pathway .
  OPTIONAL { ?pathway rdfs:label ?pathwayName }
}
GROUP BY ?pathway ?pathwayName
ORDER BY DESC(?metaboliteCount)
LIMIT 25
```

## 13. Find Metabolite Synonyms

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX ifx: <https://ifx.ncats.nih.gov/ontology/>

SELECT ?metabolite ?name ?synonym
WHERE {
  ?metabolite rdf:type ifx:Metabolite ;
              rdfs:label ?name ;
              skos:altLabel ?synonym .
}
LIMIT 50
```

## 14. Find Cross-References For One Metabolite

```sparql
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT ?xref
WHERE {
  <https://ifx.ncats.nih.gov/resource/Metabolite/RAMP_C_000265500> skos:exactMatch ?xref .
}
ORDER BY ?xref
```

## 15. Count Distinct Sources

```sparql
PREFIX ifx: <https://ifx.ncats.nih.gov/ontology/>

SELECT ?source (COUNT(*) AS ?n)
WHERE {
  ?resource ifx:sources ?source .
}
GROUP BY ?source
ORDER BY DESC(?n)
LIMIT 50
```

## 16. Inspect One Resource Fully

Replace the IRI with a real exported resource.

```sparql
SELECT ?p ?o
WHERE {
  <https://ifx.ncats.nih.gov/resource/Metabolite/RAMP_C_000265500> ?p ?o .
}
ORDER BY ?p
```

## 17. Sanity Check Class Distribution

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

SELECT ?class (COUNT(*) AS ?n)
WHERE {
  ?resource rdf:type ?class .
}
GROUP BY ?class
ORDER BY DESC(?n)
LIMIT 100
```

## Notes

- If a query returns nothing, first inspect one known resource with query 16.
- The ontology and property definitions for this export are in `ramp_rdf_ontology.ttl`.
- For very large datasets, start with `LIMIT` and narrow to one class or relation at a time.
