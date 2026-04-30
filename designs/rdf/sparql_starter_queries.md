# SPARQL Starter Queries

These are starter queries for the IFX RDF export derived from RaMP.

See also:

- `designs/rdf/ramp_rdf_ontology.ttl`
- `designs/rdf/ramp_rdf_handoff_note.md`

## Public Endpoint

The public SPARQL endpoint for the current prototype is:

```text
https://stitcher.ncats.io/ramp/sparql
```

You can send queries with either `GET` or `POST`. `POST` is recommended for
longer queries.

Example `POST` query with `curl`:

```bash
curl -s \
  -H 'Accept: application/sparql-results+json' \
  --data 'query=SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }' \
  https://stitcher.ncats.io/ramp/sparql
```

Example `GET` query in a browser:

```text
https://stitcher.ncats.io/ramp/sparql?query=SELECT%20(COUNT(*)%20AS%20%3Fn)%20WHERE%20%7B%20%3Fs%20%3Fp%20%3Fo%20%7D
```

## Prefixes

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
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

## 2. Find A Metabolite From An Alternate ID And Show All Properties

This example works against the current exported subset.

```sparql
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

SELECT ?metabolite ?p ?o
WHERE {
  ?metabolite skos:exactMatch <https://identifiers.org/hmdb:HMDB0000001> ;
              ?p ?o .
}
ORDER BY ?metabolite ?p ?o
```

If you want to inspect a different identifier namespace, replace the object IRI
accordingly, for example:

- `https://identifiers.org/kegg.compound:C01152`
- `http://purl.obolibrary.org/obo/CHEBI_70958`
- `https://identifiers.org/hmdb:HMDB0000002`

## 3. Find Pathways For A Metabolite And Show Pathway And Edge Properties

This example starts from a known metabolite alternate ID and returns named
columns for both:

- pathway node properties
- `ifx:MetabolitePathwayEdge` statement properties

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX ifx: <https://ifx.ncats.nih.gov/ontology/>

SELECT
  ?metabolite
  ?metName
  ?pathway
  ?pathwayName
  ?pathwayId
  ?pathwaySourceId
  ?pathwayType
  ?pathwayCategory
  ?pathwaySources
  ?edgeStmt
  ?edgeSource
  ?edgeSources
WHERE {
  ?metabolite skos:exactMatch <https://identifiers.org/hmdb:HMDB0000001> ;
              ifx:hasPathway ?pathway .

  OPTIONAL { ?metabolite rdfs:label ?metName }

  OPTIONAL { ?pathway rdfs:label ?pathwayName }
  OPTIONAL { ?pathway ifx:id ?pathwayId }
  OPTIONAL { ?pathway ifx:source_id ?pathwaySourceId }
  OPTIONAL { ?pathway ifx:type ?pathwayType }
  OPTIONAL { ?pathway ifx:category ?pathwayCategory }
  OPTIONAL { ?pathway ifx:sources ?pathwaySources }

  OPTIONAL {
    ?edgeStmt rdf:type ifx:MetabolitePathwayEdge ;
              ifx:subject ?metabolite ;
              ifx:predicate ifx:hasPathway ;
              ifx:object ?pathway .
    OPTIONAL { ?edgeStmt ifx:source ?edgeSource }
    OPTIONAL { ?edgeStmt ifx:sources ?edgeSources }
  }
}
ORDER BY ?pathway
LIMIT 25
```

## 4. Find A Pathway By Pathway Name

Replace the pathway name string with the pathway you want to inspect.

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ifx: <https://ifx.ncats.nih.gov/ontology/>

SELECT ?pathway ?p ?o
WHERE {
  ?pathway rdf:type ifx:Pathway ;
           rdfs:label "Histidine metabolism" ;
           ?p ?o .
}
ORDER BY ?pathway ?p ?o
```

## 5. Retrieve Metabolites And Proteins For A Given Pathway

This returns one row per metabolite or protein linked to the named pathway.

```sparql
PREFIX rdf:
  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
  PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
  PREFIX ifx: <https://ifx.ncats.nih.gov/ontology/>

  SELECT
    ?pathway
    ?pathwayName
    ?analyte
    ?analyteType
    ?analyteName
    ?edgeStmt
    ?edgeSource
    ?edgeSources
  WHERE {
    ?pathway rdf:type ifx:Pathway ;
             rdfs:label "Histidine metabolism" .
    BIND("Histidine metabolism" AS ?pathwayName)

    {
      ?analyte rdf:type ifx:Metabolite ;
               ifx:hasPathway ?pathway .
      BIND("Metabolite" AS ?analyteType)
      OPTIONAL { ?analyte rdfs:label ?analyteName }

      OPTIONAL {
        ?edgeStmt rdf:type ifx:MetabolitePathwayEdge ;
                  ifx:subject ?analyte ;
                  ifx:predicate ifx:hasPathway ;
                  ifx:object ?pathway .
        OPTIONAL { ?edgeStmt ifx:source ?edgeSource }
        OPTIONAL { ?edgeStmt ifx:sources ?edgeSources }
      }
    }
    UNION
    {
      ?analyte rdf:type ifx:Protein ;
               ifx:hasPathway ?pathway .
      BIND("Protein" AS ?analyteType)
      OPTIONAL { ?analyte rdfs:label ?analyteName }

      OPTIONAL {
        ?edgeStmt rdf:type ifx:ProteinPathwayEdge ;
                  ifx:subject ?analyte ;
                  ifx:predicate ifx:hasPathway ;
                  ifx:object ?pathway .
        OPTIONAL { ?edgeStmt ifx:source ?edgeSource }
        OPTIONAL { ?edgeStmt ifx:sources ?edgeSources }
      }
    }
  }
  ORDER BY ?analyteType ?analyteName ?analyte
  LIMIT 200
```

## 6. Find Reactions For An Analyte And Show Reaction And Edge Properties

This example starts from a known metabolite alternate ID and returns named
columns for both:

- reaction node properties
- `ifx:MetaboliteReactionEdge` statement properties

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX ifx: <https://ifx.ncats.nih.gov/ontology/>

SELECT
  ?metabolite
  ?metName
  ?reaction
  ?reactionLabel
  ?reactionId
  ?reactionSourceId
  ?reactionDirection
  ?reactionEquation
  ?reactionHtmlEquation
  ?reactionIsTransport
  ?reactionSources
  ?edgeStmt
  ?edgeSubstrateProduct
  ?edgeIsCofactor
  ?edgeSources
WHERE {
  ?metabolite skos:exactMatch <https://identifiers.org/hmdb:HMDB0000010> ;
              ifx:hasReaction ?reaction .

  OPTIONAL { ?metabolite rdfs:label ?metName }

  OPTIONAL { ?reaction ifx:label ?reactionLabel }
  OPTIONAL { ?reaction ifx:id ?reactionId }
  OPTIONAL { ?reaction ifx:source_id ?reactionSourceId }
  OPTIONAL { ?reaction ifx:direction ?reactionDirection }
  OPTIONAL { ?reaction ifx:equation ?reactionEquation }
  OPTIONAL { ?reaction ifx:html_equation ?reactionHtmlEquation }
  OPTIONAL { ?reaction ifx:is_transport ?reactionIsTransport }
  OPTIONAL { ?reaction ifx:sources ?reactionSources }

  OPTIONAL {
    ?edgeStmt rdf:type ifx:MetaboliteReactionEdge ;
              ifx:subject ?metabolite ;
              ifx:predicate ifx:hasReaction ;
              ifx:object ?reaction .
    OPTIONAL { ?edgeStmt ifx:substrate_product ?edgeSubstrateProduct }
    OPTIONAL { ?edgeStmt ifx:is_cofactor ?edgeIsCofactor }
    OPTIONAL { ?edgeStmt ifx:sources ?edgeSources }
  }
}
ORDER BY ?reaction
LIMIT 50
```

## 7. Start From A List Of Alternate IDs And Find Linked Pathways

This query starts from a mixed list of alternate IDs and returns any matching
metabolites or proteins together with their linked pathways.

Replace the example IRIs in the `VALUES` block with the identifiers you want to
test. For proteins in this subset, useful identifiers are often UniProt,
NCBIGene, or Ensembl IRIs if they were exported as `skos:exactMatch`.

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
PREFIX ifx: <https://ifx.ncats.nih.gov/ontology/>

SELECT DISTINCT
  ?inputXref
  ?analyte
  ?analyteType
  ?analyteName
  ?pathway
  ?pathwayName
  ?pathwayId
  ?pathwaySourceId
WHERE {
  VALUES ?inputXref {
    <https://identifiers.org/hmdb:HMDB0000010>
    <https://identifiers.org/hmdb:HMDB0000002>
    <https://identifiers.org/ncbigene:1312>
  }

  {
    ?analyte rdf:type ifx:Metabolite ;
             skos:exactMatch ?inputXref ;
             ifx:hasPathway ?pathway .
    BIND("Metabolite" AS ?analyteType)
  }
  UNION
  {
    ?analyte rdf:type ifx:Protein ;
             skos:exactMatch ?inputXref ;
             ifx:hasPathway ?pathway .
    BIND("Protein" AS ?analyteType)
  }

  OPTIONAL { ?analyte rdfs:label ?analyteName }
  OPTIONAL { ?pathway rdfs:label ?pathwayName }
  OPTIONAL { ?pathway ifx:id ?pathwayId }
  OPTIONAL { ?pathway ifx:source_id ?pathwaySourceId }
}
ORDER BY ?inputXref ?analyteType ?pathwayName ?pathway
LIMIT 200
```
