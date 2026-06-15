# Legacy GraphQL API

This directory contains the old GraphQL/dashboard API implementation that used
`APIAdapter`, Arango API adapters, Strawberry query models, and the
`graphql-container` runtime.

It has been moved out of the active source tree because the current project is
moving toward a registry-backed resolver API. The code is retained here as a
reference for prior schema, query, facet, and container work, but active ETL and
QA Browser code should not import from this directory.
