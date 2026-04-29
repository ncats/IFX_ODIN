# Fuseki Deployment

This directory contains a read-only Fuseki deployment intended for public SPARQL query access to a RaMP-derived RDF subset.

## Security model

- Public users can query the dataset at `/ramp/sparql`.
- Public users cannot use upload or update endpoints because they are not configured in `configuration/assembler.ttl`.
- Fuseki administrative access is protected by the `FUSEKI_ADMIN_PASSWORD` you provide at startup.

This is sufficient for a prototype server, but you should still put the service behind HTTPS in production. If you want the admin UI fully hidden from the public internet, block `/$/` at the reverse proxy or firewall layer.

## Layout

- `docker-compose.yml`: runtime container definition
- `configuration/assembler.ttl`: read-only dataset service definition
- `databases/`: persisted TDB2 database files on the host

The `configuration/` directory is bind-mounted read-write because this Fuseki image expects to be able to write under `/fuseki-base/configuration` during startup.

## First-time setup

1. Create an environment file in this directory:

```env
FUSEKI_ADMIN_PASSWORD=change-this-password
```

Use a long random password made of letters, numbers, `_`, `-`, or `.`. Avoid characters such as `&`, `/`, and `\` because this image writes the password into `shiro.ini` during startup and special replacement characters can corrupt the generated auth line.

2. Create the database directory:

```bash
mkdir -p rdf-container/databases/ramp
```

3. Make sure the mounted directories are writable by the container user:

```bash
chmod -R a+rwX rdf-container/configuration rdf-container/databases
```

4. Start the container:

```bash
cd rdf-container
docker compose up -d
```

The public query endpoint will be:

```text
http://HOSTNAME:3030/ramp/sparql
```

The browser query UI will be:

```text
http://HOSTNAME:3030/#/dataset/ramp/query
```

## Loading RDF data

Load data into the persistent TDB2 store with the Jena bulk loader. Stop Fuseki first so only one JVM is accessing the database during the load.

```bash
cd rdf-container
docker compose stop fuseki
docker compose run --rm \
  -v /absolute/path/to/gramp_metabohub_subset.nt:/data/import.nt:ro \
  fuseki \
  tdb2.tdbloader --loc=/fuseki-base/databases/ramp /data/import.nt
docker compose up -d
```

After the server starts again, verify the dataset with:

```sparql
SELECT (COUNT(*) AS ?n)
WHERE {
  ?s ?p ?o
}
```

## Notes

- The configured dataset name is `ramp`, not `gramp`.
- `tdb2:unionDefaultGraph true` makes default-graph queries match across named graphs in the TDB2 dataset.
- If you later want HTTPS and a friendlier public UI, put this service behind Nginx or Caddy and optionally add YASGUI.
- If you want to block public access to the admin routes, do it in the reverse proxy instead of bind-mounting `shiro.ini` into this image.
