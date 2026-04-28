from src.use_cases.gramp.convert_gramp_to_rdf import (
    GRAMP_EDGE_PREDICATE_MAP,
    GRAMP_EXCLUDED_FIELDS,
    GRAMP_METABOHUB_COLLECTIONS,
    GrampBiolinkRdfConverter,
)
from src.use_cases.arango_to_rdf import load_credentials


def main():
    converter = GrampBiolinkRdfConverter(
        arango_credentials=load_credentials("./src/use_cases/secrets/ifxdev_arangodb.yaml"),
        arango_db_name="gramp",
        output_file="./output/gramp_full.nt",
        base_resource_uri="https://ifx.ncats.nih.gov/resource/",
        base_ontology_uri="https://ifx.ncats.nih.gov/ontology/",
        edge_predicate_map=GRAMP_EDGE_PREDICATE_MAP,
        excluded_fields=GRAMP_EXCLUDED_FIELDS,
    )

    try:
        converter.export(include_collections=GRAMP_METABOHUB_COLLECTIONS)
    finally:
        converter.close()


if __name__ == "__main__":
    main()
