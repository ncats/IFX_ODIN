"""Calculate persisted generic-structure classifications from the merged graph."""

from __future__ import annotations

from typing import Generator, List

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.metabolite_harmonization import MetaboliteIdentifier
from src.shared.arango_adapter import ArangoAdapter
from src.shared.db_credentials import DBCredentials
from src.shared.metabolite_generic_structure import (
    GENERIC_STRUCTURE_CLASSIFIER_VERSION,
    classify_generic_structure,
)
from src.shared.record_merger import FieldConflictBehavior


class MetaboliteGenericStructureAdapter(InputAdapter, ArangoAdapter):
    """Derive ``is_generic_structure`` after all structure sources are merged."""

    batch_size = 25000
    field_conflict_behavior = FieldConflictBehavior.KeepLast

    def __init__(self, credentials: DBCredentials, database_name: str):
        ArangoAdapter.__init__(self, credentials=credentials, database_name=database_name)

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.PostProcessing

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(version=GENERIC_STRUCTURE_CLASSIFIER_VERSION)

    @staticmethod
    def _metabolite_structures(row: dict) -> list[dict]:
        return [
            {
                "source": prop.get("source"),
                "source_id": prop.get("source_id"),
                "smiles": (
                    prop.get("iso_smiles")
                    or prop.get("isomeric_smiles")
                    or prop.get("canonical_smiles")
                ),
                "formula": prop.get("molecular_formula"),
                "inchi": prop.get("inchi"),
            }
            for prop in row.get("chem_props") or []
        ]

    @staticmethod
    def _chemical_entity_structure(row: dict) -> dict:
        return {
            "source": "ChEBI",
            "source_id": row.get("id"),
            "smiles": row.get("smiles"),
            "formula": row.get("formula"),
            "inchi": row.get("inchi"),
        }

    def get_all(self) -> Generator[List[MetaboliteIdentifier], None, None]:
        db = self.get_db()
        classification_by_id: dict[str, bool | None] = {}
        known_ids = set()
        for row in db.aql.execute(
            """
            FOR d IN MetaboliteIdentifier
              RETURN {id: d.id, chem_props: d.chem_props || []}
            """,
            batch_size=1000,
            max_runtime=600,
        ):
            identifier = row["id"]
            known_ids.add(identifier)
            classification_by_id[identifier] = classify_generic_structure(
                self._metabolite_structures(row)
            )

        for row in db.aql.execute(
            """
            FOR d IN ChemicalEntity
              FILTER d.smiles != null OR d.formula != null OR d.inchi != null
              RETURN KEEP(d, "id", "smiles", "formula", "inchi")
            """,
            batch_size=1000,
            max_runtime=600,
        ):
            identifier = row["id"]
            if identifier not in known_ids:
                continue
            chemical_classification = classify_generic_structure([
                self._chemical_entity_structure(row)
            ])
            current = classification_by_id.get(identifier)
            if chemical_classification is True:
                classification_by_id[identifier] = True
            elif current is None and chemical_classification is False:
                classification_by_id[identifier] = False

        batch = []
        for identifier in sorted(classification_by_id):
            classification = classification_by_id[identifier]
            if classification is None:
                continue
            batch.append(MetaboliteIdentifier(
                id=identifier,
                is_generic_structure=classification,
            ))
            if len(batch) >= self.batch_size:
                yield batch
                batch = []
        if batch:
            yield batch
