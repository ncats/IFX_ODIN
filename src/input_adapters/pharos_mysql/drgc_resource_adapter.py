from typing import Generator, List

from src.input_adapters.pharos_mysql.base import Pharos319Adapter
from src.models.drgc_resource import DRGCResource
from src.shared.sqlalchemy_tables.pharos_tables_old import (
    DrgcResource as mysql_DrgcResource,
    Protein as mysql_Protein,
    T2TC as mysql_T2TC,
)


class DRGCResourceAdapter(Pharos319Adapter):
    batch_size: int = 1000

    def get_all(self) -> Generator[List[DRGCResource], None, None]:
        rows = (
            self.get_session().query(
                mysql_DrgcResource.rssid,
                mysql_DrgcResource.resource_type,
                mysql_DrgcResource.json,
                mysql_DrgcResource.target_id,
                mysql_Protein.uniprot,
            )
            .join(mysql_T2TC, mysql_T2TC.target_id == mysql_DrgcResource.target_id)
            .join(mysql_Protein, mysql_Protein.id == mysql_T2TC.protein_id)
            .order_by(mysql_DrgcResource.id)
        )

        batch: List[DRGCResource] = []
        for rssid, resource_type, json_payload, legacy_target_id, uniprot in rows:
            batch.append(
                DRGCResource(
                    id=rssid,
                    uniprot_id=uniprot,
                    rssid=rssid,
                    resource_type=resource_type,
                    json=json_payload,
                    legacy_target_id=legacy_target_id,
                )
            )
            if len(batch) >= self.batch_size:
                yield batch
                batch = []

        if batch:
            yield batch
