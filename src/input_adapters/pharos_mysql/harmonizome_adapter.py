from typing import Generator, List

from src.input_adapters.pharos_mysql.base import Pharos319Adapter
from src.models.harmonizome import (
    HarmonizomeGeneAttributeType,
    HarmonizomeHgramCDF,
)
from src.shared.sqlalchemy_tables.pharos_tables_old import (
    GeneAttributeType as mysql_GeneAttributeType,
    HgramCDF as mysql_HgramCDF,
    Protein as mysql_Protein,
)


class HarmonizomeGeneAttributeTypeAdapter(Pharos319Adapter):
    batch_size: int = 1000

    def get_all(self) -> Generator[List[HarmonizomeGeneAttributeType], None, None]:
        session = self.get_session()
        try:
            rows = (
                session
                .query(
                    mysql_GeneAttributeType.id,
                    mysql_GeneAttributeType.name,
                    mysql_GeneAttributeType.association,
                    mysql_GeneAttributeType.description,
                    mysql_GeneAttributeType.resource_group,
                    mysql_GeneAttributeType.measurement,
                    mysql_GeneAttributeType.attribute_group,
                    mysql_GeneAttributeType.attribute_type,
                    mysql_GeneAttributeType.pubmed_ids,
                    mysql_GeneAttributeType.url,
                )
                .order_by(mysql_GeneAttributeType.id)
                .all()
            )
        finally:
            session.close()

        batch: List[HarmonizomeGeneAttributeType] = []
        for row in rows:
            (
                legacy_id,
                name,
                association,
                description,
                resource_group,
                measurement,
                attribute_group,
                attribute_type,
                pubmed_ids,
                url,
            ) = row
            batch.append(
                HarmonizomeGeneAttributeType(
                    id=f"old-pharos-harmonizome-gat:{legacy_id}",
                    legacy_id=legacy_id,
                    name=name,
                    association=association,
                    description=description,
                    resource_group=resource_group,
                    measurement=measurement,
                    attribute_group=attribute_group,
                    attribute_type=attribute_type,
                    pubmed_ids=pubmed_ids,
                    url=url,
                )
            )
            if len(batch) >= self.batch_size:
                yield batch
                batch = []

        if batch:
            yield batch


class HarmonizomeHgramCDFAdapter(Pharos319Adapter):
    batch_size: int = 100_000

    def get_all(self) -> Generator[List[HarmonizomeHgramCDF], None, None]:
        last_id = 0
        while True:
            session = self.get_session()
            try:
                rows = (
                    session
                    .query(
                        mysql_HgramCDF.id,
                        mysql_HgramCDF.protein_id,
                        mysql_HgramCDF.type,
                        mysql_HgramCDF.attr_count,
                        mysql_HgramCDF.attr_cdf,
                        mysql_Protein.uniprot,
                        mysql_Protein.geneid,
                        mysql_Protein.sym,
                    )
                    .join(mysql_Protein, mysql_Protein.id == mysql_HgramCDF.protein_id)
                    .filter(mysql_HgramCDF.id > last_id)
                    .order_by(mysql_HgramCDF.id)
                    .limit(self.batch_size)
                    .all()
                )
            finally:
                session.close()

            if not rows:
                break

            batch: List[HarmonizomeHgramCDF] = []
            for row in rows:
                (
                    legacy_id,
                    legacy_protein_id,
                    type_name,
                    attr_count,
                    attr_cdf,
                    uniprot,
                    geneid,
                    symbol,
                ) = row
                batch.append(
                    HarmonizomeHgramCDF(
                        id=f"old-pharos-harmonizome-hgram:{legacy_id}",
                        legacy_protein_id=legacy_protein_id,
                        legacy_uniprot=uniprot,
                        legacy_geneid=geneid,
                        legacy_symbol=symbol,
                        type=type_name,
                        attr_count=attr_count,
                        attr_cdf=attr_cdf,
                    )
                )
                last_id = legacy_id

            yield batch
