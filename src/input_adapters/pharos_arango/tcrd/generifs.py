import time
from typing import Generator, List, Union
from src.input_adapters.pharos_arango.tcrd.protein import PharosArangoAdapter
from src.models.datasource_version_info import DataSourceDetails
from src.models.gene import Gene
from src.models.generif import GeneGeneRifRelationship, GeneRif
from src.models.node import Node, Relationship


def generif_query(last_key: str = None, limit: int = 10000) -> str:
    filter_clause = f'FILTER assoc._key > "{last_key}"' if last_key else ""
    return f"""
    FOR assoc IN GeneGeneRifRelationship
        {filter_clause}
        SORT assoc._key
        LIMIT {limit}
        LET rif = DOCUMENT(assoc._to)
        RETURN {{
            "_key": assoc._key,
            "ifxgene_id": assoc.start_id,
            "gene_id": assoc.gene_id,
            "text": rif.text,
            "date": assoc.date,
            "pmids": assoc.pmids
        }}
    """



def generif_version_query():
    return """FOR assoc IN `GeneGeneRifRelationship`
    LIMIT 1
    RETURN assoc.creation
    """


class GeneRifAdapter(PharosArangoAdapter):
    batch_size = 10_000
    min_batch_size = 500

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        last_key = None
        batch_size = self.batch_size

        while True:
            query = generif_query(last_key=last_key, limit=batch_size)

            start_time = time.time()
            try:
                rows = list(self.runQuery(query))
            except Exception as e:
                if "504" in str(e) and batch_size > self.min_batch_size:
                    print(f"504 error after last_key={last_key}, reducing batch_size from {batch_size} to {batch_size // 2}")
                    batch_size //= 2
                    continue
                raise

            elapsed = time.time() - start_time
            print(f"Fetched {len(rows)} rows after last_key={last_key} (batch_size={batch_size}) in {elapsed:.2f} sec")

            if not rows:
                break  # no more results

            yield list(set([
                GeneGeneRifRelationship(
                    start_node=Gene(id=row['ifxgene_id']),
                    end_node=GeneRif(text=row['text'], id=str(hash(row['text']))),
                    gene_id=row['gene_id'],
                    date=row['date'],
                    pmids=row['pmids']
                )
                for row in rows
            ]))



            # Advance bookmark
            last_key = rows[-1]["_key"]

    def get_version_info_query(self) -> DataSourceDetails:
        raw_version_info = self.runQuery(generif_version_query())[0]
        return DataSourceDetails.parse_tsv(raw_version_info)
