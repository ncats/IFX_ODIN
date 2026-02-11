import os
import csv
from datetime import datetime
from typing import Generator, List
import gffutils

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.gene import Gene, GeneticLocation, Strand


class GeneAdapter(InputAdapter):
    version_info: DatasourceVersionInfo
    data_file_path: str

    def __init__(self, data_file_path: str, version_file_path: str):
        InputAdapter.__init__(self)
        self.data_file_path = data_file_path
        ensembl_version = None

        with open(version_file_path, "r", encoding="utf-8") as vf:
            reader = csv.DictReader(line for line in vf if line.strip())
            first_row = next(reader)
            ensembl_version = first_row.get("ENSEMBL_VERSION")

        self.version_info = DatasourceVersionInfo(
            version=ensembl_version,
            download_date=datetime.fromtimestamp(os.path.getmtime(data_file_path)).date()
        )

    def connect_to_temp_gene_db(self):
        db_path = os.path.join(os.path.dirname(self.data_file_path), 'genes.db')
        rebuild_db = self.db_has_changed_since_build(db_path)
        if rebuild_db:
            return self.create_new_temp_db(db_path)
        else:
            return self.connect_to_existing_temp_db(db_path)

    def connect_to_existing_temp_db(self, db_path):
        print(f"File hasn't changed recently, Reusing database at {db_path}...")
        db = gffutils.FeatureDB(db_path)
        return db

    def create_new_temp_db(self, db_path):
        print(f"Building/updating database at {db_path}...")
        db = gffutils.create_db(
            self.data_file_path,
            dbfn=db_path,
            force=True,
            disable_infer_genes=True,
            disable_infer_transcripts=True,
            merge_strategy='merge'
        )
        return db

    def db_has_changed_since_build(self, db_path):
        rebuild_db = True
        if os.path.exists(db_path):
            gtf_mtime = os.path.getmtime(self.data_file_path)
            db_mtime = os.path.getmtime(db_path)
            if db_mtime >= gtf_mtime:
                rebuild_db = False
        return rebuild_db

    def get_all(self) -> Generator[List[Gene], None, None]:
        db = self.connect_to_temp_gene_db()
        genes = []
        for gene in db.features_of_type('gene'):
            if gene.chrom.find('.') >= 0:
                continue
            genes.append(Gene(
                id=gene.id,
                location=GeneticLocation(
                    chromosome=gene.chrom,
                    start=gene.start,
                    end=gene.end,
                    strand=Strand.parse(gene.strand)
                ),
                symbol=gene.attributes.get('gene_name', [None])[0],
                biotype=gene.attributes.get('gene_biotype', [None])[0]
            ))

        yield genes


    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.ENSEMBL

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info