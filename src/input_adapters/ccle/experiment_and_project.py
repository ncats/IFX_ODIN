import os
from abc import ABC
from datetime import date, datetime
from typing import List, Union

from src.constants import DataSourceName
from src.interfaces.input_adapter import NodeInputAdapter, RelationshipInputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import Node, Relationship
from src.models.pounce.experiment import Experiment


class CCLEInputAdapter(NodeInputAdapter, RelationshipInputAdapter, ABC):
    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.CCLE

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version="CCLE 2019"
        )

    def get_experiment_name(self):
        return f"{self.get_datasource_name().value} - {self.get_version().version}"

    def __init__(self):
        NodeInputAdapter.__init__(self)
        RelationshipInputAdapter.__init__(self)

class CCLEFileInputAdapter(CCLEInputAdapter, ABC):
    file_path: str
    download_date: date

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version="CCLE 2019",
            download_date=self.download_date
        )

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.download_date = datetime.fromtimestamp(os.path.getmtime(file_path)).date()
        CCLEInputAdapter.__init__(self)


class ExperimentAdapter(CCLEInputAdapter):
    def get_all(self) -> List[Union[Node, Relationship]]:
        return [Experiment(
            id=self.get_experiment_name(),
            name=self.get_datasource_name().value,
            type='RNA-seq',
            description='The Cancer Cell Line Encyclopedia (CCLE) project started in 2008 as a collaboration between the Broad Institute, and the Novartis Institutes for Biomedical Research and its Genomics Institute of the Novartis Research Foundation. The goal is to conduct a detailed genetic and pharmacologic characterization of a large panel of human cancer models, to develop integrated computational analyses that link distinct pharmacologic vulnerabilities to genomic patterns and to translate cell line integrative genomics into cancer patient stratification. Later the MD Anderson and Harvard Medical school joined the project. As of summer of 2018 CCLE continues its efforts as part of the Broad Cancer Dependency Map Project.',
            category="in vitro"
        )]