from abc import ABC
from typing import List

from src.interfaces.output_adapter import OutputAdapter

from src.shared.db_credentials import DBCredentials
from src.shared.graphdb_data_loader import GraphDBDataLoader, Neo4jDataLoader, MemgraphDataLoader


class GraphDBOutputAdapter(OutputAdapter, ABC):
    loader: GraphDBDataLoader
    post_processing: List[str]

    def __init__(self, post_processing: List[str] = [], **kwargs):
        super().__init__()
        self.post_processing = post_processing

    def create_or_truncate_datastore(self) -> bool:
        return self.loader.delete_all_data_and_indexes()


    def store(self, objects) -> bool:
        if not isinstance(objects, list):
            objects = [objects]

        object_groups = self.sort_and_convert_objects(objects)
        for obj_list, labels, is_relationship, start_labels, end_labels in object_groups.values():
            if is_relationship:
                self.loader.load_relationship_records(obj_list, start_labels, labels, end_labels)
            else:
                self.loader.load_node_records(obj_list, labels)
        return True

class MemgraphOutputAdapter(GraphDBOutputAdapter):

    loader = MemgraphDataLoader

    def __init__(self, credentials: DBCredentials, post_processing: List[str] = [], **kwargs):
        super().__init__(post_processing, **kwargs)
        self.loader = MemgraphDataLoader(credentials, **kwargs)


    def do_post_processing(self) -> None:
        for post_process in self.post_processing:
            print('Running post processing step:')
            print(post_process)
            self.loader.memgraph.execute(post_process)


class Neo4jOutputAdapter(GraphDBOutputAdapter):
    loader: Neo4jDataLoader

    def __init__(self, credentials: DBCredentials, post_processing: List[str] = [], **kwargs):
        super().__init__(post_processing, **kwargs)
        self.loader = Neo4jDataLoader(credentials, **kwargs)

    def do_post_processing(self) -> None:
        for post_process in self.post_processing:
            print('Running post processing step:')
            print(post_process)
            with self.loader.driver.session() as session:
                session.run(post_process)