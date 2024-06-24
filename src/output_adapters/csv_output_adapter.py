from dataclasses import fields

import os
import shutil
import csv

from src.interfaces.output_adapter import OutputAdapter
from src.models.analyte import Analyte
from src.models.node import Relationship


class CsvOutputAdapter(OutputAdapter):
    destination_directory: str

    def __init__(self, destination_directory: str):
        self.destination_directory = os.path.expanduser(destination_directory)

    def create_or_truncate_datastore(self) -> bool:
        if not os.path.exists(self.destination_directory):
            os.makedirs(self.destination_directory)
            print(f"Destination: '{self.destination_directory}' is ready.")
            return True
        else:
            files_exist = any(os.path.isfile(os.path.join(self.destination_directory, f)) for f in os.listdir(self.destination_directory))
            if files_exist:
                user_input = input(f"Directory {self.destination_directory} is not empty. Do you want to delete all files in it? (yes/no): ")
                if user_input.lower() == 'yes':
                    for filename in os.listdir(self.destination_directory):
                        file_path = os.path.join(self.destination_directory, filename)
                        if os.path.isfile(file_path) or os.path.islink(file_path):
                            os.unlink(file_path)
                        elif os.path.isdir(file_path):
                            shutil.rmtree(file_path)
                    print(f"Destination: '{self.destination_directory}' is ready.")
                    return True
                else:
                    print("Operation cancelled by the user.")
                    return False
            else:
                print(f"Destination: '{self.destination_directory}' is ready.")
                return True

    @staticmethod
    def default_headers_and_data(obj):
        headers = [field.name for field in fields(obj)]
        data = [getattr(obj, field) for field in headers]
        if isinstance(obj, Relationship):
            start_node = headers.index('start_node')
            end_node = headers.index('end_node')
            headers[start_node] = 'start_id'
            headers[end_node] = 'end_id'
            data[start_node] = obj.start_node.id
            data[end_node] = obj.end_node.id
        return headers, data

    def store(self, objects) -> bool:
        if not isinstance(objects, list):
            objects = [objects]
        for obj in objects:
            file_path = f"{self.destination_directory}/{obj.__class__.__name__}.csv"

            headers, data = self.default_headers_and_data(obj)

            if isinstance(obj, MergeObject):
                nested_obj = obj.obj
                field = obj.field
                file_path = f"{self.destination_directory}/{nested_obj.__class__.__name__}.{field}.csv"

                if isinstance(nested_obj, Analyte):
                    if field == 'synonyms':
                        headers = ['id', 'synonyms', 'sources']
                        data = [
                            nested_obj.id,
                            list(set([syn.term for syn in nested_obj.synonyms])),
                            list(set([syn.source for syn in nested_obj.synonyms]))
                        ]
                    if field == 'equivalent_ids':
                        headers = ['id', 'equivalent_ids', 'equivalent_id_types', 'equivalent_id_statuses', 'equivalent_id_sources']
                        data = [
                            nested_obj.id,
                            list(set([equiv.id for equiv in nested_obj.equivalent_ids])),
                            list(set([equiv.type for equiv in nested_obj.equivalent_ids])),
                            list(set([equiv.status for equiv in nested_obj.equivalent_ids])),
                            list(set([equiv.source for equiv in nested_obj.equivalent_ids])),
                        ]

            file_exists = os.path.isfile(file_path)
            with open(file_path, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)
                if not file_exists:
                    writer.writerow(headers)
                writer.writerow(data)
        return True
