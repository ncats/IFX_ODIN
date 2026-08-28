import yaml
from pathlib import Path

from src.registry.storage import load_registry_credentials
from src.shared.db_credentials import DBCredentials
from src.use_cases.arango_to_mysql import ArangoToMySqlConverter

arango_credentials_file = "./src/use_cases/secrets/ifxdev_arangodb.yaml"
object_storage_credentials_file = "./src/use_cases/secrets/aws_ifx_registry.yaml"
mysql_credentials_file = "./src/use_cases/secrets/galeradev_write.yaml"

# arango_credentials_file = "./src/use_cases/secrets/local_arangodb.yaml"
# object_storage_credentials_file = "./src/use_cases/secrets/local_s3.yaml"
# mysql_credentials_file = "./src/use_cases/secrets/local_mysql.yaml"

with open(arango_credentials_file, "r") as file:
    arango_credentials = DBCredentials.from_yaml(yaml.safe_load(file))

with open(mysql_credentials_file, "r") as file:
    mysql_credentials = DBCredentials.from_yaml(yaml.safe_load(file))

object_storage_credentials = load_registry_credentials(Path(object_storage_credentials_file))

conv = ArangoToMySqlConverter(
    arango_credentials=arango_credentials,
    arango_db_name='pounce',
    mysql_credentials=mysql_credentials,
    mysql_db_name='omicsdb_dev2',
    object_storage_credentials=object_storage_credentials)

conv.convert()
