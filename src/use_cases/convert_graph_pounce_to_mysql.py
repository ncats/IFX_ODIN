import yaml

from src.shared.db_credentials import DBCredentials
from src.use_cases.arango_to_mysql import ArangoToMySqlConverter

arango_credentials_file = "./src/use_cases/secrets/ifxdev_arangodb.yaml"
mysql_credentials_file = "./src/use_cases/secrets/galeradev_write.yaml"
minio_credentials_file = "./src/use_cases/secrets/ifxdev_minio.yacaml"

# arango_credentials_file = "./src/use_cases/secrets/local_arangodb.yaml"
# mysql_credentials_file = "./src/use_cases/secrets/local_mysql.yaml"
# minio_credentials_file = "./src/use_cases/secrets/local_minio.yaml"

with open(arango_credentials_file, "r") as file:
    arango_credentials = DBCredentials.from_yaml(yaml.safe_load(file))

with open(mysql_credentials_file, "r") as file:
    mysql_credentials = DBCredentials.from_yaml(yaml.safe_load(file))

with open(minio_credentials_file, "r") as file:
    minio_credentials = DBCredentials.from_yaml(yaml.safe_load(file))

conv = ArangoToMySqlConverter(
    arango_credentials=arango_credentials,
    arango_db_name='pounce',
    mysql_credentials=mysql_credentials,
    mysql_db_name='omicsdb_dev2',
    minio_credentials=minio_credentials)

conv.convert()