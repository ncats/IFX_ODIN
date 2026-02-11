from dataclasses import dataclass


@dataclass
class DBCredentials:
    url: str
    user: str
    password: str
    port: int = None
    schema: str = None
    internal_url: str = None

    @staticmethod
    def from_yaml(yaml_dict: dict):
        return DBCredentials(
            url=yaml_dict['url'],
            user=yaml_dict['user'],
            password=yaml_dict['password'],
            port=yaml_dict.get('port'),
            schema=yaml_dict.get('schema'),
            internal_url=yaml_dict.get('internal_url')
        )