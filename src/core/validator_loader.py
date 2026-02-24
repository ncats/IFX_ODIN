from typing import List

import yaml

from src.core.config import create_object_from_config
from src.core.validator import AllowedValuesValidator, RequiredValidator, Validator

BUILT_IN_RULES = {
    "required": RequiredValidator,
    "allowed_values": AllowedValuesValidator,
}


def load_validators(yaml_file_or_dict) -> List[Validator]:
    if not isinstance(yaml_file_or_dict, dict):
        with open(yaml_file_or_dict) as f:
            yaml_file_or_dict = yaml.safe_load(f)
    config = yaml_file_or_dict

    validators = []
    for entity, value in config.items():
        if entity == "cross_entity":
            # Custom validators that span multiple entities — loaded via import/class
            for entry in value:
                if "import" in entry:
                    validators.append(create_object_from_config(entry))
            continue

        for field_name, rules in value.items():
            for rule_config in rules:
                rule_type = rule_config.get("rule")
                if rule_type and rule_type in BUILT_IN_RULES:
                    cls = BUILT_IN_RULES[rule_type]
                    kwargs = {k: v for k, v in rule_config.items() if k != "rule"}
                    validators.append(cls(entity=entity, field=field_name, **kwargs))
                elif "import" in rule_config:
                    validators.append(create_object_from_config(rule_config))

    return validators