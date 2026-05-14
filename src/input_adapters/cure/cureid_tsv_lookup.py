import csv
from collections import defaultdict
from typing import Dict, List, Tuple


def normalize_cureid_label(value) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        value = str(value)
    normalized = " ".join(value.split()).strip()
    return normalized or None


def load_cureid_label_lookup(tsv_file: str) -> Dict[str, List[Tuple[str, str]]]:
    label_map: Dict[str, set[Tuple[str, str]]] = defaultdict(set)
    with open(tsv_file, newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            for side in ("subject", "object"):
                label = normalize_cureid_label(row.get(f"{side}_label_original"))
                final_label = normalize_cureid_label(row.get(f"{side}_final_label"))
                curie = normalize_cureid_label(row.get(f"{side}_final_curie"))
                if label is None or curie is None:
                    continue
                label_map[label].add((curie, final_label or label))
    return {
        label: sorted(matches, key=lambda item: (item[0], item[1]))
        for label, matches in label_map.items()
    }
