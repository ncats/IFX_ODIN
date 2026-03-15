import json
from pathlib import Path

import pandas as pd


def _safe_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(dtype=str)
    return df[col].fillna("").astype(str)


def compute_dataframe_diff(
    old_df: pd.DataFrame,
    new_df: pd.DataFrame,
    id_col: str,
    label_col: str | None = None,
    compare_cols: list[str] | None = None,
) -> dict:
    """
    Compare two DataFrames and return:
      - added_ids
      - removed_ids
      - label_changes (optional)
      - field_changes (optional)
    """
    results = {}

    old_ids = set(_safe_series(old_df, id_col))
    new_ids = set(_safe_series(new_df, id_col))

    old_ids.discard("")
    new_ids.discard("")

    results["added_ids"] = sorted(list(new_ids - old_ids))
    results["removed_ids"] = sorted(list(old_ids - new_ids))

    common_ids = sorted(old_ids & new_ids)

    if label_col and label_col in old_df.columns and label_col in new_df.columns:
        old_map = old_df.set_index(id_col)[label_col].fillna("").astype(str).to_dict()
        new_map = new_df.set_index(id_col)[label_col].fillna("").astype(str).to_dict()

        label_changes = []
        for key in common_ids:
            if old_map.get(key, "") != new_map.get(key, ""):
                label_changes.append({
                    id_col: key,
                    "old": old_map.get(key, ""),
                    "new": new_map.get(key, "")
                })
        results["label_changes"] = label_changes

    if compare_cols:
        field_changes = []
        old_idx = old_df.set_index(id_col)
        new_idx = new_df.set_index(id_col)

        for key in common_ids:
            for col in compare_cols:
                if col not in old_idx.columns or col not in new_idx.columns:
                    continue
                old_val = str(old_idx.at[key, col]) if key in old_idx.index else ""
                new_val = str(new_idx.at[key, col]) if key in new_idx.index else ""
                old_val = "" if old_val == "nan" else old_val
                new_val = "" if new_val == "nan" else new_val

                if old_val != new_val:
                    field_changes.append({
                        id_col: key,
                        "field": col,
                        "old": old_val,
                        "new": new_val
                    })

        results["field_changes"] = field_changes

    return results


def write_diff_json(diff: dict, output_file: Path):
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w") as f:
        json.dump(diff, f, indent=2)