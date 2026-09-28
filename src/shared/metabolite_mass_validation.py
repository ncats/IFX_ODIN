from typing import Dict, Iterable, List, Optional


MW_VALIDATION_VERSION = "component-aware-v1"


def _summary(values: Iterable[float]) -> Optional[dict]:
    ordered = sorted(float(value) for value in values if value is not None and value > 0)
    if not ordered:
        return None
    midpoint = len(ordered) // 2
    median = (
        ordered[midpoint]
        if len(ordered) % 2
        else (ordered[midpoint - 1] + ordered[midpoint]) / 2
    )
    return {
        "min": ordered[0],
        "max": ordered[-1],
        "median": median,
        "count": len(ordered),
    }


def _spread(summary: Optional[dict]) -> Optional[float]:
    if not summary or summary["min"] <= 0:
        return None
    return (summary["max"] - summary["min"]) / summary["min"]


def _close(left: float, right: float, absolute_tolerance: float, relative_tolerance: float) -> bool:
    return abs(left - right) <= max(
        absolute_tolerance,
        relative_tolerance * max(abs(left), abs(right)),
    )


def assess_mass_profiles(
    member_ids: List[str],
    profiles_by_id: Dict[str, dict],
    *,
    spread_threshold: float,
    absolute_match_tolerance: float = 0.1,
    relative_match_tolerance: float = 0.001,
) -> Optional[dict]:
    """Assess one clique without comparing average and monoisotopic channels."""
    channel_results = {}
    problematic_channels = []
    for channel in ("average", "monoisotopic"):
        observations = [
            {
                "member_id": member_id,
                "value": float(value),
            }
            for member_id in member_ids
            for value in (profiles_by_id.get(member_id, {}).get("whole", {}).get(channel) or [])
            if value is not None and float(value) > 0
        ]
        summary = _summary(item["value"] for item in observations)
        spread = _spread(summary)
        channel_results[channel] = {
            "summary": summary,
            "spread": spread,
            "spread_percent": spread * 100 if spread is not None else None,
        }
        if spread is not None and spread > spread_threshold:
            problematic_channels.append((channel, observations))

    if not problematic_channels:
        return None

    component_matches = []
    rescued_channels = set()
    for channel, observations in problematic_channels:
        clusters = []
        for observation in sorted(observations, key=lambda item: item["value"]):
            cluster_index = next((
                index
                for index, cluster in enumerate(clusters)
                if _close(
                    observation["value"],
                    cluster[0]["value"],
                    absolute_match_tolerance,
                    relative_match_tolerance,
                )
            ), None)
            if cluster_index is None:
                clusters.append([observation])
                cluster_index = len(clusters) - 1
            else:
                clusters[cluster_index].append(observation)
            observation["cluster_index"] = cluster_index

        explained_clusters = set()
        seen_matches = set()
        for whole in observations:
            for component_owner in observations:
                if whole["cluster_index"] == component_owner["cluster_index"]:
                    continue
                for component in profiles_by_id.get(component_owner["member_id"], {}).get("components", []):
                    component_value = component.get(channel)
                    if component_value is None or not _close(
                        whole["value"],
                        float(component_value),
                        absolute_match_tolerance,
                        relative_match_tolerance,
                    ):
                        continue
                    match_key = (
                        channel,
                        whole["cluster_index"],
                        component_owner["cluster_index"],
                        whole["member_id"],
                        component_owner["member_id"],
                        component.get("component_index"),
                    )
                    if match_key in seen_matches:
                        continue
                    seen_matches.add(match_key)
                    explained_clusters.update({
                        whole["cluster_index"], component_owner["cluster_index"]
                    })
                    component_matches.append({
                        "channel": channel,
                        "whole_member_id": whole["member_id"],
                        "whole_mass": whole["value"],
                        "component_member_id": component_owner["member_id"],
                        "component_index": component.get("component_index"),
                        "component_mass": float(component_value),
                        "component_formula": component.get("molecular_formula"),
                        "component_smiles": component.get("smiles"),
                    })
        channel_results[channel]["mass_cluster_count"] = len(clusters)
        channel_results[channel]["explained_mass_cluster_count"] = len(explained_clusters)
        if len(clusters) == len(explained_clusters):
            rescued_channels.add(channel)

    problem_channel_names = {channel for channel, _observations in problematic_channels}
    severity = "warning" if problem_channel_names <= rescued_channels else "error"
    return {
        "severity": severity,
        "reason": "component_mass_match" if severity == "warning" else "unexplained_mass_difference",
        "mass_summary": None,
        "mass_summaries": {
            channel: result["summary"]
            for channel, result in channel_results.items()
        },
        "channel_results": channel_results,
        "component_matches": component_matches,
    }
