import hashlib
import json
from typing import Dict, Iterable, List, Optional


MW_VALIDATION_VERSION = "component-aware-v1"
MW_FINDING_FINGERPRINT_VERSION = "mw-finding-v1"


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _finding_anchor(member_ids: List[str]) -> Optional[str]:
    """Choose a durable member that lets later stages find the reviewed clique."""
    prefix_order = {
        "KEGG.COMPOUND": 0,
        "CHEBI": 1,
        "HMDB": 2,
        "PUBCHEM.COMPOUND": 3,
        "REFMET": 4,
    }
    ordered = sorted(
        set(member_ids),
        key=lambda identifier: (
            prefix_order.get(identifier.rsplit(":", 1)[0], 99),
            identifier,
        ),
    )
    return ordered[0] if ordered else None


def mass_validation_finding_metadata(
    member_ids: List[str],
    profiles_by_id: Dict[str, dict],
    assessment: dict,
    *,
    spread_threshold: float,
    absolute_match_tolerance: float = 0.1,
    relative_match_tolerance: float = 0.001,
) -> dict:
    """Build an exact, order-independent identity for one saved MW finding.

    Membership is deliberately part of the reviewed evidence. Any identifier
    joining or leaving the clique therefore makes a published acceptance stale.
    """
    ordered_members = sorted(set(member_ids))
    finding_identity = {
        "check": "mw_spread",
        "validator_version": MW_VALIDATION_VERSION,
        "member_ids": ordered_members,
    }
    evidence = {
        "fingerprint_version": MW_FINDING_FINGERPRINT_VERSION,
        **finding_identity,
        "spread_threshold": spread_threshold,
        "absolute_match_tolerance": absolute_match_tolerance,
        "relative_match_tolerance": relative_match_tolerance,
        "profiles": {
            member_id: {
                "whole": {
                    channel: sorted(set(
                        float(value)
                        for value in (profiles_by_id.get(member_id, {}).get("whole", {}).get(channel) or [])
                        if value is not None
                    ))
                    for channel in ("average", "monoisotopic")
                },
                "whole_observations": sorted(
                    [
                        {
                            key: observation.get(key)
                            for key in (
                                "source", "source_id", "channel", "value",
                                "molecular_formula",
                            )
                        }
                        for observation in (
                            profiles_by_id.get(member_id, {}).get("whole_observations") or []
                        )
                    ],
                    key=_canonical_json,
                ),
                "components": sorted(
                    [
                        {
                            key: component.get(key)
                            for key in (
                                "source", "source_id", "component_index", "average",
                                "monoisotopic", "molecular_formula", "smiles",
                            )
                        }
                        for component in (profiles_by_id.get(member_id, {}).get("components") or [])
                    ],
                    key=_canonical_json,
                ),
            }
            for member_id in ordered_members
        },
        "channel_results": assessment.get("channel_results") or {},
        "component_matches": sorted(
            assessment.get("component_matches") or [], key=_canonical_json
        ),
    }
    finding_digest = hashlib.sha256(
        _canonical_json(finding_identity).encode("utf-8")
    ).hexdigest()
    evidence_digest = hashlib.sha256(
        _canonical_json(evidence).encode("utf-8")
    ).hexdigest()
    return {
        "finding_id": f"mw-{finding_digest[:24]}",
        "anchor_id": _finding_anchor(ordered_members),
        "member_ids": ordered_members,
        "evidence_fingerprint": evidence_digest,
        "evidence_fingerprint_version": MW_FINDING_FINGERPRINT_VERSION,
    }


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


def cluster_mass_observations(
    observations: List[dict],
    *,
    absolute_match_tolerance: float = 0.1,
    relative_match_tolerance: float = 0.001,
) -> List[List[dict]]:
    """Cluster same-channel observations using the validator's match tolerance."""
    clusters: List[List[dict]] = []
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
    return clusters


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
        clusters = cluster_mass_observations(
            observations,
            absolute_match_tolerance=absolute_match_tolerance,
            relative_match_tolerance=relative_match_tolerance,
        )

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
