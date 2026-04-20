"""
Reusable step functions for the SnapStart version cleaner.

Imported by both:
- POC.py                  (local read-only CLI)
- infra/src/orchestrator.py (Lambda Durable Function orchestrator)

Every function is pure (takes explicit boto3 clients / config dicts, returns
plain dicts/lists) so it plugs into context.step(lambda _: step_fn(...),
name=...) inside the durable orchestrator without further wrapping.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError


# ---------------------------------------------------------------------------
# Config dataclass-ish helpers
# ---------------------------------------------------------------------------
DEFAULTS = {
    "lookback_days": 30,
    "min_age_days": 14,
    "keep_last_n": 3,
    "period_seconds": 3600,
    "function_name_prefix": None,
    "s3_report_uri": None,
    # Approval-notification wiring. When any of these are None the step falls
    # back to log-only behavior (useful for the local CLI in POC.py).
    "approval_topic_arn": None,
    "approval_bundle_bucket": None,
    "approval_resolver_url": None,
}


def normalize_config(cfg: dict[str, Any]) -> dict[str, Any]:
    """Fill defaults; required: cfg['region']."""
    if not cfg.get("region"):
        raise ValueError("config.region is required")
    merged = {**DEFAULTS, **cfg}
    return merged


def build_clients(region: str):
    """
    Returns (lambda_client, cloudwatch_client, s3_client, sns_client).
    Kept positional so existing callers that unpack the first three keep
    working; the SNS client is additive for the approval-email step.
    """
    session = boto3.Session(region_name=region)
    return (
        session.client("lambda"),
        session.client("cloudwatch"),
        session.client("s3"),
        session.client("sns"),
    )


# ---------------------------------------------------------------------------
# Step 1 - Discover SnapStart-enabled published versions
# ---------------------------------------------------------------------------
def step1_discover_snapstart(lambda_client, cfg: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Single paginated ListFunctions(FunctionVersion="ALL"), filtered inline to
    published versions that were configured with SnapStart at publish time
    (SnapStart.ApplyOn == "PublishedVersions").

    We intentionally do NOT gate on OptimizationStatus == "On" here. Lambda
    flips OptimizationStatus to "Off" on two classes of version that are
    still legitimate cleanup targets:

      - Idle versions: after ~14 days without invocations Lambda drops the
        cached snapshot and sets OptimizationStatus="Off". The version still
        exists, still clutters the version list, and is safe to delete.
      - Failed versions: SnapStart pre-snapshot init raised, the version
        landed in State=Failed, OptimizationStatus never became "On" (or
        was flipped Off). These versions can never be invoked and are
        dead weight.

    The ListFunctions response already contains the SnapStart dict per row, so
    we do not need a GetFunctionConfiguration round-trip just to know which
    versions were SnapStart-configured. State is NOT populated in this response
    (verified in us-west-2), so step2_confirm_active still re-fetches for the
    kept subset to apply the deletable-state filter.
    """
    name_prefix = cfg.get("function_name_prefix")
    paginator = lambda_client.get_paginator("list_functions")
    kept: list[dict[str, Any]] = []

    for page in paginator.paginate(FunctionVersion="ALL"):
        for fn in page.get("Functions", []):
            if fn.get("Version") == "$LATEST":
                continue
            if name_prefix and not fn.get("FunctionName", "").startswith(name_prefix):
                continue
            snap = fn.get("SnapStart") or {}
            if snap.get("ApplyOn") != "PublishedVersions":
                continue
            kept.append(
                {
                    "FunctionName": fn.get("FunctionName"),
                    "Version": fn.get("Version"),
                    "Runtime": fn.get("Runtime"),
                    "LastModified": fn.get("LastModified"),
                    "FunctionArn": fn.get("FunctionArn"),
                    "SnapStartOptimizationStatus": snap.get("OptimizationStatus"),
                    "SnapStartApplyOn": snap.get("ApplyOn"),
                }
            )

    return kept


# ---------------------------------------------------------------------------
# Step 2 - Confirm terminal deletable state per SnapStart candidate
#          (Active | Inactive | Failed)
# ---------------------------------------------------------------------------
DELETABLE_STATES = frozenset({"Active", "Inactive", "Failed"})


def step2_confirm_active(
    lambda_client,
    snapstart_versions: list[dict[str, Any]],
    cfg: dict[str, Any],
) -> list[dict[str, Any]]:
    """
    For each SnapStart candidate, call GetFunctionConfiguration with the
    version qualifier and keep rows in a terminal, deletable state:

      Active    - normal; in use or recently invoked.
      Inactive  - idle >14d; Lambda already dropped the snapshot. Still
                  deletable and the most common post-SnapStart-rollout
                  cleanup target.
      Failed    - init-time error (SnapStart pre-snapshot failure, etc).
                  Can never be invoked, can never self-recover.

    Pending and PendingDelete are dropped because DeleteFunction races with
    Lambda's own state machine for those.

    The step name is kept as step2_confirm_active for backwards
    compatibility with existing durable-execution checkpoints; the
    behaviour is now "confirm terminal state".
    """
    _ = cfg  # unused; kept to match the orchestrator signature
    kept: list[dict[str, Any]] = []
    for v in snapstart_versions:
        function_name = v["FunctionName"]
        version = v["Version"]
        try:
            cfg_resp = lambda_client.get_function_configuration(
                FunctionName=function_name, Qualifier=version
            )
        except ClientError as exc:
            print(
                f"[warn] GetFunctionConfiguration failed for "
                f"{function_name}:{version}: {exc}",
                file=sys.stderr,
            )
            continue
        state = cfg_resp.get("State")
        if state not in DELETABLE_STATES:
            continue
        enriched = dict(v)
        enriched["State"] = state
        enriched["StateReasonCode"] = cfg_resp.get("StateReasonCode")
        enriched["StateReason"] = cfg_resp.get("StateReason")
        enriched["LastModified"] = cfg_resp.get("LastModified") or v.get("LastModified")
        kept.append(enriched)
    return kept


# ---------------------------------------------------------------------------
# Stage A - age + keep-last-N
# ---------------------------------------------------------------------------
def _parse_last_modified(value: str | None) -> datetime | None:
    """Lambda returns LastModified as ISO-8601 with a timezone offset."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def step3_apply_stage_a(
    versions: list[dict[str, Any]],
    cfg: dict[str, Any],
) -> list[dict[str, Any]]:
    """
    Stage A annotates each row (does NOT drop rows, so the final report keeps
    full visibility):

      age_days                 - float days since LastModified
      is_among_keep_last_n     - True if this version is in the newest N for
                                 its function (keep_last_n, default 3), using
                                 LastModified desc with numeric Version as
                                 tiebreaker
      stage_a_pass             - True iff age_days >= min_age_days AND
                                 is_among_keep_last_n is False

    Downstream stages decide the final candidate flag; Stage A just scores.
    """
    min_age_days = int(cfg["min_age_days"])
    keep_last_n = int(cfg["keep_last_n"])
    now = datetime.now(timezone.utc)

    grouped: dict[str, list[dict[str, Any]]] = {}
    for v in versions:
        grouped.setdefault(v["FunctionName"], []).append(v)

    def _sort_key(v: dict[str, Any]):
        lm = _parse_last_modified(v.get("LastModified")) or datetime.min.replace(
            tzinfo=timezone.utc
        )
        try:
            ver_int = int(v.get("Version") or -1)
        except ValueError:
            ver_int = -1
        return (lm, ver_int)

    kept_ids: set[tuple[str, str]] = set()
    for function_name, rows in grouped.items():
        rows_sorted = sorted(rows, key=_sort_key, reverse=True)
        for r in rows_sorted[:keep_last_n]:
            kept_ids.add((function_name, r["Version"]))

    annotated: list[dict[str, Any]] = []
    for v in versions:
        lm = _parse_last_modified(v.get("LastModified"))
        if lm is None:
            age_days = None
            age_ok = False
        else:
            age_days = (now - lm).total_seconds() / 86400.0
            age_ok = age_days >= min_age_days
        is_among_keep_last_n = (v["FunctionName"], v["Version"]) in kept_ids
        out = dict(v)
        out["age_days"] = None if age_days is None else round(age_days, 2)
        out["is_among_keep_last_n"] = is_among_keep_last_n
        out["stage_a_pass"] = bool(age_ok and not is_among_keep_last_n)
        annotated.append(out)
    return annotated


# ---------------------------------------------------------------------------
# Stage B - usage + alias protection
# ---------------------------------------------------------------------------
def _latest_nonzero_invocation_timestamp(
    cw_client,
    dimensions: list[dict[str, str]],
    lookback_days: int,
    period_seconds: int,
) -> datetime | None:
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=lookback_days)
    resp = cw_client.get_metric_statistics(
        Namespace="AWS/Lambda",
        MetricName="Invocations",
        Dimensions=dimensions,
        StartTime=start,
        EndTime=end,
        Period=period_seconds,
        Statistics=["Sum"],
    )
    nz = [dp for dp in resp.get("Datapoints", []) if dp.get("Sum", 0) > 0]
    if not nz:
        return None
    return max(dp["Timestamp"] for dp in nz)


def _list_aliases(
    lambda_client, function_name: str
) -> dict[str, list[dict[str, Any]]]:
    paginator = lambda_client.get_paginator("list_aliases")
    version_to_refs: dict[str, list[dict[str, Any]]] = {}
    for page in paginator.paginate(FunctionName=function_name):
        for alias in page.get("Aliases", []):
            alias_name = alias.get("Name")
            primary_version = alias.get("FunctionVersion")
            routing = alias.get("RoutingConfig") or {}
            additional = routing.get("AdditionalVersionWeights") or {}
            additional_total = sum(float(w) for w in additional.values())
            primary_weight = max(0.0, 1.0 - additional_total)
            version_to_refs.setdefault(primary_version, []).append(
                {
                    "alias_name": alias_name,
                    "reference_type": "primary",
                    "weight": primary_weight,
                }
            )
            for weighted_version, weight in additional.items():
                version_to_refs.setdefault(weighted_version, []).append(
                    {
                        "alias_name": alias_name,
                        "reference_type": "weighted",
                        "weight": float(weight),
                    }
                )
    return version_to_refs


def step4_usage_and_alias_check(
    lambda_client,
    cw_client,
    versions: list[dict[str, Any]],
    cfg: dict[str, Any],
) -> list[dict[str, Any]]:
    """
    For each version annotate:
      version_last_invoked_utc         - Resource=<fn>:<ver>  (per-qualifier)
      function_level_last_invoked_utc  - FunctionName=<fn>    (aggregate fallback)
      aliases[]                        - alias references + per-alias last-invoked
      protected_by_alias               - True if any alias references this version
      stage_b_pass                     - True iff no aliases AND no per-version
                                         activity AND function-level activity
                                         is either absent or older than
                                         min_age_days

    function_level_last_invoked_utc acts as the safety-net signal: when an
    account doesn't publish per-qualifier Resource metrics (dev accounts
    invoking unqualified ARNs), we still get a coarse "is this function
    alive?" check so we don't recommend deleting a busy function's old
    version purely because CW per-qualifier data is missing.
    """
    lookback_days = int(cfg["lookback_days"])
    min_age_days = int(cfg["min_age_days"])
    period_seconds = int(cfg["period_seconds"])

    function_level_cache: dict[str, datetime | None] = {}
    aliases_cache: dict[str, dict[str, list[dict[str, Any]]]] = {}

    now = datetime.now(timezone.utc)
    out: list[dict[str, Any]] = []

    for v in versions:
        function_name = v["FunctionName"]
        version = v["Version"]

        try:
            version_last = _latest_nonzero_invocation_timestamp(
                cw_client,
                [{"Name": "Resource", "Value": f"{function_name}:{version}"}],
                lookback_days,
                period_seconds,
            )
        except ClientError as exc:
            print(
                f"[warn] CW version query failed for {function_name}:{version}: {exc}",
                file=sys.stderr,
            )
            version_last = None

        if function_name not in function_level_cache:
            try:
                function_level_cache[function_name] = _latest_nonzero_invocation_timestamp(
                    cw_client,
                    [{"Name": "FunctionName", "Value": function_name}],
                    lookback_days,
                    period_seconds,
                )
            except ClientError as exc:
                print(
                    f"[warn] CW function-level query failed for {function_name}: {exc}",
                    file=sys.stderr,
                )
                function_level_cache[function_name] = None
        function_last = function_level_cache[function_name]

        if function_name not in aliases_cache:
            try:
                aliases_cache[function_name] = _list_aliases(lambda_client, function_name)
            except ClientError as exc:
                print(
                    f"[warn] ListAliases failed for {function_name}: {exc}",
                    file=sys.stderr,
                )
                aliases_cache[function_name] = {}
        alias_refs_raw = list(aliases_cache[function_name].get(version, []))

        alias_refs: list[dict[str, Any]] = []
        for ref in alias_refs_raw:
            try:
                alias_last = _latest_nonzero_invocation_timestamp(
                    cw_client,
                    [
                        {
                            "Name": "Resource",
                            "Value": f"{function_name}:{ref['alias_name']}",
                        }
                    ],
                    lookback_days,
                    period_seconds,
                )
            except ClientError as exc:
                print(
                    f"[warn] CW alias query failed for "
                    f"{function_name}:{ref['alias_name']}: {exc}",
                    file=sys.stderr,
                )
                alias_last = None
            alias_refs.append({**ref, "alias_last_invoked_utc": _iso(alias_last)})

        protected_by_alias = bool(alias_refs)
        version_has_activity = version_last is not None
        function_level_recent = bool(
            function_last and (now - function_last).total_seconds() / 86400.0 < min_age_days
        )
        stage_b_pass = (
            (not protected_by_alias)
            and (not version_has_activity)
            and (not function_level_recent)
        )

        enriched = dict(v)
        enriched["version_last_invoked_utc"] = _iso(version_last)
        enriched["function_level_last_invoked_utc"] = _iso(function_last)
        enriched["aliases"] = alias_refs
        enriched["protected_by_alias"] = protected_by_alias
        enriched["stage_b_pass"] = stage_b_pass
        out.append(enriched)
    return out


# ---------------------------------------------------------------------------
# Reason flags + candidate_for_deletion assembly
# ---------------------------------------------------------------------------
def _reason_flags(row: dict[str, Any], min_age_days: int) -> list[str]:
    flags: list[str] = []
    if row.get("age_days") is not None and row["age_days"] < min_age_days:
        flags.append("too_recent")
    if row.get("is_among_keep_last_n"):
        flags.append("protected_kept_last_n")
    if row.get("protected_by_alias"):
        flags.append("alias_protected")
    if row.get("version_last_invoked_utc"):
        flags.append("has_recent_invocations")
    if (
        not row.get("version_last_invoked_utc")
        and row.get("function_level_last_invoked_utc")
        and not row.get("stage_b_pass")
    ):
        flags.append("function_level_activity_uncertain")
    if row.get("stage_a_pass") and row.get("stage_b_pass"):
        flags.append("no_activity_and_stale")
    return flags


def step5_build_report(
    scored_rows: list[dict[str, Any]], cfg: dict[str, Any]
) -> dict[str, Any]:
    """
    Final shape: list of per-version report objects plus a summary block.
    This is the object handed to the approval reviewer and (later) to the
    delete step.
    """
    min_age_days = int(cfg["min_age_days"])
    rows: list[dict[str, Any]] = []
    stage_a_pass = stage_b_pass = candidates = alias_protected = 0

    for r in scored_rows:
        flags = _reason_flags(r, min_age_days)
        candidate = bool(r.get("stage_a_pass") and r.get("stage_b_pass"))
        if r.get("stage_a_pass"):
            stage_a_pass += 1
        if r.get("stage_b_pass"):
            stage_b_pass += 1
        if candidate:
            candidates += 1
        if r.get("protected_by_alias"):
            alias_protected += 1
        rows.append(
            {
                "function_name": r["FunctionName"],
                "version": r["Version"],
                "runtime": r.get("Runtime"),
                "state": r.get("State"),
                "state_reason_code": r.get("StateReasonCode"),
                "state_reason": r.get("StateReason"),
                "snapstart_optimization_status": r.get("SnapStartOptimizationStatus"),
                "snapstart_apply_on": r.get("SnapStartApplyOn"),
                "last_modified": r.get("LastModified"),
                "age_days": r.get("age_days"),
                "is_among_keep_last_n": r.get("is_among_keep_last_n"),
                "version_last_invoked_utc": r.get("version_last_invoked_utc"),
                "function_level_last_invoked_utc": r.get("function_level_last_invoked_utc"),
                "aliases": r.get("aliases") or [],
                "protected_by_alias": r.get("protected_by_alias", False),
                "stage_a_pass": r.get("stage_a_pass", False),
                "stage_b_pass": r.get("stage_b_pass", False),
                "candidate_for_deletion": candidate,
                "reason_flags": flags,
            }
        )

    state_counts: dict[str, int] = {}
    for r in scored_rows:
        s = r.get("State") or "Unknown"
        state_counts[s] = state_counts.get(s, 0) + 1

    return {
        "summary": {
            "total_deletable_state_versions": len(scored_rows),
            "state_breakdown": state_counts,
            "stage_a_pass": stage_a_pass,
            "stage_b_pass": stage_b_pass,
            "candidates_for_deletion": candidates,
            "alias_protected": alias_protected,
            "scan_config": {
                "region": cfg["region"],
                "lookback_days": cfg["lookback_days"],
                "min_age_days": cfg["min_age_days"],
                "keep_last_n": cfg["keep_last_n"],
                "period_seconds": cfg["period_seconds"],
                "function_name_prefix": cfg.get("function_name_prefix"),
            },
        },
        "rows": rows,
    }


# ---------------------------------------------------------------------------
# Publish + approval + delete (used by the durable orchestrator)
# ---------------------------------------------------------------------------
def step5_publish_report(
    s3_client, report: dict[str, Any], cfg: dict[str, Any]
) -> dict[str, Any]:
    """
    Return a report_location dict. If --s3-report-uri is configured, upload
    a JSON file and return its s3:// URI; otherwise return {"inline": report}
    so the orchestrator can pass it to the approval notification as-is.
    """
    uri = cfg.get("s3_report_uri")
    if not uri:
        return {"inline": report}
    if not uri.startswith("s3://"):
        raise ValueError(f"s3_report_uri must be s3://..., got: {uri}")
    without_scheme = uri[len("s3://") :]
    bucket, _, prefix = without_scheme.partition("/")
    prefix = prefix.rstrip("/")
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    key = f"{prefix + '/' if prefix else ''}snapstart-candidates-{ts}.json"
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(report, default=str, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    return {"s3_uri": f"s3://{bucket}/{key}"}


# ---------------------------------------------------------------------------
# Durable-step wrappers (S3-backed intermediates)
# ---------------------------------------------------------------------------
# Durable function step checkpoints are capped at 256 KiB. Running the full
# scan pipeline (ListFunctions + per-version GetFunctionConfiguration +
# aliases + CloudWatch metrics) through successive checkpoints blows that
# cap once a region has a few hundred SnapStart versions.
#
# The wrappers below solve that without collapsing the pipeline into one
# opaque step:
#   - each phase persists its bulky intermediate row list to
#     s3://<bundle_bucket>/intermediate/<execution_id>/<NN-phase>.json
#   - each phase returns a small "handle" dict containing only the bucket,
#     key, and a few counts -- well under the checkpoint cap
#   - the next phase receives that handle, loads the blob back from S3,
#     runs the existing step1..step5 pure function, and writes its own blob
#
# Idempotency: the S3 keys are deterministic per (bundle_bucket,
# execution_id), so a step retry overwrites the same key with the same body.
# The durable SDK takes care of not re-invoking a step whose checkpoint
# already succeeded; this only matters for in-step retries.


def _intermediate_key(execution_id: str, phase: str) -> str:
    return f"intermediate/{execution_id}/{phase}.json"


def _put_intermediate(
    s3_client, bucket: str, execution_id: str, phase: str, payload: Any
) -> dict[str, Any]:
    key = _intermediate_key(execution_id, phase)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, default=str).encode("utf-8"),
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )
    return {"bucket": bucket, "key": key}


def _get_intermediate(s3_client, handle: dict[str, Any]) -> Any:
    bucket = handle["bucket"]
    key = handle["key"]
    resp = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(resp["Body"].read().decode("utf-8"))


def step_discover_snapstart(
    lambda_client,
    s3_client,
    cfg: dict[str, Any],
    bundle_bucket: str,
    execution_id: str,
) -> dict[str, Any]:
    """Phase 1: paginate ListFunctions and keep SnapStart-enabled rows."""
    rows = step1_discover_snapstart(lambda_client, cfg)
    handle = _put_intermediate(s3_client, bundle_bucket, execution_id, "01-snapstart", rows)
    return {**handle, "count": len(rows)}


def step_confirm_active(
    lambda_client,
    s3_client,
    discover_handle: dict[str, Any],
    cfg: dict[str, Any],
    bundle_bucket: str,
    execution_id: str,
) -> dict[str, Any]:
    """Phase 2: GetFunctionConfiguration per candidate, keep State in {Active, Inactive, Failed}."""
    snapstart_rows = _get_intermediate(s3_client, discover_handle)
    active = step2_confirm_active(lambda_client, snapstart_rows, cfg)
    handle = _put_intermediate(s3_client, bundle_bucket, execution_id, "02-active", active)
    return {**handle, "count": len(active)}


def step_apply_stage_a(
    s3_client,
    active_handle: dict[str, Any],
    cfg: dict[str, Any],
    bundle_bucket: str,
    execution_id: str,
) -> dict[str, Any]:
    """Phase 3: annotate with age_days / keep_last_n / stage_a_pass."""
    active = _get_intermediate(s3_client, active_handle)
    annotated = step3_apply_stage_a(active, cfg)
    handle = _put_intermediate(s3_client, bundle_bucket, execution_id, "03-stage-a", annotated)
    stage_a_pass = sum(1 for r in annotated if r.get("stage_a_pass"))
    return {**handle, "count": len(annotated), "stage_a_pass": stage_a_pass}


def step_usage_and_alias_check(
    lambda_client,
    cw_client,
    s3_client,
    stage_a_handle: dict[str, Any],
    cfg: dict[str, Any],
    bundle_bucket: str,
    execution_id: str,
) -> dict[str, Any]:
    """Phase 4: CloudWatch invocations + alias protection (step4 logic)."""
    annotated = _get_intermediate(s3_client, stage_a_handle)
    scored = step4_usage_and_alias_check(lambda_client, cw_client, annotated, cfg)
    handle = _put_intermediate(s3_client, bundle_bucket, execution_id, "04-stage-b", scored)
    stage_b_pass = sum(1 for r in scored if r.get("stage_b_pass"))
    alias_protected = sum(1 for r in scored if r.get("protected_by_alias"))
    return {
        **handle,
        "count": len(scored),
        "stage_b_pass": stage_b_pass,
        "alias_protected": alias_protected,
    }


def step_build_and_upload_report(
    s3_client,
    stage_b_handle: dict[str, Any],
    cfg: dict[str, Any],
    bundle_bucket: str,
    execution_id: str,
) -> dict[str, Any]:
    """
    Phase 5: build the final report dict, upload to reports/<execution_id>.json,
    and return the small handle downstream steps + approval email consume.

    Returned handle is bounded by candidate count * ~200 bytes (typically a
    few KB), not the full row list -- the full report stays only in S3.
    """
    scored = _get_intermediate(s3_client, stage_b_handle)
    report = step5_build_report(scored, cfg)

    key = f"reports/{execution_id}.json"
    s3_client.put_object(
        Bucket=bundle_bucket,
        Key=key,
        Body=json.dumps(report, default=str).encode("utf-8"),
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )

    rows = report.get("rows", [])
    candidates = [
        {
            "function_name": r["function_name"],
            "version": r["version"],
            "age_days": r.get("age_days"),
            "last_modified": r.get("last_modified"),
            "function_level_last_invoked_utc": r.get("function_level_last_invoked_utc"),
        }
        for r in rows
        if r.get("candidate_for_deletion")
    ]

    return {
        "report_bucket": bundle_bucket,
        "report_key": key,
        "report_s3_uri": f"s3://{bundle_bucket}/{key}",
        "summary": report["summary"],
        "candidates": candidates,
    }


def run_scan_and_upload_report(
    lambda_client,
    cw_client,
    s3_client,
    cfg: dict[str, Any],
    bundle_bucket: str,
    execution_id: str,
) -> dict[str, Any]:
    """
    Back-compat single-call scan pipeline. Kept for POC.py's local CLI path
    (which doesn't go through the durable orchestrator, so it doesn't need
    per-phase checkpoints). The Lambda orchestrator now chains step_* wrappers
    above for per-phase visibility in the Durable Operations panel.
    """
    snapstart = step1_discover_snapstart(lambda_client, cfg)
    active = step2_confirm_active(lambda_client, snapstart, cfg)
    staged_a = step3_apply_stage_a(active, cfg)
    scored = step4_usage_and_alias_check(lambda_client, cw_client, staged_a, cfg)
    report = step5_build_report(scored, cfg)

    key = f"reports/{execution_id}.json"
    s3_client.put_object(
        Bucket=bundle_bucket,
        Key=key,
        Body=json.dumps(report, default=str).encode("utf-8"),
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )

    rows = report.get("rows", [])
    candidates = [
        {
            "function_name": r["function_name"],
            "version": r["version"],
            "age_days": r.get("age_days"),
            "last_modified": r.get("last_modified"),
            "function_level_last_invoked_utc": r.get("function_level_last_invoked_utc"),
        }
        for r in rows
        if r.get("candidate_for_deletion")
    ]

    return {
        "report_bucket": bundle_bucket,
        "report_key": key,
        "report_s3_uri": f"s3://{bundle_bucket}/{key}",
        "summary": report["summary"],
        "candidates": candidates,
    }


def _format_approval_email(
    scan_result: dict[str, Any],
    approve_url: str,
    reject_url: str,
    cfg: dict[str, Any],
) -> tuple[str, str]:
    """
    Build (subject, body) for the SNS email. SNS email delivery is always
    text/plain, so we emit a plain-text message whose URLs most mail clients
    will auto-linkify. Candidate rows are truncated to a readable slice;
    the full report lives at scan_result["report_s3_uri"].
    """
    summary = scan_result.get("summary", {})
    candidates = scan_result.get("candidates", [])

    scan_cfg = summary.get("scan_config", {})
    subject = (
        f"[SnapStart Cleaner] Approval needed -- "
        f"{len(candidates)} version(s) in {scan_cfg.get('region', cfg.get('region'))}"
    )

    lines: list[str] = []
    lines.append(
        "A SnapStart Lambda version scan finished and is waiting on your approval."
    )
    lines.append("")
    lines.append("=== SCAN CONFIG ===")
    for k, v in scan_cfg.items():
        lines.append(f"  {k}: {v}")
    lines.append("")
    lines.append("=== SUMMARY ===")
    lines.append(
        f"  Total SnapStart versions scanned        : "
        f"{summary.get('total_deletable_state_versions', summary.get('total_snapstart_active_versions', 0))}"
    )
    state_breakdown = summary.get("state_breakdown") or {}
    if state_breakdown:
        parts = ", ".join(f"{s}={n}" for s, n in sorted(state_breakdown.items()))
        lines.append(f"  State breakdown                         : {parts}")
    lines.append(f"  Stage A pass (age + keep_last_n)        : {summary.get('stage_a_pass', 0)}")
    lines.append(f"  Stage B pass (no usage, no alias)       : {summary.get('stage_b_pass', 0)}")
    lines.append(
        f"  CANDIDATES FOR DELETION (approvable)    : "
        f"{summary.get('candidates_for_deletion', 0)}"
    )
    lines.append(f"  Alias-protected (skipped)               : {summary.get('alias_protected', 0)}")
    lines.append("")

    max_rows = 40
    lines.append(f"=== CANDIDATES (showing up to {max_rows}) ===")
    for r in candidates[:max_rows]:
        lines.append(
            f"  {r['function_name']}:{r['version']}  "
            f"age={r.get('age_days')}d  "
            f"last_invoked_fn={r.get('function_level_last_invoked_utc') or 'none'}  "
            f"last_modified={r.get('last_modified')}"
        )
    if len(candidates) > max_rows:
        lines.append(f"  ... and {len(candidates) - max_rows} more (see full report).")
    lines.append("")

    report_uri = scan_result.get("report_s3_uri")
    if report_uri:
        lines.append(f"Full report: {report_uri}")
        lines.append("")

    lines.append("=== DECIDE ===")
    lines.append("Click APPROVE to delete the listed versions:")
    lines.append(f"  {approve_url}")
    lines.append("")
    lines.append("Click REJECT to cancel deletion and exit the workflow:")
    lines.append(f"  {reject_url}")
    lines.append("")
    lines.append(
        "These links are one-shot and expire when the 7-day approval window closes."
    )
    return subject, "\n".join(lines)


def step6_send_approval_notification(
    callback_id: str,
    scan_result: dict[str, Any],
    cfg: dict[str, Any],
    s3_client=None,
    sns_client=None,
) -> dict[str, Any]:
    """
    Publish the approval request.

    When cfg provides approval_topic_arn + approval_bundle_bucket +
    approval_resolver_url:
      1. Upload {callback_id, report, cfg} to
         s3://<bundle_bucket>/pending-callbacks/<callback_id>.json so the
         resolver Lambda can look up the candidate list on click.
      2. Publish a plain-text SNS message containing a summary, the top
         candidates, and two one-click URLs that hit the resolver.

    When those config keys are missing (local CLI / sam local invoke with no
    approval infra), fall back to the old log-only behavior so the durable
    execution can still be resumed manually via `sam local callback succeed`.

    Returns an observability dict so callers / step replay can see whether
    the notification went out.
    """
    topic_arn = cfg.get("approval_topic_arn")
    bucket = cfg.get("approval_bundle_bucket")
    resolver_url = cfg.get("approval_resolver_url")

    if not (topic_arn and bucket and resolver_url):
        print(
            json.dumps(
                {
                    "approval_mode": "log-only",
                    "approval_callback_id": callback_id,
                    "report_s3_uri": scan_result.get("report_s3_uri"),
                    "candidate_count": len(scan_result.get("candidates", [])),
                    "region": cfg.get("region"),
                    "hint": (
                        "Set approval_topic_arn + approval_bundle_bucket + "
                        "approval_resolver_url (or the matching env vars on "
                        "the orchestrator) to enable email-with-links flow."
                    ),
                },
                default=str,
            ),
            file=sys.stderr,
        )
        return {"mode": "log-only", "callback_id": callback_id}

    if s3_client is None or sns_client is None:
        raise ValueError(
            "step6_send_approval_notification requires s3_client and sns_client "
            "when approval_topic_arn + approval_bundle_bucket + "
            "approval_resolver_url are configured"
        )

    # Pending-callback bundle holds the exact candidate list that was shown
    # to the approver, so the resolver resumes the durable execution with
    # precisely the approved rows (not a re-queried list that might have
    # drifted). We intentionally do NOT store the whole report here -- the
    # full report is already at scan_result.report_s3_uri.
    key = f"pending-callbacks/{callback_id}.json"
    bundle = {
        "callback_id": callback_id,
        "created_utc": _iso(datetime.now(timezone.utc)),
        "region": cfg.get("region"),
        "candidates": scan_result.get("candidates", []),
        "summary": scan_result.get("summary", {}),
        "report_s3_uri": scan_result.get("report_s3_uri"),
    }
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(bundle, default=str).encode("utf-8"),
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )

    resolver_base = resolver_url.rstrip("/")
    approve_url = f"{resolver_base}/approve?cb={callback_id}"
    reject_url = f"{resolver_base}/reject?cb={callback_id}"

    subject, body = _format_approval_email(
        scan_result, approve_url, reject_url, cfg
    )
    # SNS email delivery enforces a 100-char subject limit. Trim defensively.
    if len(subject) > 100:
        subject = subject[:97] + "..."

    sns_client.publish(TopicArn=topic_arn, Subject=subject, Message=body)

    return {
        "mode": "sns",
        "callback_id": callback_id,
        "topic_arn": topic_arn,
        "bundle_s3_uri": f"s3://{bucket}/{key}",
        "approve_url": approve_url,
        "reject_url": reject_url,
    }


def step7_delete_versions(
    lambda_client,
    approved_versions: list[dict[str, Any]],
    cfg: dict[str, Any],
    dry_run: bool = True,
) -> list[dict[str, Any]]:
    """
    Delete the approved (function_name, version) pairs via
    lambda:DeleteFunction(FunctionName=<name>, Qualifier=<version>).

    POC stays with dry_run=True — it only reports what WOULD be deleted.
    Before flipping to real deletes, add:
      - lambda:ListEventSourceMappings check against the qualified ARN
      - direct-trigger audit (API Gateway integrations, EventBridge rules...)

    cfg is accepted for symmetry with the orchestrator signature; not used
    beyond logging.
    """
    _ = cfg
    results: list[dict[str, Any]] = []
    for entry in approved_versions:
        function_name = entry.get("function_name")
        version = entry.get("version")
        if not function_name or not version:
            results.append(
                {
                    "function_name": function_name,
                    "version": version,
                    "status": "skipped_invalid_entry",
                }
            )
            continue
        if dry_run:
            results.append(
                {
                    "function_name": function_name,
                    "version": version,
                    "status": "dry_run_would_delete",
                }
            )
            continue
        try:
            lambda_client.delete_function(FunctionName=function_name, Qualifier=version)
            results.append(
                {
                    "function_name": function_name,
                    "version": version,
                    "status": "deleted",
                }
            )
        except ClientError as exc:
            results.append(
                {
                    "function_name": function_name,
                    "version": version,
                    "status": "error",
                    "error": str(exc),
                }
            )
    return results


def _format_completion_email(
    outcome: str,
    scan_result: dict[str, Any],
    deletion_results: list[dict[str, Any]] | None,
    cfg: dict[str, Any],
    dry_run: bool,
    approval: dict[str, Any] | None,
) -> tuple[str, str]:
    """Build (subject, body) for the terminal-state email. Plain text only."""
    summary = scan_result.get("summary", {}) if scan_result else {}
    region = cfg.get("region") or summary.get("scan_config", {}).get("region", "?")

    counts = {"deleted": 0, "dry_run_would_delete": 0, "error": 0, "skipped_invalid_entry": 0}
    for r in deletion_results or []:
        status = r.get("status") or "unknown"
        counts[status] = counts.get(status, 0) + 1

    if outcome == "done":
        verb = "DRY-RUN" if dry_run else "DELETED"
        primary = counts.get("dry_run_would_delete", 0) if dry_run else counts.get("deleted", 0)
        subject = (
            f"[SnapStart Cleaner] Cleanup complete -- "
            f"{verb} {primary}, errors {counts.get('error', 0)} in {region}"
        )
    elif outcome == "rejected":
        subject = f"[SnapStart Cleaner] Rejected -- no versions deleted in {region}"
    else:
        subject = f"[SnapStart Cleaner] Approval timed out -- no versions deleted in {region}"

    lines: list[str] = []
    if outcome == "done":
        mode_line = "DRY RUN (no AWS deletes issued)" if dry_run else "LIVE (AWS deletes issued)"
        lines.append(f"Cleanup finished in {region} -- mode: {mode_line}.")
    elif outcome == "rejected":
        lines.append(
            f"Cleanup was REJECTED in {region}. No SnapStart Lambda versions were deleted."
        )
    else:
        lines.append(
            f"Cleanup TIMED OUT in {region} before an approver responded. "
            "No SnapStart Lambda versions were deleted."
        )
    lines.append("")

    if summary:
        lines.append("=== SCAN SUMMARY ===")
        lines.append(
            f"  Total SnapStart versions scanned        : "
            f"{summary.get('total_deletable_state_versions', summary.get('total_snapstart_active_versions', 0))}"
        )
        state_breakdown = summary.get("state_breakdown") or {}
        if state_breakdown:
            parts = ", ".join(f"{s}={n}" for s, n in sorted(state_breakdown.items()))
            lines.append(f"  State breakdown                         : {parts}")
        lines.append(f"  Stage A pass (age + keep_last_n)        : {summary.get('stage_a_pass', 0)}")
        lines.append(f"  Stage B pass (no usage, no alias)       : {summary.get('stage_b_pass', 0)}")
        lines.append(f"  Candidates presented for approval       : {summary.get('candidates_for_deletion', 0)}")
        lines.append(f"  Alias-protected (skipped)               : {summary.get('alias_protected', 0)}")
        lines.append("")

    if outcome == "done":
        lines.append("=== DELETION RESULTS ===")
        lines.append(f"  Total acted on           : {len(deletion_results or [])}")
        if dry_run:
            lines.append(f"  Would-delete (dry run)  : {counts.get('dry_run_would_delete', 0)}")
        else:
            lines.append(f"  Successfully deleted    : {counts.get('deleted', 0)}")
        lines.append(f"  Errors                   : {counts.get('error', 0)}")
        lines.append(f"  Skipped (invalid entry)  : {counts.get('skipped_invalid_entry', 0)}")
        lines.append("")

        errors = [r for r in (deletion_results or []) if r.get("status") == "error"]
        if errors:
            max_err = 30
            lines.append(f"=== ERRORS (showing up to {max_err}) ===")
            for r in errors[:max_err]:
                lines.append(
                    f"  {r.get('function_name')}:{r.get('version')}  -- {r.get('error')}"
                )
            if len(errors) > max_err:
                lines.append(f"  ... and {len(errors) - max_err} more (see full report).")
            lines.append("")

    if approval:
        src = approval.get("decision_source") or "unknown"
        approver = approval.get("approver") or "unknown"
        lines.append(f"Decision source: {src}  (approver: {approver})")
    report_uri = scan_result.get("report_s3_uri") if scan_result else None
    if report_uri:
        lines.append(f"Full scan report: {report_uri}")
    lines.append("")
    return subject, "\n".join(lines)


def step8_send_completion_notification(
    outcome: str,
    scan_result: dict[str, Any],
    deletion_results: list[dict[str, Any]] | None,
    cfg: dict[str, Any],
    dry_run: bool,
    approval: dict[str, Any] | None = None,
    sns_client=None,
) -> dict[str, Any]:
    """
    Send the terminal-state email to the approval SNS topic when the
    orchestrator finishes (approved-and-deleted, rejected, or timed out).

    outcome is one of: "done" | "rejected" | "timeout".

    Falls back to log-only when approval_topic_arn is not configured (local
    CLI path), mirroring step6's behavior so the orchestrator stays usable
    offline.

    Replay safety: this runs inside context.step(), so on checkpoint retries
    SNS Publish may be called more than once -- same caveat documented for
    step6_send_approval_notification. Acceptable for the POC.
    """
    topic_arn = cfg.get("approval_topic_arn")
    if not topic_arn:
        print(
            json.dumps(
                {
                    "completion_mode": "log-only",
                    "outcome": outcome,
                    "dry_run": dry_run,
                    "deletion_count": len(deletion_results or []),
                    "region": cfg.get("region"),
                    "hint": (
                        "Set approval_topic_arn (or APPROVAL_TOPIC_ARN env var) "
                        "to enable completion email notifications."
                    ),
                },
                default=str,
            ),
            file=sys.stderr,
        )
        return {"mode": "log-only", "outcome": outcome}

    if sns_client is None:
        raise ValueError(
            "step8_send_completion_notification requires sns_client when "
            "approval_topic_arn is configured"
        )

    subject, body = _format_completion_email(
        outcome, scan_result, deletion_results, cfg, dry_run, approval
    )
    if len(subject) > 100:
        subject = subject[:97] + "..."

    sns_client.publish(TopicArn=topic_arn, Subject=subject, Message=body)
    return {
        "mode": "sns",
        "outcome": outcome,
        "topic_arn": topic_arn,
        "subject": subject,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
