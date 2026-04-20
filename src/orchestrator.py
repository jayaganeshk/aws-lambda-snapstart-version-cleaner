"""
Durable Function orchestrator for the SnapStart version cleaner.

Mirrors POC.py's pipeline but wraps each stage in context.step(...) and
adds a context.wait_for_callback(...) gate so a human can review the
deletion-candidate report before step7 runs.

Approval flow (production wiring):
  1. step6_send_approval_notification uploads the pending bundle to
     s3://$APPROVAL_BUNDLE_BUCKET/pending-callbacks/<callback_id>.json
     and publishes an SNS email to $APPROVAL_TOPIC_ARN that contains
     two one-click URLs pointing at $APPROVAL_RESOLVER_URL/{approve,reject}.
  2. The resolver Lambda (infra/src/approval_resolver.py) calls
     lambda:SendDurableExecutionCallbackSuccess with
     {"approved": bool, "approved_versions": [...]}.
  3. This orchestrator resumes, branches on `approved`, and runs step7.

Local fallback: when those env vars are unset, step6 falls back to
log-only and you resume the wait manually with
`sam local callback succeed <exec-id> <callback-id> --payload ...`.

Runtime: Python 3.13.
"""

from __future__ import annotations

import json
import os
from typing import Any

from aws_durable_execution_sdk_python import (
    DurableContext,
    durable_execution,
)
from aws_durable_execution_sdk_python.config import Duration, WaitForCallbackConfig

from steps import (
    build_clients,
    normalize_config,
    step6_send_approval_notification,
    step7_delete_versions,
    step8_send_completion_notification,
    step_apply_stage_a,
    step_build_and_upload_report,
    step_confirm_active,
    step_discover_snapstart,
    step_usage_and_alias_check,
)

APPROVAL_TIMEOUT_SECONDS = 86400 * 7  # 7 days -- reviewer window before auto-reject

# DELETE_DRY_RUN is read from the environment so the same deployment package
# can ship safely (default "true") and be flipped to "false" once the flow
# has been validated end-to-end against a non-prod account.
_DELETE_DRY_RUN = os.environ.get("DELETE_DRY_RUN", "true").strip().lower() == "true"


def _approval_config(base_cfg: dict[str, Any]) -> dict[str, Any]:
    """Merge approval-flow env vars onto the caller's event config."""
    return {
        **base_cfg,
        "approval_topic_arn": os.environ.get("APPROVAL_TOPIC_ARN")
        or base_cfg.get("approval_topic_arn"),
        "approval_bundle_bucket": os.environ.get("APPROVAL_BUNDLE_BUCKET")
        or base_cfg.get("approval_bundle_bucket"),
        "approval_resolver_url": os.environ.get("APPROVAL_RESOLVER_URL")
        or base_cfg.get("approval_resolver_url"),
    }


def _coerce_approval_payload(raw: Any) -> dict[str, Any]:
    """
    The durable SDK surfaces the callback Result as whatever the external
    caller POSTed. Accept dict (already decoded) or JSON string/bytes.
    """
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return {}
    return {}


@durable_execution
def lambda_handler(event: dict[str, Any], context: DurableContext):
    cfg = _approval_config(normalize_config(event or {}))

    lambda_client, cw_client, s3_client, sns_client = build_clients(cfg["region"])

    # The durable-execution checkpoint payload is capped at 256 KiB per step.
    # Rather than collapse the scan into one opaque step, each phase persists
    # its bulky intermediate list to s3://<bundle_bucket>/intermediate/<exec>/
    # and checkpoints only a tiny S3 handle + a few counts. That keeps every
    # phase visible in the Durable Operations panel while staying well under
    # the 256 KiB cap.
    bundle_bucket = cfg.get("approval_bundle_bucket")
    if not bundle_bucket:
        raise RuntimeError(
            "APPROVAL_BUNDLE_BUCKET is not configured; the orchestrator "
            "needs an S3 bucket to offload the full scan report."
        )

    # The execution ARN keeps S3 keys stable across in-step retries within
    # one durable execution. Format ends in .../<execution-name>/<exec-id>;
    # we slice the last two path segments to get a URL-safe stable id.
    exec_arn = context.execution_context.durable_execution_arn
    execution_id = "__".join(exec_arn.split("/")[-2:])

    discover_handle = context.step(
        lambda _: step_discover_snapstart(
            lambda_client, s3_client, cfg, bundle_bucket, execution_id
        ),
        name="discover-snapstart",
    )
    active_handle = context.step(
        lambda _: step_confirm_active(
            lambda_client, s3_client, discover_handle, cfg, bundle_bucket, execution_id
        ),
        name="confirm-active",
    )
    stage_a_handle = context.step(
        lambda _: step_apply_stage_a(
            s3_client, active_handle, cfg, bundle_bucket, execution_id
        ),
        name="stage-a-age-and-keep-last-n",
    )
    stage_b_handle = context.step(
        lambda _: step_usage_and_alias_check(
            lambda_client, cw_client, s3_client, stage_a_handle, cfg, bundle_bucket, execution_id
        ),
        name="stage-b-usage-and-alias",
    )
    scan_result = context.step(
        lambda _: step_build_and_upload_report(
            s3_client, stage_b_handle, cfg, bundle_bucket, execution_id
        ),
        name="build-and-upload-report",
    )

    # The send_approval closure runs once when the wait starts. It must be
    # deterministic on replay; scan_result is stable (S3 PutObject is
    # idempotent on identical key+body, and the candidate list is frozen in
    # the checkpoint). SNS Publish on replay is NOT idempotent and will
    # send a duplicate email -- acceptable trade-off for this POC; a
    # production build should gate the publish on a "sent" marker in the
    # bundle key.
    #
    # The submitter signature is (callback_id, wait_context) in the current
    # SDK; we don't need the wait_context so it's ignored.
    def send_approval(callback_id: str, _wait_context) -> None:
        step6_send_approval_notification(
            callback_id=callback_id,
            scan_result=scan_result,
            cfg=cfg,
            s3_client=s3_client,
            sns_client=sns_client,
        )

    approval_raw = context.wait_for_callback(
        send_approval,
        name="approval",
        config=WaitForCallbackConfig(
            timeout=Duration(seconds=APPROVAL_TIMEOUT_SECONDS),
        ),
    )
    approval = _coerce_approval_payload(approval_raw)

    if not approval.get("approved"):
        # Rejected (or auto-rejected on the not-approved path). Fire the
        # terminal-state email so the approver gets closure even when no
        # deletes ran. Timeouts typically surface as an SDK exception, not
        # this branch, but the resolver could also post approved=false on
        # an explicit timeout sweep -- respect that here.
        outcome = "timeout" if approval.get("timed_out") else "rejected"
        context.step(
            lambda _: step8_send_completion_notification(
                outcome=outcome,
                scan_result=scan_result,
                deletion_results=None,
                cfg=cfg,
                dry_run=_DELETE_DRY_RUN,
                approval=approval,
                sns_client=sns_client,
            ),
            name=f"send-{outcome}-email",
        )
        return {
            "status": "rejected_or_timeout",
            "report_s3_uri": scan_result.get("report_s3_uri"),
            "summary": scan_result.get("summary"),
            "approval": approval,
        }

    # Approved: delete the versions. approved_versions is sent by the
    # resolver and matches the candidate_for_deletion rows at approval time.
    approved_versions = approval.get("approved_versions") or []
    deletion_results = context.step(
        lambda _: step7_delete_versions(
            lambda_client, approved_versions, cfg, dry_run=_DELETE_DRY_RUN
        ),
        name="delete-versions",
    )

    context.step(
        lambda _: step8_send_completion_notification(
            outcome="done",
            scan_result=scan_result,
            deletion_results=deletion_results,
            cfg=cfg,
            dry_run=_DELETE_DRY_RUN,
            approval=approval,
            sns_client=sns_client,
        ),
        name="send-completion-email",
    )

    return {
        "status": "done",
        "report_s3_uri": scan_result.get("report_s3_uri"),
        "summary": scan_result.get("summary"),
        "deletion_results": deletion_results,
        "dry_run": _DELETE_DRY_RUN,
    }
