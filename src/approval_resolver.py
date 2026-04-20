"""
Approval resolver Lambda.

Backs a public Function URL with two one-click endpoints that the email
recipient lands on:

  GET  /approve?cb=<callback_id>     -> safe preview page with a "Confirm" button
  POST /approve?cb=<callback_id>     -> actually resolve + delete the bundle
  GET  /reject?cb=<callback_id>      -> safe preview page with a "Confirm" button
  POST /reject?cb=<callback_id>      -> actually resolve + delete the bundle

Why the GET/POST split: Outlook Safe Links, Gmail link-scan, and corporate
email-security gateways all GET-prefetch any URL in an inbound email (often
3-5 times) to scan for malware. If GET consumed the bundle we'd burn the
one-shot token on the prescan and the human's browser click would land on
an already-expired page -- which is exactly what was happening in practice.
HTML form POSTs are not followed by prescanners, so the real state change
moves to POST.

Flow on POST:
  1. Look up the pending-callback bundle at
     s3://<APPROVAL_BUNDLE_BUCKET>/pending-callbacks/<callback_id>.json
     (written by step6_send_approval_notification).
  2. Call lambda:SendDurableExecutionCallbackSuccess with a JSON result
     of the form {"approved": bool, "approved_versions": [...]} so the
     durable orchestrator can resume and decide whether to delete.
     We use *Success* for both approve and reject so the orchestrator
     sees a uniform return shape; the "approved" boolean carries intent.
     Failure is reserved for genuine processing errors.
  3. Delete the S3 object so the same link cannot be replayed.
  4. Return a small HTML confirmation page.

Security note: the callback_id is the capability token. Treat the S3
bundle + one-shot delete as the consumed-on-use guarantee. For
production harden with HMAC-signed URLs and/or Function URL AuthType=
AWS_IAM with SigV4 link signing.
"""

from __future__ import annotations

import html
import json
import logging
import os
from typing import Any
from urllib.parse import parse_qs, urlparse

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET = os.environ["APPROVAL_BUNDLE_BUCKET"]
BUNDLE_PREFIX = "pending-callbacks/"

_s3 = boto3.client("s3")
_lambda = boto3.client("lambda")


# ---------------------------------------------------------------------------
# Request parsing (Function URL event shape == API GW v2 payload)
# ---------------------------------------------------------------------------
def _parse_request(event: dict[str, Any]) -> tuple[str, str, str | None]:
    """Return (method, decision, callback_id). decision is 'approve'|'reject'|''."""
    http_ctx = event.get("requestContext", {}).get("http", {}) or {}
    method = (http_ctx.get("method") or event.get("httpMethod") or "GET").upper()

    raw_path = (
        event.get("rawPath")
        or http_ctx.get("path")
        or urlparse(event.get("path", "")).path
        or ""
    )
    decision = ""
    if raw_path.rstrip("/").endswith("/approve") or raw_path == "/approve":
        decision = "approve"
    elif raw_path.rstrip("/").endswith("/reject") or raw_path == "/reject":
        decision = "reject"

    qs = event.get("queryStringParameters") or {}
    callback_id = qs.get("cb") or qs.get("callback_id")
    if not callback_id and event.get("rawQueryString"):
        parsed = parse_qs(event["rawQueryString"])
        callback_id = (parsed.get("cb") or parsed.get("callback_id") or [None])[0]

    return method, decision, callback_id


# ---------------------------------------------------------------------------
# S3 bundle I/O
# ---------------------------------------------------------------------------
def _load_bundle(callback_id: str) -> dict[str, Any] | None:
    key = f"{BUNDLE_PREFIX}{callback_id}.json"
    try:
        obj = _s3.get_object(Bucket=BUCKET, Key=key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in ("NoSuchKey", "404", "AccessDenied"):
            return None
        raise
    return json.loads(obj["Body"].read().decode("utf-8"))


def _consume_bundle(callback_id: str) -> None:
    key = f"{BUNDLE_PREFIX}{callback_id}.json"
    try:
        _s3.delete_object(Bucket=BUCKET, Key=key)
    except ClientError as exc:
        # Best-effort; the S3 lifecycle rule is the backstop.
        logger.warning("Failed to delete pending bundle %s: %s", key, exc)


# ---------------------------------------------------------------------------
# Callback resolution
# ---------------------------------------------------------------------------
def _resolve_callback(callback_id: str, approved: bool, bundle: dict[str, Any]) -> None:
    """
    Send a uniform {approved, approved_versions, ...} payload to the
    durable-execution callback. We always call *Success* so the
    orchestrator can branch on the `approved` flag; *Failure* would make
    the wait raise and skip the controlled rejected-or-timeout branch.
    """
    candidates = bundle.get("candidates") or []
    approved_versions = (
        [
            {"function_name": r["function_name"], "version": r["version"]}
            for r in candidates
        ]
        if approved
        else []
    )
    result_payload = {
        "approved": approved,
        "approved_versions": approved_versions,
        "approver": bundle.get("approver"),
        "decision_source": "function-url",
    }
    _lambda.send_durable_execution_callback_success(
        CallbackId=callback_id,
        Result=json.dumps(result_payload).encode("utf-8"),
    )


# ---------------------------------------------------------------------------
# HTML responses
# ---------------------------------------------------------------------------
_HEADERS_HTML = {
    "Content-Type": "text/html; charset=utf-8",
    # These two tell Safe Links / Gmail / corporate gateways not to cache or
    # prefetch the page. Not a guarantee but reduces the stale-preview risk.
    "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    "X-Robots-Tag": "noindex, nofollow",
}


def _html_response(status: int, title: str, body: str) -> dict[str, Any]:
    html_doc = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{title}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="robots" content="noindex,nofollow">
  <meta http-equiv="cache-control" content="no-store">
  <style>
    body {{ font-family: system-ui, sans-serif; max-width: 640px; margin: 4rem auto; padding: 0 1rem; color: #222; }}
    .card {{ border: 1px solid #e1e4e8; border-radius: 8px; padding: 2rem; background: #fafbfc; }}
    h1 {{ margin-top: 0; }}
    .ok {{ color: #1a7f37; }}
    .warn {{ color: #bf8700; }}
    .err {{ color: #cf222e; }}
    code {{ background: #eef1f4; padding: 0.1rem 0.3rem; border-radius: 3px; }}
    .btn {{ display: inline-block; padding: 0.6rem 1.2rem; border-radius: 6px; border: 1px solid #0969da; background: #0969da; color: #fff; font-weight: 600; cursor: pointer; font-size: 1rem; }}
    .btn:hover {{ background: #0860c7; }}
    .btn-reject {{ background: #cf222e; border-color: #cf222e; }}
    .btn-reject:hover {{ background: #b81b27; }}
    .muted {{ color: #57606a; font-size: 0.9rem; }}
    form {{ margin-top: 1.25rem; }}
  </style>
</head>
<body>
  <div class="card">
    <h1>{title}</h1>
    {body}
  </div>
</body>
</html>"""
    return {
        "statusCode": status,
        "headers": _HEADERS_HTML,
        "body": html_doc,
    }


def _render_confirm_page(decision: str, callback_id: str, bundle: dict[str, Any]) -> dict[str, Any]:
    """
    GET handler: show a human a read-only summary and a POST-form button.

    Loading the bundle here does NOT consume it. The callback is only
    resolved when the human (or anything capable of POST) submits the form.
    """
    candidate_count = len(bundle.get("candidates") or [])
    summary = bundle.get("summary") or {}
    total_scanned = summary.get("total_snapstart_active_versions", "?")
    region = (summary.get("scan_config") or {}).get("region", "?")

    safe_cb = html.escape(callback_id, quote=True)
    action_path = f"/{decision}?cb={safe_cb}"

    if decision == "approve":
        title = "Confirm deletion approval"
        button_label = "Confirm approval"
        button_class = "btn"
        lede = (
            f'<p class="ok"><strong>{candidate_count}</strong> SnapStart Lambda '
            f"version(s) will be deleted from <code>{html.escape(str(region))}</code>.</p>"
        )
    else:
        title = "Confirm rejection"
        button_label = "Confirm rejection"
        button_class = "btn btn-reject"
        lede = (
            f'<p class="warn">Reject the cleanup. The orchestrator will exit '
            f"with <code>rejected_or_timeout</code> and no versions will be "
            "deleted.</p>"
        )

    body = (
        f"{lede}"
        f'<p class="muted">Scan summary: {total_scanned} SnapStart Active versions '
        f"scanned in {html.escape(str(region))}. "
        f"{candidate_count} flagged as deletable.</p>"
        f'<p class="muted">Clicking the button below records your decision. '
        "Email-security prescanners that opened this link will not have "
        "taken any action.</p>"
        f'<form method="POST" action="{html.escape(action_path, quote=True)}">'
        f'  <button type="submit" class="{button_class}">{button_label}</button>'
        "</form>"
    )
    return _html_response(200, title, body)


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------
def _invalid_link() -> dict[str, Any]:
    return _html_response(
        400,
        "Invalid link",
        '<p class="err">The link is missing an <code>/approve</code> or '
        "<code>/reject</code> path. Please use the buttons in the "
        "notification email.</p>",
    )


def _missing_callback() -> dict[str, Any]:
    return _html_response(
        400,
        "Invalid link",
        '<p class="err">The link is missing the callback token '
        "(<code>cb</code>). Please use the buttons in the notification "
        "email.</p>",
    )


def _expired_link() -> dict[str, Any]:
    return _html_response(
        410,
        "Link already used or expired",
        '<p class="warn">This approval link has already been used, or '
        "the 8-day approval window has elapsed. If the orchestrator is "
        "still waiting, ask your operator to re-run the scan.</p>",
    )


def lambda_handler(event: dict[str, Any], _context: Any) -> dict[str, Any]:
    method, decision, callback_id = _parse_request(event)

    if decision not in ("approve", "reject"):
        return _invalid_link()
    if not callback_id:
        return _missing_callback()

    # HEAD / OPTIONS: zero-side-effect, no body. Covers email scanners that
    # probe with HEAD before a GET and CORS preflights (we don't actually
    # need CORS, but answering cleanly avoids a scary 500 in logs).
    if method in ("HEAD", "OPTIONS"):
        return {"statusCode": 200, "headers": _HEADERS_HTML, "body": ""}

    bundle = _load_bundle(callback_id)
    if bundle is None:
        return _expired_link()

    # GET = safe preview. Email-security prescanners follow GETs but not
    # HTML-form POSTs, so this is what actually keeps the one-shot token
    # alive until the human clicks the button.
    if method == "GET":
        return _render_confirm_page(decision, callback_id, bundle)

    # Anything other than POST at this point is an unexpected method; treat
    # as a preview (same as GET) rather than mutating state.
    if method != "POST":
        return _render_confirm_page(decision, callback_id, bundle)

    approved = decision == "approve"
    try:
        _resolve_callback(callback_id, approved, bundle)
    except ClientError as exc:
        logger.exception("Failed to resolve durable callback")
        return _html_response(
            502,
            "Could not record decision",
            f'<p class="err">Lambda rejected the callback: <code>{html.escape(str(exc))}</code>. '
            "If this persists, contact your operator.</p>",
        )

    _consume_bundle(callback_id)

    candidate_count = len(bundle.get("candidates") or [])
    if approved:
        body = (
            '<p class="ok">Approved. The orchestrator has been notified to '
            f"delete <strong>{candidate_count}</strong> SnapStart Lambda "
            "version(s) from the report.</p>"
            "<p>You can close this page.</p>"
        )
        return _html_response(200, "Deletion approved", body)

    body = (
        '<p class="warn">Rejected. The orchestrator will exit with '
        "<code>rejected_or_timeout</code> and no versions will be "
        "deleted.</p>"
        "<p>You can close this page.</p>"
    )
    return _html_response(200, "Deletion rejected", body)
