"""OAuth-authenticated invocation of the attached Agent endpoint."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import time
from typing import Any

from databricks.sdk import WorkspaceClient

READ_ONLY_TOOLS = {
    "get_market_overview",
    "get_product_performance",
    "get_variance_drivers",
    "decompose_reimbursement_change",
    "detect_reimbursement_outliers",
    "get_drug_profile",
    "search_drug_context",
}
MAX_APPROVAL_ROUNDS = 10
APPROVAL_TTL_SECONDS = 15 * 60
CONVERSATION_TTL_SECONDS = 8 * 60 * 60


def _approval_requests(response: dict[str, Any]) -> list[dict[str, Any]]:
    output = response.get("output")
    if not isinstance(output, list):
        return []
    return [
        item for item in output
        if isinstance(item, dict) and item.get("type") == "mcp_approval_request"
    ]


def _extract_answer(response: dict[str, Any]) -> str | None:
    """Extract assistant text from Responses API and legacy endpoint payloads."""
    output_text = response.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text

    output = response.get("output")
    if isinstance(output, str) and output.strip():
        return output
    if isinstance(output, list):
        text_parts: list[str] = []
        for item in output:
            if not isinstance(item, dict):
                continue
            content = item.get("content")
            if isinstance(content, str):
                text_parts.append(content)
                continue
            if not isinstance(content, list):
                continue
            for part in content:
                if isinstance(part, str):
                    text_parts.append(part)
                elif isinstance(part, dict) and isinstance(part.get("text"), str):
                    text_parts.append(part["text"])
        if text_parts:
            return "\n".join(part for part in text_parts if part.strip())

    choices = response.get("choices") or []
    if choices and isinstance(choices[0], dict):
        return (choices[0].get("message") or {}).get("content") or choices[0].get("text")
    content = response.get("content")
    answer = response.get("answer")
    return content if isinstance(content, str) else answer if isinstance(answer, str) else None


def _signing_key() -> bytes:
    key = os.getenv("APPROVAL_SIGNING_KEY") or os.getenv("DATABRICKS_CLIENT_SECRET")
    if not key:
        raise RuntimeError("The App runtime approval-signing credential is unavailable.")
    return key.encode("utf-8")


def _encode_context(context: dict[str, Any]) -> str:
    payload = json.dumps(context, separators=(",", ":"), sort_keys=True).encode("utf-8")
    signature = hmac.new(_signing_key(), payload, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(signature + payload).decode("ascii")


def _decode_context(token: str, ttl_seconds: int, error_message: str) -> dict[str, Any]:
    try:
        signed = base64.b64decode(str(token), altchars=b"-_", validate=True)
        signature, payload = signed[:32], signed[32:]
        expected = hmac.new(_signing_key(), payload, hashlib.sha256).digest()
        if len(signature) != 32 or not hmac.compare_digest(signature, expected):
            raise ValueError
        context = json.loads(payload)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError(error_message) from exc
    issued_at = context.get("issued_at")
    if not isinstance(issued_at, int) or not 0 <= time.time() - issued_at <= ttl_seconds:
        raise ValueError(error_message)
    return context


def _encode_approval(context: dict[str, Any]) -> str:
    return _encode_context(context)


def _decode_approval(token: str) -> dict[str, Any]:
    context = _decode_context(
        token,
        APPROVAL_TTL_SECONDS,
        "The write approval is invalid or expired. Start the investigation again.",
    )
    if not isinstance(context.get("history"), list) or not isinstance(context.get("approvals"), list):
        raise ValueError("The write approval is invalid. Start the investigation again.")
    return context


def _encode_conversation(endpoint: str, history: list[dict[str, Any]]) -> str:
    return _encode_context({
        "issued_at": int(time.time()),
        "endpoint": endpoint,
        "history": history,
    })


def _decode_conversation(token: str, endpoint: str) -> list[dict[str, Any]]:
    context = _decode_context(
        token,
        CONVERSATION_TTL_SECONDS,
        "The conversation expired or is invalid. Start a new conversation.",
    )
    if context.get("endpoint") != endpoint or not isinstance(context.get("history"), list):
        raise ValueError("The conversation is invalid. Start a new conversation.")
    return context["history"]


def _approval_response(request_id: str, approve: bool) -> dict[str, Any]:
    return {
        "type": "mcp_approval_response",
        "approval_request_id": request_id,
        "approve": approve,
    }


def _approval_result(endpoint: str, history: list[dict[str, Any]],
                     approvals: list[dict[str, Any]],
                     policy_approved: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    proposed = [
        {
            "id": request.get("id"),
            "name": request.get("name", "unknown"),
            "server_label": request.get("server_label"),
            "arguments": request.get("arguments", "{}"),
        }
        for request in approvals
    ]
    token = _encode_approval({
        "issued_at": int(time.time()),
        "endpoint": endpoint,
        "history": history,
        "approvals": [
            *(
                {
                    "id": request.get("id"),
                    "name": request.get("name", "unknown"),
                }
                for request in (policy_approved or [])
            ),
            *proposed,
        ],
    })
    names = ", ".join(str(request["name"]) for request in proposed)
    return {
        "answer": f"Review and approve the proposed write: {names}.",
        "approval_required": True,
        "approval_token": token,
        "proposed_writes": proposed,
    }


def _run_agent(endpoint: str, history: list[dict[str, Any]]) -> dict[str, Any]:
    client = WorkspaceClient()
    path = f"/serving-endpoints/{endpoint}/invocations"
    response: Any = None
    for _ in range(MAX_APPROVAL_ROUNDS):
        # Pass a new list to the SDK. The local history is extended after the
        # response returns; sharing the same list with the request body can
        # retroactively mutate recorded/retried request payloads.
        response = client.api_client.do("POST", path, body={"input": list(history)})
        if not isinstance(response, dict):
            return {
                "answer": str(response),
                "raw": response,
                "conversation_token": _encode_conversation(endpoint, history),
            }
        approvals = _approval_requests(response)
        if not approvals:
            break
        output = response.get("output") or []
        history.extend(item for item in output if isinstance(item, dict))
        protected = [request for request in approvals if request.get("name") not in READ_ONLY_TOOLS]
        read_only = [request for request in approvals if request.get("name") in READ_ONLY_TOOLS]
        if protected:
            # If the model requested read and write tools together, carry the
            # policy-approved read requests in the signed continuation as well.
            return _approval_result(endpoint, history, protected, read_only)
        history.extend(_approval_response(request["id"], True) for request in read_only)
    else:
        raise RuntimeError("The agent exceeded the maximum number of MCP approval rounds.")

    answer = _extract_answer(response)
    output = response.get("output") if isinstance(response, dict) else None
    if isinstance(output, list):
        history.extend(item for item in output if isinstance(item, dict))
    elif answer:
        history.append({"role": "assistant", "content": answer})
    return {
        "answer": answer or "The agent returned no displayable answer.",
        "raw": response,
        "conversation_token": _encode_conversation(endpoint, history),
    }


def ask_agent(message: str, conversation_token: str | None = None) -> dict[str, Any]:
    endpoint = os.getenv("AGENT_ENDPOINT")
    if not endpoint:
        raise RuntimeError("AGENT_ENDPOINT is not configured from the finance-agent App resource.")
    prompt = " ".join(str(message or "").split())
    if not prompt or len(prompt) > 8000:
        raise ValueError("Question must be between 1 and 8000 characters.")
    history = _decode_conversation(conversation_token, endpoint) if conversation_token else []
    history.append({"role": "user", "content": prompt})
    return _run_agent(endpoint, history)


def continue_agent(approval_token: str, approve: bool) -> dict[str, Any]:
    context = _decode_approval(approval_token)
    endpoint = os.getenv("AGENT_ENDPOINT")
    if not endpoint or context.get("endpoint") != endpoint:
        raise ValueError("The Agent endpoint changed. Start the investigation again.")
    decisions = []
    for request in context["approvals"]:
        request_id = request.get("id")
        if not isinstance(request_id, str) or not request_id:
            raise ValueError("The write approval is invalid. Start the investigation again.")
        is_read_only = request.get("name") in READ_ONLY_TOOLS
        decisions.append(_approval_response(request_id, True if is_read_only else approve))
    history = context["history"]
    history.extend(decisions)
    result = _run_agent(endpoint, history)
    if not approve:
        result["approval_cancelled"] = True
    return result
