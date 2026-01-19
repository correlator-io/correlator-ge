"""OpenLineage event emitter for Correlator.

This module handles HTTP communication with Correlator's lineage endpoint.
Consistent with correlator-dbt emitter pattern for PDK compatibility.

The emitter:
    - Accepts RunEvent objects (serialization happens here)
    - Sends events to Correlator's /api/v1/lineage/events endpoint
    - Handles response codes (200/204, 207 partial success, 4xx, 5xx)
    - Raises exceptions on errors (action layer catches them)

Architecture:
    ValidationAction builds events → passes RunEvent to emit_events → HTTP POST

Requirements:
    - Great Expectations >= 1.3.0
    - openlineage-python >= 1.0.0
"""

import logging
from datetime import datetime
from enum import Enum
from typing import Any, Optional, Union
from uuid import UUID

import attr
import requests
from openlineage.client.event_v2 import (
    DatasetEvent,
    JobEvent,
    RunEvent,
)

logger = logging.getLogger(__name__)

# Type alias for OpenLineage event types
Event = Union[RunEvent, DatasetEvent, JobEvent]


def _serialize_attr_value(
    inst: type,  # noqa: ARG001
    field: attr.Attribute,  # noqa: ARG001
    value: Any,
) -> Any:
    """Custom serializer for attr values (matches correlator-dbt pattern).

    Handles datetime, UUID, and Enum serialization for JSON compatibility.
    Used as value_serializer argument to attr.asdict().

    Args:
        inst: The attr class instance (unused, required by attr API).
        field: The attr field being serialized (unused, required by attr API).
        value: The value to serialize.

    Returns:
        Serialized value (ISO format for datetime, string for UUID/Enum, unchanged otherwise).
    """
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    return value


def emit_events(
    events: list[Event],
    endpoint: str,
    api_key: Optional[str] = None,
    session: Optional[requests.Session] = None,
    timeout: int = 30,
) -> None:
    """Emit OpenLineage events to Correlator.

    Serializes events and sends to Correlator's lineage endpoint.
    Events are wrapped in an array (Correlator API requirement).

    Args:
        events: List of OpenLineage event objects (RunEvent, DatasetEvent, JobEvent).
        endpoint: Full Correlator API endpoint URL.
        api_key: Optional API key for X-API-Key header.
        session: Pre-configured requests.Session. If None, creates default session.
        timeout: Request timeout in seconds (default: 30, matches dbt-correlator).

    Raises:
        ConnectionError: If unable to connect to Correlator.
        TimeoutError: If request times out.
        ValueError: If Correlator returns an error response (4xx, 5xx).

    Example:
        >>> from openlineage.client.run import RunEvent
        >>> events = [run_event]  # RunEvent object
        >>> emit_events(events, "http://localhost:8080/api/v1/lineage/events")
    """
    if session is None:
        session = requests.Session()

    # Serialize events to dicts
    event_dicts = [
        attr.asdict(event, value_serializer=_serialize_attr_value)  # type: ignore[call-arg]
        for event in events
    ]

    headers: dict[str, str] = {"Content-Type": "application/json"}
    if api_key:
        headers["X-API-Key"] = api_key

    try:
        response = session.post(
            endpoint,
            json=event_dicts,
            headers=headers,
            timeout=timeout,
        )
        _handle_response(response, len(events))

    except requests.exceptions.Timeout as e:
        raise TimeoutError(f"Timeout emitting events to {endpoint}") from e
    except requests.exceptions.ConnectionError as e:
        raise ConnectionError(f"Connection error emitting events to {endpoint}") from e


def _handle_response(response: requests.Response, event_count: int) -> None:
    """Handle Correlator API response (matches correlator-dbt pattern).

    Args:
        response: HTTP response from Correlator.
        event_count: Number of events that were sent.

    Raises:
        ValueError: If response indicates an error (4xx, 5xx).
    """
    # Success: 200 OK or 204 No Content
    if response.status_code in (200, 204):
        logger.info(f"Successfully emitted {event_count} events")

        # Parse summary if available (200 with body)
        if response.status_code == 200 and response.text:
            try:
                body = response.json()
                if "summary" in body:
                    summary = body["summary"]
                    logger.info(
                        f"Response: {summary.get('successful', 0)} successful, "
                        f"{summary.get('failed', 0)} failed"
                    )
            except (ValueError, KeyError):
                pass  # Response parsing is best-effort
        return

    # Partial success: 207 Multi-Status
    if response.status_code == 207:
        try:
            body = response.json()
            summary = body.get("summary", {})
            successful = summary.get("successful", 0)
            received = summary.get("received", event_count)
            failed_events = body.get("failed_events", [])

            logger.warning(
                f"Partial success: {successful}/{received} events succeeded. "
                f"Failed events: {failed_events}"
            )

            # Log individual failures for debugging
            for failed in failed_events:
                index = failed.get("index", "?")
                reason = failed.get("reason", "Unknown error")
                logger.error(f"Event {index} failed: {reason}")
        except (ValueError, KeyError):
            logger.warning("Partial success (207) but could not parse response")
        return

    # Client errors: 4xx
    if response.status_code == 429:
        raise ValueError("Rate limited by Correlator")

    if 400 <= response.status_code < 500:
        raise ValueError(
            f"Event rejected by Correlator ({response.status_code}): "
            f"{response.text[:500]}"
        )

    # Server errors: 5xx or unexpected codes
    raise ValueError(
        f"Correlator returned {response.status_code}: {response.text[:500]}"
    )
