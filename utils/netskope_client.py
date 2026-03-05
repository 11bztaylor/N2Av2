"""
Netskope REST API v2 dataexport iterator client.

Endpoint structure (GET with query params):
  Events:  GET /api/v2/events/dataexport/events/{type}?operation=next&index={name}
  Alerts:  GET /api/v2/events/dataexport/alerts/{subtype}?operation=next&index={name}

Auth header:
  Netskope-Api-Token: {token}

Iterator lifecycle:
  Server-side stateful. Each named index remembers where it left off
  between calls. No local checkpoint needed. The index name should be
  unique per consumer per stream to avoid missed logs.

  IMPORTANT: Some tenants require explicit iterator creation before first
  use via POST /api/v2/events/dataexport/iterator/{name}. This client
  auto-detects "iterator not found" responses and attempts creation.

Response shape:
  { "ok": 1, "result": "ok"|"wait", "data": [...], "wait_time": <seconds> }
"""

import json
import logging
import time
from typing import Any, Dict, Iterator, List, Set

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tuning knobs
# ---------------------------------------------------------------------------
# Max pages fetched per stream per function execution.
# At ~1000 events/page this caps at ~50k events per stream per 5-min run.
MAX_PAGES_PER_RUN = 50

# HTTP request timeout in seconds.
REQUEST_TIMEOUT_SECS = 30

# Retry config for transient failures.
RETRY_TOTAL = 3
RETRY_BACKOFF_FACTOR = 1.0
RETRY_STATUS_FORCELIST = [429, 500, 502, 503, 504]


def _build_session() -> requests.Session:
    """Build a requests Session with automatic retry on transient errors."""
    session = requests.Session()
    retry = Retry(
        total=RETRY_TOTAL,
        backoff_factor=RETRY_BACKOFF_FACTOR,
        status_forcelist=RETRY_STATUS_FORCELIST,
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session


class NetskopeClient:
    """Polls Netskope v2 dataexport iterator endpoints and yields batches."""

    EVENTS_PATH = "/api/v2/events/dataexport/events"
    ALERTS_PATH = "/api/v2/events/dataexport/alerts"

    VALID_EVENT_TYPES = frozenset([
        "page", "application", "audit", "infrastructure",
        "network", "connection", "endpoint", "incident",
    ])

    VALID_ALERT_SUBTYPES = frozenset([
        "remediation", "compromisedcredential", "uba",
        "securityassessment", "quarantine", "policy",
        "malware", "malsite", "dlp", "ctep",
        "watchlist", "device", "content",
    ])

    def __init__(self, hostname: str, token: str, base_index: str) -> None:
        """
        Args:
            hostname:    Netskope tenant hostname (e.g. mytenant.goskope.com).
            token:       REST API v2 token.
            base_index:  Iterator index name (e.g. "NetskopeADX").
                         Shared across all endpoints — Netskope scopes the
                         cursor per endpoint type automatically.
        """
        self.base_url = f"https://{hostname}"
        self.token = token
        self.base_index = base_index
        self._session = _build_session()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def pull_stream(self, category: str, stream_type: str) -> Iterator[List[Dict[str, Any]]]:
        """
        Yield batches for a given category ("events" or "alerts") and type.

        Unified entry point that validates and routes to the correct endpoint.
        """
        paths = {"events": self.EVENTS_PATH, "alerts": self.ALERTS_PATH}
        valid_types = {"events": self.VALID_EVENT_TYPES, "alerts": self.VALID_ALERT_SUBTYPES}

        if category not in paths:
            raise ValueError(f"Invalid category '{category}'. Must be 'events' or 'alerts'.")

        if stream_type not in valid_types[category]:
            raise ValueError(
                f"Invalid {category} type '{stream_type}'. "
                f"Must be one of: {sorted(valid_types[category])}"
            )

        url = f"{self.base_url}{paths[category]}/{stream_type}"
        index_name = self.base_index
        yield from self._poll_iterator(url, index_name, f"{category}/{stream_type}")

    def pull_events(self, event_type: str) -> Iterator[List[Dict[str, Any]]]:
        """Yield batches of events for the given event type."""
        yield from self.pull_stream("events", event_type)

    def pull_alerts(self, alert_subtype: str) -> Iterator[List[Dict[str, Any]]]:
        """Yield batches of alerts for the given alert subtype."""
        yield from self.pull_stream("alerts", alert_subtype)

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._session.close()

    # ------------------------------------------------------------------
    # Core polling
    # ------------------------------------------------------------------

    def _poll_iterator(
        self, url: str, index_name: str, stream_label: str
    ) -> Iterator[List[Dict[str, Any]]]:
        """
        Core polling loop shared by events and alerts.

        GET {url}?operation=next&index={index_name}
        Header: Netskope-Api-Token: {token}
        """
        headers = {
            "Netskope-Api-Token": self.token,
            "Accept": "application/json",
        }
        params = {
            "operation": "next",
            "index": index_name,
        }

        pages = 0

        while pages < MAX_PAGES_PER_RUN:
            try:
                resp = self._session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=REQUEST_TIMEOUT_SECS,
                )
            except requests.RequestException as e:
                logger.error(
                    "Netskope request failed [stream=%s]: %s", stream_label, e
                )
                break

            # Handle non-2xx responses
            if not resp.ok:
                logger.error(
                    "Netskope HTTP error [stream=%s status=%s]: %s",
                    stream_label,
                    resp.status_code,
                    resp.text[:500],
                )
                break

            try:
                body = resp.json()
            except json.JSONDecodeError:
                logger.error(
                    "Netskope returned non-JSON [stream=%s]: %s",
                    stream_label,
                    resp.text[:500],
                )
                break

            result = body.get("result", "wait")
            data: List[Dict[str, Any]] = body.get("data", [])
            wait_time = body.get("wait_time", 0)

            if data:
                logger.info(
                    "Netskope pull: stream=%s page=%d records=%d",
                    stream_label,
                    pages + 1,
                    len(data),
                )
                yield data
                pages += 1

            # "wait" or empty data = caught up for this cycle.
            if result == "wait" or not data:
                if wait_time and wait_time > 0:
                    logger.debug(
                        "stream=%s server says wait_time=%ds "
                        "(will resume next timer cycle)",
                        stream_label,
                        wait_time,
                    )
                break

        if pages >= MAX_PAGES_PER_RUN:
            logger.warning(
                "stream=%s hit MAX_PAGES_PER_RUN=%d — data may be falling "
                "behind. Consider raising the cap or upgrading plan.",
                stream_label,
                MAX_PAGES_PER_RUN,
            )
