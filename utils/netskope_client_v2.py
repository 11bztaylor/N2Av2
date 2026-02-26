"""
Netskope REST API v2 dataexport iterator client.

Uses the v2 dataexport iterator endpoints for streaming event/alert data.
This replaces the original client which incorrectly used /api/v2/events/data/
(the time-range query endpoint) with POST + JSON body.

Endpoint structure (GET with query params):
  Events:  GET /api/v2/events/dataexport/events/{type}?operation=next&index={name}
  Alerts:  GET /api/v2/events/dataexport/alerts/{subtype}?operation=next&index={name}

Valid event types:
  page, application, audit, infrastructure, network, connection, endpoint, incident

Valid alert subtypes:
  remediation, compromisedcredential, uba, securityassessment,
  quarantine, policy, malware, malsite, dlp, ctep,
  watchlist, device, content

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
  {
    "ok": 1,
    "result": "ok" | "wait",
    "data": [...],
    "wait_time": <seconds>
  }
  result "ok"   + data  -> batch returned, more may follow
  result "wait"  / empty -> caught up, stop until next timer fire
"""

import logging
import time
from typing import Any, Dict, Iterator, List, Optional, Set

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
    ITERATOR_PATH = "/api/v2/events/dataexport/iterator"

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
            base_index:  Base iterator name (e.g. "NetskopeADX").
                         Per-stream index becomes "{base_index}_{category}_{type}"
                         so each stream maintains independent progress.
        """
        self.base_url = f"https://{hostname}"
        self.token = token
        self.base_index = base_index
        self._session = _build_session()
        self._ensured_iterators: Set[str] = set()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def pull_events(self, event_type: str) -> Iterator[List[Dict[str, Any]]]:
        """
        Yield batches of events for the given event type.

        Valid types: page, application, audit, infrastructure,
                     network, connection, endpoint, incident.
        """
        if event_type not in self.VALID_EVENT_TYPES:
            raise ValueError(
                f"Invalid event type '{event_type}'. "
                f"Must be one of: {sorted(self.VALID_EVENT_TYPES)}"
            )
        url = f"{self.base_url}{self.EVENTS_PATH}/{event_type}"
        index_name = f"{self.base_index}_events_{event_type}"
        yield from self._poll_iterator(url, index_name, f"events/{event_type}")

    def pull_alerts(self, alert_subtype: str) -> Iterator[List[Dict[str, Any]]]:
        """
        Yield batches of alerts for the given alert subtype.

        Valid subtypes: remediation, compromisedcredential, uba,
                        securityassessment, quarantine, policy,
                        malware, malsite, dlp, ctep,
                        watchlist, device, content.
        """
        if alert_subtype not in self.VALID_ALERT_SUBTYPES:
            raise ValueError(
                f"Invalid alert subtype '{alert_subtype}'. "
                f"Must be one of: {sorted(self.VALID_ALERT_SUBTYPES)}"
            )
        url = f"{self.base_url}{self.ALERTS_PATH}/{alert_subtype}"
        index_name = f"{self.base_index}_alerts_{alert_subtype}"
        yield from self._poll_iterator(url, index_name, f"alerts/{alert_subtype}")

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._session.close()

    # ------------------------------------------------------------------
    # Iterator lifecycle
    # ------------------------------------------------------------------

    def _ensure_iterator(self, index_name: str) -> None:
        """
        Ensure the iterator exists on the Netskope side.

        Some tenants require explicit creation via:
          POST /api/v2/events/dataexport/iterator/{name}

        We call this once per index_name per client lifetime and cache
        the result. If creation returns 409 (already exists), that's fine.
        If the tenant auto-creates on first 'next' call, this is a harmless
        no-op that returns quickly.
        """
        if index_name in self._ensured_iterators:
            return

        url = f"{self.base_url}{self.ITERATOR_PATH}/{index_name}"
        headers = {
            "Netskope-Api-Token": self.token,
            "Accept": "application/json",
        }

        try:
            resp = self._session.post(
                url, headers=headers, timeout=REQUEST_TIMEOUT_SECS
            )

            if resp.status_code in (200, 201):
                logger.info("Iterator created: %s", index_name)
            elif resp.status_code == 409:
                # Already exists — expected on subsequent runs
                logger.debug("Iterator already exists: %s", index_name)
            elif resp.status_code == 400:
                # Some tenants don't require explicit creation and return
                # 400 on the creation endpoint. That's fine — the iterator
                # is implicitly created on first 'next' call.
                logger.debug(
                    "Iterator creation returned 400 for %s "
                    "(tenant may auto-create). Proceeding.",
                    index_name,
                )
            else:
                logger.warning(
                    "Iterator creation unexpected status=%s for %s: %s",
                    resp.status_code,
                    index_name,
                    resp.text[:300],
                )
        except requests.RequestException as e:
            logger.warning(
                "Iterator creation request failed for %s: %s "
                "(will attempt polling anyway)",
                index_name,
                e,
            )

        # Cache regardless of outcome — don't retry every 5 min
        self._ensured_iterators.add(index_name)

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
        # Ensure the iterator exists before first poll
        self._ensure_iterator(index_name)

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
            except ValueError:
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
