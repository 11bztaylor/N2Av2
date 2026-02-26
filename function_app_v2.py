"""
Netskope -> ADX ingestion function (v2).

Timer-triggered every 5 minutes. Polls the Netskope REST API v2
dataexport iterator endpoints and queues events/alerts into ADX.

Architecture:
  All streams land in Netskope_Raw staging table with:
    - TimeGenerated  (datetime, derived from $.timestamp via ingestion mapping)
    - StreamType     (string, injected by this function as "events_{type}" or "alerts_{subtype}")
    - RawData        (dynamic, full Netskope API payload)

  ADX update policies then fan out from Netskope_Raw to typed tables
  (see adx/tables/04_create_update_policies_v2.kql).

API endpoints:
  Events:  GET /api/v2/events/dataexport/events/{type}?operation=next&index={name}
  Alerts:  GET /api/v2/events/dataexport/alerts/{subtype}?operation=next&index={name}

Key differences from v1:
  - Correct v2 dataexport iterator endpoints (was using /api/v2/events/data/)
  - GET + query params (was POST + JSON body)
  - Correct auth header: Netskope-Api-Token (was Netskope-Token)
  - Alerts are per-subtype endpoints (was treating as single unified stream)
  - dlp removed from event streams (it's an alert subtype)
  - Per-batch error handling (was per-stream, causing data loss)
  - Ingestion mapping reference passed to ADX (was missing)
  - Clean toggle naming (was Sentinel-legacy naming with typos)
  - Added incident event type + watchlist/device/content alert subtypes
"""

import logging
import os

import azure.functions as func

from utils.adx_client_v2 import AdxClient
from utils.netskope_client_v2 import NetskopeClient

app = func.FunctionApp()
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
RAW_TABLE = "Netskope_Raw"
RAW_MAPPING = "Netskope_Raw_Mapping"

# ---------------------------------------------------------------------------
# Event stream registry: (v2_event_type, app_setting_toggle)
#
# These are the valid v2 dataexport event types.
# Each maps to: GET /api/v2/events/dataexport/events/{type}
# ---------------------------------------------------------------------------
EVENT_STREAMS = [
    ("page",           "IngestEventsPage"),
    ("application",    "IngestEventsApplication"),
    ("audit",          "IngestEventsAudit"),
    ("infrastructure", "IngestEventsInfrastructure"),
    ("network",        "IngestEventsNetwork"),
    ("connection",     "IngestEventsConnection"),
    ("endpoint",       "IngestEventsEndpoint"),
    ("incident",       "IngestEventsIncident"),
]

# ---------------------------------------------------------------------------
# Alert subtype registry: (v2_alert_subtype, app_setting_toggle)
#
# Each subtype is a SEPARATE v2 dataexport endpoint.
# Each maps to: GET /api/v2/events/dataexport/alerts/{subtype}
#
# NOTE: In Sentinel's connector, alerts were abstracted as a single stream.
#       In the actual v2 API, each subtype has its own endpoint and iterator.
# ---------------------------------------------------------------------------
ALERT_SUBTYPES = [
    ("remediation",           "IngestAlertsRemediation"),
    ("compromisedcredential", "IngestAlertsCompromisedCredential"),
    ("uba",                   "IngestAlertsUba"),
    ("securityassessment",    "IngestAlertsSecurityAssessment"),
    ("quarantine",            "IngestAlertsQuarantine"),
    ("policy",                "IngestAlertsPolicy"),
    ("malware",               "IngestAlertsMalware"),
    ("malsite",               "IngestAlertsMalsite"),
    ("dlp",                   "IngestAlertsDlp"),
    ("ctep",                  "IngestAlertsCtep"),
    ("watchlist",             "IngestAlertsWatchlist"),
    ("device",                "IngestAlertsDevice"),
    ("content",               "IngestAlertsContent"),
]


def _is_enabled(setting: str) -> bool:
    """Check if an app setting toggle is set to 'Yes' (case-insensitive)."""
    return os.environ.get(setting, "No").strip().lower() == "yes"


# ---------------------------------------------------------------------------
# Timer trigger - every 5 minutes
# ---------------------------------------------------------------------------
@app.timer_trigger(
    schedule="0 */5 * * * *",
    arg_name="timer",
    run_on_startup=False,
    use_monitor=True,
)
def netskope_ingest(timer: func.TimerRequest) -> None:
    """Main entry point. Fires every 5 min, pulls all enabled streams."""

    if timer.past_due:
        logger.warning(
            "Timer fired past due. Previous execution may have overrun the "
            "10-min Consumption plan limit. Consider Elastic Premium (EP1)."
        )

    # --- Read config from app settings ---
    hostname = os.environ["NetskopeHostname"]
    token    = os.environ["NetskopeApiToken"]
    index    = os.environ.get("NetskopeIndex", "").strip() or "NetskopeADX"
    adx_uri  = os.environ["ADX_CLUSTER_URI"]
    adx_db   = os.environ["ADX_DATABASE"]

    ns  = NetskopeClient(hostname, token, index)
    adx = AdxClient(adx_uri, adx_db)

    total = 0

    try:
        # --- Events ---
        for event_type, toggle in EVENT_STREAMS:
            if _is_enabled(toggle):
                total += _run_event_stream(ns, adx, event_type)
            else:
                logger.debug("events/%s disabled, skipping.", event_type)

        # --- Alerts (each subtype = separate v2 endpoint) ---
        for alert_subtype, toggle in ALERT_SUBTYPES:
            if _is_enabled(toggle):
                total += _run_alert_stream(ns, adx, alert_subtype)
            else:
                logger.debug("alerts/%s disabled, skipping.", alert_subtype)

    finally:
        ns.close()

    logger.info("Ingestion run complete. Total records queued: %d", total)


# ---------------------------------------------------------------------------
# Stream runners
# ---------------------------------------------------------------------------

def _run_event_stream(
    ns: NetskopeClient,
    adx: AdxClient,
    event_type: str,
) -> int:
    """
    Pull all available pages for one event stream and ingest into Netskope_Raw.

    Stamps each record with stream_type = "events_{event_type}" so ADX
    update policies can route to the correct typed table.

    Error handling is PER-BATCH: if one batch fails ADX ingest, the next
    batch is still attempted. This prevents the Netskope iterator from
    advancing past data we never ingested.
    """
    stream_label = f"events/{event_type}"
    count = 0

    for batch in ns.pull_events(event_type):
        try:
            for rec in batch:
                rec["stream_type"] = f"events_{event_type}"
            adx.ingest_batch(RAW_TABLE, batch, mapping_reference=RAW_MAPPING)
            count += len(batch)
        except Exception as e:
            logger.error(
                "stream=%s ingest failed on batch (records so far: %d): %s",
                stream_label, count, e,
            )
            # Continue to next batch — don't abandon remaining pages

    logger.info("stream=%s complete. %d records queued.", stream_label, count)
    return count


def _run_alert_stream(
    ns: NetskopeClient,
    adx: AdxClient,
    alert_subtype: str,
) -> int:
    """
    Pull all available pages for one alert subtype and ingest into Netskope_Raw.

    Stamps each record with stream_type = "alerts_{alert_subtype}".
    Same per-batch error handling as _run_event_stream.
    """
    stream_label = f"alerts/{alert_subtype}"
    count = 0

    for batch in ns.pull_alerts(alert_subtype):
        try:
            for rec in batch:
                rec["stream_type"] = f"alerts_{alert_subtype}"
            adx.ingest_batch(RAW_TABLE, batch, mapping_reference=RAW_MAPPING)
            count += len(batch)
        except Exception as e:
            logger.error(
                "stream=%s ingest failed on batch (records so far: %d): %s",
                stream_label, count, e,
            )

    logger.info("stream=%s complete. %d records queued.", stream_label, count)
    return count
