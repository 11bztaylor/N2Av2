"""
Netskope -> ADX ingestion function.

Timer-triggered every 5 minutes. Polls the Netskope REST API v2
dataexport iterator endpoints and queues events/alerts into ADX.

Architecture:
  All streams land in Netskope_Raw staging table with:
    - TimeGenerated  (datetime, derived from $.timestamp via ingestion mapping)
    - StreamType     (string, injected by this function as "events_{type}" or "alerts_{subtype}")
    - RawData        (dynamic, full Netskope API payload)

  ADX update policies then fan out from Netskope_Raw to typed tables
  (see adx/tables/04_create_update_policies.kql).

API endpoints:
  Events:  GET /api/v2/events/dataexport/events/{type}?operation=next&index={name}
  Alerts:  GET /api/v2/events/dataexport/alerts/{subtype}?operation=next&index={name}
"""

import logging
import os

import azure.functions as func
from azure.kusto.data.exceptions import KustoClientError, KustoServiceError

from utils.adx_client import AdxClient
from utils.netskope_client import NetskopeClient

app = func.FunctionApp()
logger = logging.getLogger(__name__)

# Wire LOG_LEVEL app setting to Python logging (default: INFO).
# NOTE: logging.basicConfig() is a no-op in Azure Functions because the host
# pre-configures the root logger before user code loads. Set levels directly.
_log_level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
logger.setLevel(_log_level)

# Wire AZURE_LOG_LEVEL for Azure/Kusto SDK loggers
# (controlled via Bicep detailedKustoLogging param).
_azure_log_level = getattr(
    logging, os.environ.get("AZURE_LOG_LEVEL", "WARNING").upper(), logging.WARNING
)
logging.getLogger("azure").setLevel(_azure_log_level)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
RAW_TABLE = "Netskope_Raw"
RAW_MAPPING = "Netskope_Raw_Mapping"

# ---------------------------------------------------------------------------
# Stream registry: (category, type/subtype, app_setting_toggle)
#
# Events map to:  GET /api/v2/events/dataexport/events/{type}
# Alerts map to:  GET /api/v2/events/dataexport/alerts/{subtype}
# ---------------------------------------------------------------------------
STREAMS = [
    # Events
    ("events", "page",           "IngestEventsPage"),
    ("events", "application",    "IngestEventsApplication"),
    ("events", "audit",          "IngestEventsAudit"),
    ("events", "infrastructure", "IngestEventsInfrastructure"),
    ("events", "network",        "IngestEventsNetwork"),
    ("events", "connection",     "IngestEventsConnection"),
    ("events", "endpoint",       "IngestEventsEndpoint"),
    ("events", "incident",       "IngestEventsIncident"),
    # Alerts (each subtype is a separate v2 endpoint)
    ("alerts", "remediation",           "IngestAlertsRemediation"),
    ("alerts", "compromisedcredential", "IngestAlertsCompromisedCredential"),
    ("alerts", "uba",                   "IngestAlertsUba"),
    ("alerts", "securityassessment",    "IngestAlertsSecurityAssessment"),
    ("alerts", "quarantine",            "IngestAlertsQuarantine"),
    ("alerts", "policy",                "IngestAlertsPolicy"),
    ("alerts", "malware",               "IngestAlertsMalware"),
    ("alerts", "malsite",               "IngestAlertsMalsite"),
    ("alerts", "dlp",                   "IngestAlertsDlp"),
    ("alerts", "ctep",                  "IngestAlertsCtep"),
    ("alerts", "watchlist",             "IngestAlertsWatchlist"),
    ("alerts", "device",                "IngestAlertsDevice"),
    ("alerts", "content",               "IngestAlertsContent"),
]


def _validate_config() -> None:
    """
    Validate required app settings at startup.

    Raises RuntimeError with a clear message listing all missing keys
    so the root cause is immediately visible in Application Insights.
    """
    required = [
        "NetskopeHostname",
        "NetskopeApiToken",
        "ADX_CLUSTER_URI",
        "ADX_DATABASE",
    ]
    missing = [key for key in required if not os.environ.get(key, "").strip()]
    if missing:
        logger.critical(
            "Missing required app settings: %s — function cannot start.",
            ", ".join(missing),
        )
        raise RuntimeError(
            f"Missing required app settings: {', '.join(missing)}"
        )


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

    # --- Validate required config before doing anything ---
    _validate_config()

    # --- Read config from app settings ---
    hostname = os.environ["NetskopeHostname"]
    token    = os.environ["NetskopeApiToken"]
    index    = os.environ.get("NetskopeIndex", "").strip() or "NetskopeADX"
    adx_uri  = os.environ["ADX_CLUSTER_URI"]
    adx_db   = os.environ["ADX_DATABASE"]
    mi_client_id = os.environ.get("MANAGED_IDENTITY_CLIENT_ID", "").strip() or None

    ns  = NetskopeClient(hostname, token, index)
    adx = AdxClient(adx_uri, adx_db, managed_identity_client_id=mi_client_id)

    # Log which streams are active for this run
    enabled = [
        f"{cat}/{stype}" for cat, stype, toggle in STREAMS if _is_enabled(toggle)
    ]
    logger.info(
        "Starting ingestion run. Enabled streams (%d): %s",
        len(enabled),
        ", ".join(enabled) or "(none)",
    )

    total = 0

    try:
        for category, stream_type, toggle in STREAMS:
            if _is_enabled(toggle):
                total += _run_stream(ns, adx, category, stream_type)
            else:
                logger.debug("%s/%s disabled, skipping.", category, stream_type)
    finally:
        ns.close()
        adx.close()

    logger.info("Ingestion run complete. Total records queued: %d", total)


# ---------------------------------------------------------------------------
# Stream runner
# ---------------------------------------------------------------------------

def _run_stream(
    ns: NetskopeClient,
    adx: AdxClient,
    category: str,
    stream_type: str,
) -> int:
    """
    Pull all available pages for one stream and ingest into Netskope_Raw.

    Stamps each record with stream_type = "{category}_{stream_type}" so ADX
    update policies can route to the correct typed table.

    Error handling is PER-BATCH: if one batch fails ADX ingest, the next
    batch is still attempted. This prevents the Netskope iterator from
    advancing past data we never ingested.
    """
    stream_label = f"{category}/{stream_type}"
    tag = f"{category}_{stream_type}"
    count = 0

    for batch in ns.pull_stream(category, stream_type):
        try:
            for rec in batch:
                rec["stream_type"] = tag
            adx.ingest_batch(RAW_TABLE, batch, mapping_reference=RAW_MAPPING)
            count += len(batch)
        except (KustoServiceError, KustoClientError, IOError, OSError) as e:
            logger.error(
                "stream=%s ingest failed on batch (records so far: %d): %s",
                stream_label, count, e,
            )
            # Continue to next batch — don't abandon remaining pages

    logger.info("stream=%s complete. %d records queued.", stream_label, count)
    return count
