"""
ADX Queued Ingestion client.

Uses the Function App's Managed Identity (System or User Assigned) to
authenticate. No credentials stored in code or config.

Queued ingestion is the production-recommended path:
  - Asynchronous: doesn't block function execution
  - Internally batched and retried by ADX
  - Tolerant of transient cluster issues
  - Ingestion may take 5-10 min to materialise in query results (normal)

Pre-requisite: the Managed Identity must have "Database Ingestor" role:
  .add database <db> ingestors ('aadapp=<principal-id>')

The Principal ID is the "functionAppPrincipalId" output from Bicep deployment.
"""

import io
import json
import logging
from typing import Any, Dict, List, Optional

from azure.identity import ManagedIdentityCredential
from azure.kusto.data import KustoConnectionStringBuilder, DataFormat
from azure.kusto.ingest import (
    IngestionProperties,
    QueuedIngestClient,
    ReportLevel,
)

logger = logging.getLogger(__name__)


class AdxClient:
    """Wraps ADX queued ingestion for a single database."""

    def __init__(self, cluster_uri: str, database: str) -> None:
        """
        Args:
            cluster_uri: Query endpoint, e.g. https://<cluster>.<region>.kusto.windows.net
            database:    Target ADX database name.

        The ingest URI is derived automatically:
          https://ingest-<cluster>.<region>.kusto.windows.net
        """
        self.database = database

        # Derive the ingestion endpoint from the query endpoint
        ingest_uri = cluster_uri.replace("https://", "https://ingest-", 1)

        credential = ManagedIdentityCredential()
        kcsb = KustoConnectionStringBuilder.with_azure_token_credential(
            ingest_uri, credential
        )
        self._client = QueuedIngestClient(kcsb)

        logger.info(
            "ADX client initialised: ingest_uri=%s database=%s",
            ingest_uri,
            database,
        )

    def ingest_batch(
        self,
        table: str,
        records: List[Dict[str, Any]],
        mapping_reference: Optional[str] = None,
    ) -> None:
        """
        Ingest a list of dicts as JSON-lines into the target table.

        Args:
            table:             Target ADX table name (e.g. "Netskope_Raw").
            records:           List of dicts to ingest.
            mapping_reference: Name of a pre-created JSON ingestion mapping on
                               the target table (e.g. "Netskope_Raw_Mapping").
                               When set, ADX uses this mapping to route JSON
                               fields to table columns. REQUIRED for Netskope_Raw.

        Raises on ingest failure so the caller can log and continue with
        other streams rather than silently swallowing data loss.
        """
        if not records:
            return

        # Serialise as JSON-lines (MULTIJSON = one JSON object per line)
        buf = io.BytesIO()
        for rec in records:
            buf.write((json.dumps(rec) + "\n").encode("utf-8"))
        buf.seek(0)

        props = IngestionProperties(
            database=self.database,
            table=table,
            data_format=DataFormat.MULTIJSON,
            report_level=ReportLevel.FailuresOnly,
            ingestion_mapping_reference=mapping_reference,
        )

        self._client.ingest_from_stream(buf, ingestion_properties=props)
        logger.info(
            "Queued for ingestion: table=%s records=%d mapping=%s",
            table,
            len(records),
            mapping_reference or "(auto)",
        )

    def close(self) -> None:
        """Close the underlying QueuedIngestClient and release connection pools."""
        try:
            self._client.close()
        except Exception:
            pass
