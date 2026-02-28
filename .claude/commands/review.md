# Source-Validated Code Review

Perform a thorough code review that validates every component against its official documentation and SDK source.

## Process

1. **Read all project files** — Python, Bicep, KQL, requirements.txt, host.json
2. **For each component**, look up the official docs and validate:

### Checklist

| Component | What to validate | Official source |
|---|---|---|
| Netskope API v2 | Auth header, endpoint paths, iterator lifecycle, cursor behavior | https://docs.netskope.com/en/rest-api-v2-overview/ |
| Azure Kusto SDK | `QueuedIngestClient`, `ingest_from_stream`, `IngestionProperties`, `DataFormat`, `ReportLevel` | https://learn.microsoft.com/en-us/azure/data-explorer/python-ingest-data and https://github.com/Azure/azure-kusto-python |
| ADX JSON formats | `JSON` vs `MULTIJSON`, ingestion mapping, transforms like `DateTimeFromUnixSeconds` | https://learn.microsoft.com/en-us/azure/data-explorer/ingest-json-formats |
| Azure Functions | Timer trigger syntax, `run_on_startup`, `use_monitor`, `past_due`, `functionTimeout`, logging | https://learn.microsoft.com/en-us/azure/azure-functions/functions-reference-python |
| Azure Identity | `ManagedIdentityCredential` constructor, `client_id` for user-assigned, `close()` method | https://learn.microsoft.com/en-us/python/api/azure-identity/azure.identity.managedidentitycredential |
| requests/urllib3 | `Retry` params (`total`, `backoff_factor`, `status_forcelist`, `allowed_methods`, `respect_retry_after_header`) | https://urllib3.readthedocs.io/en/stable/reference/urllib3.util.html |
| Bicep | Function App resource structure, `kind`, `reserved`, app settings, Key Vault RBAC, Flex Consumption migration | https://learn.microsoft.com/en-us/azure/azure-functions/functions-infrastructure-as-code |
| KQL | Update policies (`IsTransactional`, `PropagateIngestionProperties`), ingestion mappings, retention policies | https://learn.microsoft.com/en-us/azure/data-explorer/kusto/management/ |

3. **Use WebSearch/WebFetch** to pull current docs for any component where behavior may have changed
4. **Cross-check consistency** — stream names match between Python STREAMS list, KQL transforms, update policies, and Bicep toggles
5. **Report findings** in a table: severity (info/low/medium/high), description, action needed

## Output format

For each component, state: what the code does, what the docs say, and whether it's correct. Flag mismatches with severity. Provide source URLs for every finding.
