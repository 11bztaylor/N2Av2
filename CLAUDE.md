# Netskope -> ADX Function App — CLAUDE.md

## What This Is
Azure Function App (Python v2 model, timer-triggered) that pulls events and alerts from the Netskope REST API v2 dataexport iterator endpoints and ingests them into Azure Data Explorer (ADX).

## Architecture
```
Netskope v2 API  --->  Function App  --->  ADX Netskope_Raw  --->  Typed Tables
  (dataexport          (every 5 min)       (staging table)        (via update policies)
   iterator GET)
```

All streams land in `Netskope_Raw` with 3 columns: `TimeGenerated`, `StreamType`, `RawData (dynamic)`. Update policies fan out to typed tables filtered by `StreamType`.

## File Layout
```
N2Av2/
├── function_app.py                # Main entry point (timer trigger)
├── requirements.txt               # Python dependencies
├── main.bicep                     # Bicep template (Function App + Storage + App Insights + Key Vault)
├── azuredeploy_v2.json            # DEPRECATED ARM template (kept for reference)
├── host.json                      # Azure Functions host config
├── local.settings.template.json   # App settings template (DO NOT commit with real tokens)
├── .gitignore                     # Prevents committing local.settings.json and build artifacts
├── CLAUDE.md                      # This file
├── utils/
│   ├── __init__.py
│   ├── netskope_client.py         # Netskope v2 dataexport iterator client
│   └── adx_client.py              # ADX queued ingestion client with mapping support
└── adx/
    └── tables/
        ├── 01_create_raw_table_v2.kql      # Netskope_Raw table + retention
        ├── 02_create_mapping_v2.kql        # JSON ingestion mapping (CRITICAL)
        ├── 03_create_typed_tables_v2.kql   # Per-type/subtype tables + retention
        └── 04_create_update_policies_v2.kql # Transform functions + update policies
```

## Deploy Order
1. Run KQL files 01-04 in ADX **in order** to set up tables, mapping, and update policies
2. Deploy Bicep template (`main.bicep`) — creates Function App + infra + Key Vault
3. Copy the `grantAdxIngestorRole` output from deployment and run it in ADX
4. Deploy function code: `func azure functionapp publish <YOUR_FUNCTION_APP_NAME>`

## Key Design Decisions
- **Staging table pattern**: Everything lands in `Netskope_Raw` first. Single ingestion target, replay capability, schema evolution decoupled from API ingestion.
- **Dynamic `RawData` column**: Typed tables store the full payload as `dynamic` instead of flattening to explicit columns. Query with `| extend user = tostring(RawData.user)`.
- **Per-batch error handling**: If one ADX ingest batch fails, the next batch still runs. Prevents the Netskope iterator from advancing past data we never ingested.
- **Separate alert endpoints**: Each alert subtype is its own v2 dataexport endpoint. Not a unified stream.
- **Auto iterator creation**: Client attempts to create iterators on first use. Handles both explicit-creation and auto-creation tenants.
- **Key Vault integration**: API token stored in Key Vault, referenced via `@Microsoft.KeyVault(SecretUri=...)`. Three modes: `none`, `existing`, `create`.

## Netskope API v2 Quick Reference
```
Events:  GET /api/v2/events/dataexport/events/{type}?operation=next&index={name}
Alerts:  GET /api/v2/events/dataexport/alerts/{subtype}?operation=next&index={name}
Auth:    Netskope-Api-Token: {token}
```

**Valid event types:** page, application, audit, infrastructure, network, connection, endpoint, incident

**Valid alert subtypes:** remediation, compromisedcredential, uba, securityassessment, quarantine, policy, malware, malsite, dlp, ctep, watchlist, device, content

## App Settings Reference
| Setting | Example | Required |
|---|---|---|
| NetskopeHostname | mytenant.goskope.com | Yes |
| NetskopeApiToken | (v2 token, stored in Key Vault) | Yes |
| NetskopeIndex | NetskopeADX | No (defaults to NetskopeADX) |
| ADX_CLUSTER_URI | https://mycluster.westus2.kusto.windows.net | Yes |
| ADX_DATABASE | NetskopeDB | Yes |
| LOG_LEVEL | INFO | No (defaults to INFO) |
| AZURE_LOG_LEVEL | WARNING | No (defaults to WARNING, set to INFO for Kusto SDK debug) |
| MANAGED_IDENTITY_CLIENT_ID | (UAMI client ID) | No (empty = system-assigned, set for user-assigned) |
| IngestEvents{Type} | Yes / No | Per stream |
| IngestAlerts{Subtype} | Yes / No | Per stream |

## Environment Quirks
- `az` CLI not on bash PATH. Use: `powershell.exe -Command "& 'C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd' <args>"`
- `python` / `py` not on bash PATH. Use `python3` instead.
- Bicep validation: `powershell.exe -Command "& 'C:\Program Files\Microsoft SDKs\Azure\CLI2\wbin\az.cmd' bicep build --file main.bicep"`

## Git
- Repo root is N2Av2/ (not the parent NetskopetoADX/ directory)
- Remote: https://github.com/11bztaylor/N2Av2.git
- Main branch: `main`

## Completed Decisions
- ARM template deprecated in favor of Bicep (main.bicep)
- _v2 suffixes removed from all Python files and requirements.txt
- Key Vault integration: 3 modes (none/existing/create), default is 'create'
- logging.basicConfig() is a no-op in Azure Functions — use logger.setLevel() directly
- AZURE_LOG_LEVEL controls Kusto SDK verbosity (wired to detailedKustoLogging Bicep param)
- Unified STREAMS registry replaces separate EVENT_STREAMS + ALERT_SUBTYPES lists
- pull_stream() is the unified entry point; pull_events/pull_alerts are thin wrappers

## Gotchas
- The Netskope iterator is **server-side stateful**. If you delete and recreate with the same name, you may miss data or get duplicates.
- `dlp` is an **alert subtype**, NOT an event type.
- Queued ingestion may take **5-10 min** to materialise in ADX query results. This is normal.
- The Bicep output includes a `grantAdxIngestorRole` KQL command — **you must run this in ADX** or data silently fails to ingest.
- `device`/`content`/`incident` endpoint availability varies by tenant/license. Per-batch error handling means unsupported endpoints won't break other streams.
- Linux Consumption plan is being **retired Sept 2028**. Plan migration to Flex Consumption.
