# Netskope → Azure Data Explorer (ADX) Ingestion

[![Azure Functions](https://img.shields.io/badge/Azure%20Functions-v4%20Python%20v2-blue)](https://learn.microsoft.com/en-us/azure/azure-functions/)
[![Netskope API](https://img.shields.io/badge/Netskope-REST%20API%20v2-orange)](https://docs.netskope.com/en/rest-api-v2-overview/)
[![ADX](https://img.shields.io/badge/Azure%20Data%20Explorer-Queued%20Ingest-green)](https://learn.microsoft.com/en-us/azure/data-explorer/)

An Azure Function App that polls Netskope's REST API v2 dataexport iterator endpoints every 5 minutes and ingests events and alerts directly into Azure Data Explorer — **bypassing Microsoft Sentinel / Log Analytics entirely**.

Built as a direct replacement for the [Netskope v2 Sentinel connector](https://github.com/Azure/Azure-Sentinel/tree/master/Solutions/Netskopev2), re-targeted for ADX-native storage and querying.

---

## Architecture

```mermaid
flowchart LR
    subgraph Netskope["☁️ Netskope Tenant"]
        API["REST API v2<br/>dataexport iterators"]
    end

    subgraph Azure["☁️ Azure"]
        subgraph FuncApp["⚡ Function App<br/>(Timer: every 5 min)"]
            NS_Client["netskope_client_v2.py<br/>GET /events/{type}<br/>GET /alerts/{subtype}"]
            ADX_Client["adx_client_v2.py<br/>Queued Ingest<br/>(MULTIJSON)"]
        end

        subgraph ADX["📊 Azure Data Explorer"]
            Raw["Netskope_Raw<br/>(staging table)"]
            Mapping["Netskope_Raw_Mapping<br/>(JSON ingestion mapping)"]
            subgraph Typed["Typed Tables (via Update Policies)"]
                E1["NetskopeEventsPage"]
                E2["NetskopeEventsApplication"]
                E3["NetskopeEventsAudit"]
                E4["...5 more event tables"]
                A1["NetskopeAlertsPolicy"]
                A2["NetskopeAlertsMalware"]
                A3["NetskopeAlertsDlp"]
                A4["...10 more alert tables"]
            end
        end

        KV["🔑 Key Vault<br/>(optional)"]
        AI["📈 App Insights"]
    end

    API -- "Netskope-Api-Token" --> NS_Client
    NS_Client -- "batches" --> ADX_Client
    ADX_Client -- "Managed Identity" --> Raw
    Raw -- "update policy<br/>fan-out by StreamType" --> Typed
    Mapping -. "maps $.timestamp → TimeGenerated<br/>$.stream_type → StreamType<br/>$ → RawData" .-> Raw
    KV -. "secret ref" .-> FuncApp
    FuncApp --> AI
```

## Supported Streams (21 total)

| Category | Streams | Count |
|---|---|---|
| **Events** | `page`, `application`, `audit`, `infrastructure`, `network`, `connection`, `endpoint`, `incident` | 8 |
| **Alerts** | `remediation`, `compromisedcredential`, `uba`, `securityassessment`, `quarantine`, `policy`, `malware`, `malsite`, `dlp`, `ctep`, `watchlist`, `device`, `content` | 13 |

Each stream is independently toggleable via app settings (`Yes`/`No`). Disabled streams are skipped at zero cost.

---

## Repository Structure

```
NetskopetoADX/
├── function_app_v2.py              # Main entry point — timer trigger, stream orchestration
├── requirements_v2.txt             # Python dependencies (pin ranges)
├── azuredeploy_v2.json             # ARM template — Function App + Storage + App Insights
├── host.json                       # Azure Functions host config (10-min timeout)
├── local.settings.template.json    # App settings template — DO NOT commit with real tokens
├── CLAUDE.md                       # Developer reference & design decisions
├── README.md                       # ← You are here
├── utils/
│   ├── __init__.py
│   ├── netskope_client_v2.py       # Netskope v2 dataexport iterator client (GET + retry)
│   └── adx_client_v2.py            # ADX queued ingestion client (Managed Identity)
└── adx/
    └── tables/
        ├── 01_create_raw_table_v2.kql      # Netskope_Raw staging table + retention
        ├── 02_create_mapping_v2.kql        # JSON ingestion mapping (CRITICAL)
        ├── 03_create_typed_tables_v2.kql   # 21 typed tables + folder org + retention
        └── 04_create_update_policies_v2.kql # Transform functions + update policies
```

---

## Prerequisites

| Requirement | Details |
|---|---|
| **Netskope tenant** | REST API v2 token with dataexport permissions |
| **Azure subscription** | Resource group with Contributor access |
| **ADX cluster** | Existing cluster + database (Dev/Test SKU is fine for testing) |
| **Azure CLI** or **Portal** | For ARM deployment |
| **Azure Functions Core Tools** | For local testing and code deployment (`func` CLI) |

---

## Deployment

### Step 1 — Set up ADX tables

Run KQL files **in order** against your ADX database (via Kusto Web Explorer or Azure Data Explorer portal):

```
01_create_raw_table_v2.kql       → Creates Netskope_Raw + 90-day retention
02_create_mapping_v2.kql         → Creates Netskope_Raw_Mapping (JSON → columns)
03_create_typed_tables_v2.kql    → Creates 21 typed tables + folder org + retention
04_create_update_policies_v2.kql → Creates transform functions + update policies
```

> ⚠️ **Order matters.** Update policies reference tables that must exist first.

### Step 2 — Deploy Azure infrastructure

```bash
az deployment group create \
  --resource-group <YOUR_RG> \
  --template-file azuredeploy_v2.json \
  --parameters \
    functionAppName=<NAME> \
    storageAccountName=<STORAGE_NAME> \
    appInsightsName=<AI_NAME> \
    adxClusterUri=https://<CLUSTER>.<REGION>.kusto.windows.net \
    adxDatabaseName=<DB_NAME> \
    netskopeHostname=<TENANT>.goskope.com \
    netskopeApiToken=<YOUR_V2_TOKEN> \
    ingestEventsPage=Yes \
    ingestEventsApplication=Yes \
    ingestAlertsPolicy=Yes \
    ingestAlertsMalware=Yes \
    ingestAlertsDlp=Yes
```

The ARM template outputs a `grantAdxIngestorRole` command — **copy and run it in ADX**:

```kusto
.add database <DB> ingestors ('aadapp=<PRINCIPAL_ID>')
```

> ⚠️ **This step is required.** Without it, data silently fails to ingest.

### Step 3 — Deploy function code

Rename files for Azure Functions runtime:

```bash
mv function_app_v2.py function_app.py
mv requirements_v2.txt requirements.txt
```

Then publish:

```bash
func azure functionapp publish <YOUR_FUNCTION_APP_NAME>
```

### Step 4 — Verify

After ~10 minutes (queued ingestion latency), check ADX:

```kusto
Netskope_Raw
| take 10

NetskopeEventsPage
| take 10
```

---

## How It Works

```mermaid
sequenceDiagram
    participant Timer as ⏱️ Timer (5 min)
    participant Func as ⚡ Function App
    participant NS as ☁️ Netskope API
    participant ADX as 📊 ADX

    Timer->>Func: Fire
    loop For each enabled stream
        Func->>NS: GET /dataexport/events/{type}?operation=next&index={name}
        NS-->>Func: { result: "ok", data: [...] }
        Func->>Func: Stamp stream_type on each record
        Func->>ADX: Queued ingest → Netskope_Raw (MULTIJSON)
        ADX->>ADX: Update policy → fan out to typed table
        Note over Func,NS: Repeat until result="wait"
    end
    Func->>Func: Log total records queued
```

**Key design decisions:**

- **Staging table pattern** — all 21 streams land in `Netskope_Raw` first. Single ingestion target, replay capability, schema evolution decoupled from API.
- **Dynamic `RawData` column** — typed tables store the full payload as `dynamic`. No per-type schema maintenance. Query with `| extend user = tostring(RawData.user)`.
- **Per-batch error handling** — if one ADX ingest batch fails, the next batch still runs. Prevents the Netskope iterator from advancing past data we never ingested.
- **Server-side stateful iterators** — Netskope tracks position per index name. No local checkpoint files needed.
- **Auto iterator creation** — handles tenants that require explicit creation (409/400 gracefully ignored).

---

## Configuration Reference

### Core Settings

| Setting | Required | Example |
|---|---|---|
| `NetskopeHostname` | Yes | `mytenant.goskope.com` |
| `NetskopeApiToken` | Yes | v2 REST API token |
| `NetskopeIndex` | No | `NetskopeADX` (default) |
| `ADX_CLUSTER_URI` | Yes | `https://cluster.region.kusto.windows.net` |
| `ADX_DATABASE` | Yes | `NetskopeDB` |

### Stream Toggles

All default to `No` unless overridden in the ARM deploy. Set to `Yes` to enable.

**Events:** `IngestEventsPage`, `IngestEventsApplication`, `IngestEventsAudit`, `IngestEventsInfrastructure`, `IngestEventsNetwork`, `IngestEventsConnection`, `IngestEventsEndpoint`, `IngestEventsIncident`

**Alerts:** `IngestAlertsRemediation`, `IngestAlertsCompromisedCredential`, `IngestAlertsUba`, `IngestAlertsSecurityAssessment`, `IngestAlertsQuarantine`, `IngestAlertsPolicy`, `IngestAlertsMalware`, `IngestAlertsMalsite`, `IngestAlertsDlp`, `IngestAlertsCtep`, `IngestAlertsWatchlist`, `IngestAlertsDevice`, `IngestAlertsContent`

---

## ADX Table Organization

```mermaid
graph TD
    subgraph "Netskope/Raw"
        Raw["Netskope_Raw<br/><i>90-day retention</i>"]
    end
    subgraph "Netskope/Events"
        EP["NetskopeEventsPage"]
        EA["NetskopeEventsApplication"]
        EAu["NetskopeEventsAudit"]
        EI["NetskopeEventsInfrastructure"]
        EN["NetskopeEventsNetwork"]
        EC["NetskopeEventsConnection"]
        EEn["NetskopeEventsEndpoint"]
        EIn["NetskopeEventsIncident"]
    end
    subgraph "Netskope/Alerts"
        AR["NetskopeAlertsRemediation"]
        ACC["NetskopeAlertsCompromisedCredential"]
        AU["NetskopeAlertsUba"]
        ASA["NetskopeAlertsSecurityAssessment"]
        AQ["NetskopeAlertsQuarantine"]
        AP["NetskopeAlertsPolicy"]
        AM["NetskopeAlertsMalware"]
        AMs["NetskopeAlertsMalsite"]
        AD["NetskopeAlertsDlp"]
        AC["NetskopeAlertsCtep"]
        AW["NetskopeAlertsWatchlist"]
        ADe["NetskopeAlertsDevice"]
        ACo["NetskopeAlertsContent"]
    end

    Raw -->|"update policy<br/>WHERE StreamType"| EP & EA & EAu & EI & EN & EC & EEn & EIn
    Raw -->|"update policy<br/>WHERE StreamType"| AR & ACC & AU & ASA & AQ & AP & AM & AMs & AD & AC & AW & ADe & ACo

    style Raw fill:#FFA500,color:#000
    style EP fill:#4A90D9,color:#fff
    style EA fill:#4A90D9,color:#fff
    style EAu fill:#4A90D9,color:#fff
    style EI fill:#4A90D9,color:#fff
    style EN fill:#4A90D9,color:#fff
    style EC fill:#4A90D9,color:#fff
    style EEn fill:#4A90D9,color:#fff
    style EIn fill:#4A90D9,color:#fff
    style AR fill:#D94A4A,color:#fff
    style ACC fill:#D94A4A,color:#fff
    style AU fill:#D94A4A,color:#fff
    style ASA fill:#D94A4A,color:#fff
    style AQ fill:#D94A4A,color:#fff
    style AP fill:#D94A4A,color:#fff
    style AM fill:#D94A4A,color:#fff
    style AMs fill:#D94A4A,color:#fff
    style AD fill:#D94A4A,color:#fff
    style AC fill:#D94A4A,color:#fff
    style AW fill:#D94A4A,color:#fff
    style ADe fill:#D94A4A,color:#fff
    style ACo fill:#D94A4A,color:#fff
```

| Table Group | Retention | Count |
|---|---|---|
| `Netskope_Raw` (staging) | 90 days | 1 |
| Event tables | 180 days | 8 |
| Alert tables | 365 days | 13 |

---

## Differences from the Sentinel Connector

| Area | Sentinel v2 Connector | This Project (ADX-native) |
|---|---|---|
| Destination | Log Analytics workspace | Azure Data Explorer |
| API endpoint | `/api/v2/events/data/{type}` (time-range) | `/api/v2/events/dataexport/{events\|alerts}/{type}` (iterator) |
| HTTP method | POST + JSON body | GET + query params |
| Auth header | `Netskope-Token` | `Netskope-Api-Token` |
| Alerts | Single unified stream | Per-subtype endpoints (13 separate) |
| `dlp` stream | Listed as event type (wrong) | Correctly placed as alert subtype |
| Error handling | Per-stream (data loss on failure) | Per-batch (resilient) |
| Retry logic | None | urllib3 Retry with backoff on 429/5xx |
| Ingestion mapping | Missing (data malformed) | `Netskope_Raw_Mapping` with epoch→datetime transform |
| Schema approach | Flattened columns per table | `dynamic RawData` — query-time schema |
| Stream count | 17 (missing 4) | 21 (complete) |

---

## Gotchas & Troubleshooting

- **Iterator names are permanent.** If you delete and recreate with the same name, you may miss data or get duplicates. Use unique index names per consumer.
- **`dlp` is an alert subtype**, not an event type. The Sentinel connector had this wrong.
- **Queued ingestion latency** is 5–10 minutes. This is normal ADX behavior.
- **Run the `grantAdxIngestorRole` command** from the ARM output. Without it, data silently fails.
- **`device`/`content`/`incident`** endpoint availability varies by Netskope tenant/license tier. Per-batch error handling means unsupported endpoints won't break other streams.
- **Linux Consumption plan** is being retired Sept 2028. Plan migration to Flex Consumption.
- Check your Netskope Swagger docs at `https://<tenant>.goskope.com/apidocs/` to verify available endpoints.

---

## License

MIT
