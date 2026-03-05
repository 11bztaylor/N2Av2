// ═══════════════════════════════════════════════════════════════════════════════
// Netskope → Azure Data Explorer (ADX) Ingestion Pipeline
// Infrastructure-as-Code (Bicep)
//
// Deploys: Storage Account, Application Insights, App Service Plan (Consumption),
//          Function App (Python/Linux), and optionally a Key Vault for secret mgmt.
//
// Usage:
//   az deployment group create \
//     --resource-group <RG> \
//     --template-file main.bicep \
//     --parameters functionAppName=<name> storageAccountName=<name> \
//                  appInsightsName=<name> adxClusterUri=<uri> \
//                  adxDatabaseName=<db> netskopeHostname=<host> \
//                  netskopeApiToken=<token>
//
// After deployment, run the KQL command from the grantAdxIngestorRole output
// to authorize the Function App to ingest data into ADX.
// ═══════════════════════════════════════════════════════════════════════════════

targetScope = 'resourceGroup'

// ─── Core Infrastructure ────────────────────────────────────────────────────

@description('Name of the Azure Function App. Must be globally unique.')
@minLength(2)
@maxLength(60)
param functionAppName string

@description('Storage Account for Function App runtime state. Lowercase, no hyphens, globally unique.')
@minLength(3)
@maxLength(24)
param storageAccountName string

@description('Application Insights instance name for monitoring and alerting.')
param appInsightsName string

@description('Azure region for all resources. Defaults to the resource group location.')
param location string = resourceGroup().location

@description('Python runtime version. 3.12 RECOMMENDED for performance and security patches.')
@allowed([
  '3.10'
  '3.11'
  '3.12'
])
param pythonVersion string = '3.12'

// ─── Identity & Security ────────────────────────────────────────────────────

@description('Managed identity for ADX and Key Vault authentication. SystemAssigned = simpler, auto-managed lifecycle. UserAssigned = more control, shared across resources, survives redeploys.')
@allowed([
  'SystemAssigned'
  'UserAssigned'
])
param managedIdentityType string = 'SystemAssigned'

@description('Full resource ID of the User Assigned Managed Identity. Required when managedIdentityType is UserAssigned. Format: /subscriptions/.../resourceGroups/.../providers/Microsoft.ManagedIdentity/userAssignedIdentities/<name>')
param userAssignedIdentityResourceId string = ''

@description('Principal (Object) ID of the User Assigned Managed Identity. Required when managedIdentityType is UserAssigned. Find in Portal: UAMI resource > Properties > Principal ID.')
param userAssignedPrincipalId string = ''

@description('Client (Application) ID of the User Assigned Managed Identity. Required when managedIdentityType is UserAssigned. Find in Portal: UAMI resource > Properties > Client ID. Used by the Python SDK to select the correct identity at runtime.')
param userAssignedClientId string = ''

// ─── Key Vault ──────────────────────────────────────────────────────────────

@description('Secret management strategy for the Netskope API token. "none" = plaintext app setting (dev/test ONLY). "existing" = reference a secret already stored in your Key Vault. "create" = deploy a new Key Vault and store the token. RECOMMENDED: "existing" or "create" for production.')
@allowed([
  'none'
  'existing'
  'create'
])
param keyVaultOption string = 'create'

@description('Key Vault name. For "create": name for the new vault (globally unique, 3-24 chars). For "existing": name of your vault (must be in the same resource group, RBAC authorization must be enabled). Required when keyVaultOption is not "none".')
@minLength(0)
@maxLength(24)
param keyVaultName string = ''

@description('Secret name in Key Vault holding the Netskope API token.')
param keyVaultSecretName string = 'NetskopeApiToken'

@description('Key Vault SKU. "standard" is sufficient for secret storage. "premium" adds HSM-backed keys (not needed here).')
@allowed([
  'standard'
  'premium'
])
param keyVaultSku string = 'standard'

// ─── ADX Configuration ──────────────────────────────────────────────────────

@description('ADX cluster query endpoint. Example: https://mycluster.eastus.kusto.windows.net')
param adxClusterUri string

@description('Target ADX database name for Netskope data ingestion.')
param adxDatabaseName string

// ─── Netskope Configuration ─────────────────────────────────────────────────

@description('Netskope tenant hostname. Example: mytenant.goskope.com')
param netskopeHostname string

@secure()
@description('Netskope REST API v2 token. Required when keyVaultOption is "none" or "create". Not used when "existing" (token already in Key Vault).')
param netskopeApiToken string

@description('Iterator index name shared across all endpoints. Netskope scopes the cursor per endpoint type automatically. WARNING: Changing after initial deployment may cause data gaps or duplicates.')
param netskopeIndex string = 'NetskopeADX'

// ─── Logging & Monitoring ───────────────────────────────────────────────────

@description('Python application log level. DEBUG = very verbose (troubleshooting). INFO = normal operations (RECOMMENDED). WARNING/ERROR = quiet.')
@allowed([
  'DEBUG'
  'INFO'
  'WARNING'
  'ERROR'
])
param logLevel string = 'INFO'

@description('Enable verbose Azure Kusto SDK logging. Yes = detailed ADX connection and ingestion logs (troubleshooting). No = quiet (RECOMMENDED for normal operations).')
@allowed([
  'Yes'
  'No'
])
param detailedKustoLogging string = 'No'

// ─── Event Stream Toggles ───────────────────────────────────────────────────
// Each toggle enables/disables pulling that event type from Netskope.
// Each active stream adds API calls every 5 minutes and ADX storage (180-day retention).

@description('Web page access events. Typically the highest-volume stream. RECOMMENDED for web security monitoring.')
@allowed([ 'Yes', 'No' ])
param ingestEventsPage string = 'Yes'

@description('Cloud app activity events. RECOMMENDED for SaaS security and shadow IT detection.')
@allowed([ 'Yes', 'No' ])
param ingestEventsApplication string = 'Yes'

@description('Netskope admin audit trail. RECOMMENDED for compliance and change tracking.')
@allowed([ 'Yes', 'No' ])
param ingestEventsAudit string = 'Yes'

@description('Infrastructure events (CASB/SWG platform logs). Enable for platform-level diagnostics.')
@allowed([ 'Yes', 'No' ])
param ingestEventsInfrastructure string = 'No'

@description('Network events (firewall, IPS). Enable if using Netskope Intelligent SSE.')
@allowed([ 'Yes', 'No' ])
param ingestEventsNetwork string = 'No'

@description('Connection events (tunnel, ZTNA). Enable for zero-trust network access monitoring.')
@allowed([ 'Yes', 'No' ])
param ingestEventsConnection string = 'No'

@description('Endpoint events (NPA client, device posture). Enable if using Netskope Private Access.')
@allowed([ 'Yes', 'No' ])
param ingestEventsEndpoint string = 'No'

@description('Incident events. Enable for security incident correlation. May not be available on all Netskope licenses.')
@allowed([ 'Yes', 'No' ])
param ingestEventsIncident string = 'No'

// ─── Alert Subtype Toggles ──────────────────────────────────────────────────
// Each alert subtype has its own Netskope API endpoint and ADX iterator.
// Alerts use 365-day retention in ADX (vs 180 days for events).

@description('Remediation action alerts (quarantine, block, coach). Low volume.')
@allowed([ 'Yes', 'No' ])
param ingestAlertsRemediation string = 'No'

@description('Compromised credential alerts. RECOMMENDED for identity security. Requires threat protection license.')
@allowed([ 'Yes', 'No' ])
param ingestAlertsCompromisedCredential string = 'No'

@description('User Behavior Analytics (UBA) alerts. Enable for insider threat detection.')
@allowed([ 'Yes', 'No' ])
param ingestAlertsUba string = 'No'

@description('Security posture assessment alerts. Enable for CSPM use cases.')
@allowed([ 'Yes', 'No' ])
param ingestAlertsSecurityAssessment string = 'No'

@description('Quarantine action alerts. Enable to track file quarantine events.')
@allowed([ 'Yes', 'No' ])
param ingestAlertsQuarantine string = 'No'

@description('Real-time policy violation alerts. RECOMMENDED — core security alerting stream.')
@allowed([ 'Yes', 'No' ])
param ingestAlertsPolicy string = 'Yes'

@description('Malware detection alerts. RECOMMENDED for threat protection. Includes cloud and inline scan results.')
@allowed([ 'Yes', 'No' ])
param ingestAlertsMalware string = 'Yes'

@description('Malicious site (malsite) alerts. RECOMMENDED for web threat protection.')
@allowed([ 'Yes', 'No' ])
param ingestAlertsMalsite string = 'Yes'

@description('Data Loss Prevention (DLP) alerts. RECOMMENDED for data protection compliance.')
@allowed([ 'Yes', 'No' ])
param ingestAlertsDlp string = 'Yes'

@description('Client Traffic Exploit Prevention (CTEP) alerts. Enable if using advanced threat protection.')
@allowed([ 'Yes', 'No' ])
param ingestAlertsCtep string = 'No'

@description('Watchlist alerts. Enable to track activity from monitored users/entities.')
@allowed([ 'Yes', 'No' ])
param ingestAlertsWatchlist string = 'No'

@description('Device classification alerts. Enable for endpoint compliance. May not be available on all licenses.')
@allowed([ 'Yes', 'No' ])
param ingestAlertsDevice string = 'No'

@description('Content classification alerts (file/object). Enable for data-centric security. May not be available on all licenses.')
@allowed([ 'Yes', 'No' ])
param ingestAlertsContent string = 'No'

// ═══════════════════════════════════════════════════════════════════════════════
// Variables
// ═══════════════════════════════════════════════════════════════════════════════

var hostingPlanName = '${functionAppName}-plan'
var resourceTags = {
  Purpose: 'Netskope-Log-Segregation'
}
var isUserAssigned = managedIdentityType == 'UserAssigned'

var identityBlock = isUserAssigned
  ? {
      type: 'UserAssigned'
      userAssignedIdentities: {
        '${userAssignedIdentityResourceId}': {}
      }
    }
  : {
      type: 'SystemAssigned'
    }

// Storage connection string (key-based; Managed Identity for storage requires Functions v4.25+)
var storageConnectionString = 'DefaultEndpointsProtocol=https;AccountName=${storageAccountName};AccountKey=${storageAccount.listKeys().keys[0].value};EndpointSuffix=${environment().suffixes.storage}'

// Key Vault secret URI — constructed from params (no resource reference needed)
var kvSecretUri = 'https://${keyVaultName}${environment().suffixes.keyvaultDns}/secrets/${keyVaultSecretName}/'

// Resolve the Netskope API token app setting value based on Key Vault option
var netskopeTokenSetting = keyVaultOption != 'none'
  ? '@Microsoft.KeyVault(SecretUri=${kvSecretUri})'
  : netskopeApiToken

// Key Vault Secrets User built-in role ID
var kvSecretsUserRoleId = '4633458b-17de-408a-b874-0445c86b69e6'

// ═══════════════════════════════════════════════════════════════════════════════
// Resources
// ═══════════════════════════════════════════════════════════════════════════════

// ─── Storage Account ────────────────────────────────────────────────────────

resource storageAccount 'Microsoft.Storage/storageAccounts@2024-01-01' = {
  name: storageAccountName
  location: location
  tags: resourceTags
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    supportsHttpsTrafficOnly: true
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
  }
}

// ─── Application Insights ───────────────────────────────────────────────────

resource appInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: appInsightsName
  location: location
  tags: resourceTags
  kind: 'web'
  properties: {
    Application_Type: 'web'
    Request_Source: 'rest'
  }
}

// ─── App Service Plan (Linux Consumption) ───────────────────────────────────
// NOTE: Linux Consumption (Y1) is being retired Sept 2028.
// Plan migration to Flex Consumption when available for your region.

resource hostingPlan 'Microsoft.Web/serverfarms@2024-04-01' = {
  name: hostingPlanName
  location: location
  tags: resourceTags
  kind: 'linux'
  sku: {
    name: 'Y1'
    tier: 'Dynamic'
  }
  properties: {
    reserved: true
  }
}

// ─── Function App ───────────────────────────────────────────────────────────

resource functionApp 'Microsoft.Web/sites@2024-04-01' = {
  name: functionAppName
  location: location
  tags: resourceTags
  kind: 'functionapp,linux'
  identity: identityBlock
  properties: {
    serverFarmId: hostingPlan.id
    reserved: true
    httpsOnly: true
    siteConfig: {
      linuxFxVersion: 'PYTHON|${pythonVersion}'
      ftpsState: 'Disabled'
      minTlsVersion: '1.2'
      appSettings: [
        // --- Runtime ---
        {
          name: 'FUNCTIONS_EXTENSION_VERSION'
          value: '~4'
        }
        {
          name: 'FUNCTIONS_WORKER_RUNTIME'
          value: 'python'
        }
        {
          name: 'AzureWebJobsStorage'
          value: storageConnectionString
        }
        // WEBSITE_RUN_FROM_PACKAGE is managed by 'func azure functionapp publish'

        // --- Monitoring ---
        {
          name: 'APPLICATIONINSIGHTS_CONNECTION_STRING'
          value: appInsights.properties.ConnectionString
        }

        // --- ADX ---
        {
          name: 'ADX_CLUSTER_URI'
          value: adxClusterUri
        }
        {
          name: 'ADX_DATABASE'
          value: adxDatabaseName
        }

        // --- Netskope ---
        {
          name: 'NetskopeHostname'
          value: netskopeHostname
        }
        {
          name: 'NetskopeApiToken'
          value: netskopeTokenSetting
        }
        {
          name: 'NetskopeIndex'
          value: netskopeIndex
        }

        // --- Identity (empty = system-assigned, set = user-assigned) ---
        {
          name: 'MANAGED_IDENTITY_CLIENT_ID'
          value: isUserAssigned ? userAssignedClientId : ''
        }

        // --- Logging ---
        {
          name: 'LOG_LEVEL'
          value: logLevel
        }
        {
          name: 'AZURE_LOG_LEVEL'
          value: detailedKustoLogging == 'Yes' ? 'INFO' : 'WARNING'
        }

        // --- Event Stream Toggles ---
        {
          name: 'IngestEventsPage'
          value: ingestEventsPage
        }
        {
          name: 'IngestEventsApplication'
          value: ingestEventsApplication
        }
        {
          name: 'IngestEventsAudit'
          value: ingestEventsAudit
        }
        {
          name: 'IngestEventsInfrastructure'
          value: ingestEventsInfrastructure
        }
        {
          name: 'IngestEventsNetwork'
          value: ingestEventsNetwork
        }
        {
          name: 'IngestEventsConnection'
          value: ingestEventsConnection
        }
        {
          name: 'IngestEventsEndpoint'
          value: ingestEventsEndpoint
        }
        {
          name: 'IngestEventsIncident'
          value: ingestEventsIncident
        }

        // --- Alert Subtype Toggles ---
        {
          name: 'IngestAlertsRemediation'
          value: ingestAlertsRemediation
        }
        {
          name: 'IngestAlertsCompromisedCredential'
          value: ingestAlertsCompromisedCredential
        }
        {
          name: 'IngestAlertsUba'
          value: ingestAlertsUba
        }
        {
          name: 'IngestAlertsSecurityAssessment'
          value: ingestAlertsSecurityAssessment
        }
        {
          name: 'IngestAlertsQuarantine'
          value: ingestAlertsQuarantine
        }
        {
          name: 'IngestAlertsPolicy'
          value: ingestAlertsPolicy
        }
        {
          name: 'IngestAlertsMalware'
          value: ingestAlertsMalware
        }
        {
          name: 'IngestAlertsMalsite'
          value: ingestAlertsMalsite
        }
        {
          name: 'IngestAlertsDlp'
          value: ingestAlertsDlp
        }
        {
          name: 'IngestAlertsCtep'
          value: ingestAlertsCtep
        }
        {
          name: 'IngestAlertsWatchlist'
          value: ingestAlertsWatchlist
        }
        {
          name: 'IngestAlertsDevice'
          value: ingestAlertsDevice
        }
        {
          name: 'IngestAlertsContent'
          value: ingestAlertsContent
        }
      ]
    }
  }
}

// ─── Key Vault (only when keyVaultOption == 'create') ───────────────────────

resource keyVault 'Microsoft.KeyVault/vaults@2023-07-01' = if (keyVaultOption == 'create') {
  name: keyVaultName
  location: location
  tags: resourceTags
  properties: {
    tenantId: subscription().tenantId
    sku: {
      family: 'A'
      name: keyVaultSku
    }
    enableRbacAuthorization: true
    enableSoftDelete: true
    softDeleteRetentionInDays: 90
    enablePurgeProtection: true
  }
}

resource keyVaultSecret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = if (keyVaultOption == 'create') {
  parent: keyVault
  name: keyVaultSecretName
  properties: {
    value: netskopeApiToken
    contentType: 'Netskope REST API v2 token'
  }
}

// Grant the Function App's identity read access to Key Vault secrets (create mode)
resource kvRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = if (keyVaultOption == 'create') {
  name: guid(keyVault.id, kvSecretsUserRoleId, functionAppName)
  scope: keyVault
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', kvSecretsUserRoleId)
    principalId: isUserAssigned ? userAssignedPrincipalId : functionApp.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Outputs
// ═══════════════════════════════════════════════════════════════════════════════

var functionPrincipalId = isUserAssigned ? userAssignedPrincipalId : functionApp.identity.principalId

output functionAppName string = functionApp.name
output functionAppHostname string = functionApp.properties.defaultHostName
output functionAppPrincipalId string = functionPrincipalId

@description('Run this KQL command in your ADX cluster to grant the Function App ingest permissions.')
output grantAdxIngestorRole string = '.add database ${adxDatabaseName} ingestors (\'aadapp=${functionPrincipalId}\')'

output keyVaultUri string = keyVaultOption == 'create' ? keyVault!.properties.vaultUri : ''

@description('When using an existing Key Vault, run this command to grant the Function App secret read access.')
output grantKeyVaultAccess string = keyVaultOption == 'existing'
  ? 'az role assignment create --role "Key Vault Secrets User" --assignee ${functionPrincipalId} --scope ${resourceId('Microsoft.KeyVault/vaults', keyVaultName)}'
  : keyVaultOption == 'create'
    ? 'Automatically configured via RBAC role assignment.'
    : 'Not applicable - Key Vault not in use.'
