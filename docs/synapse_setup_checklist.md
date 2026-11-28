# Azure Synapse & ADLS Gen2 Setup Checklist (Role 4)

> Goal: Set up the shared Lakehouse + Warehouse for all 5 team members in Week 1 while keeping Azure credit burn near zero.

---

## 0. Provisioned Environment (Completed 2026-11-28)

| Resource | Configured Value | Notes / Cost State |
|---|---|---|
| **Subscription** | `Azure for Students` | Policy allows: `spaincentral`, `francecentral`, `denmarkeast`, `uaenorth`, `austriaeast` |
| **Resource Group** | `rg-logistics-control-tower` | Shared container for project resources |
| **Region** | `France Central` (`francecentral`) | Full Synapse/Spark support & low latency from Egypt |
| **Storage Account (ADLS Gen2)** | `adlslogisticsdepi19` | `Standard` · `LRS` · `Big Data Analytics` · **Hierarchical Namespace enabled** |
| **Containers** | `bronze`, `silver`, `gold`, `checkpoints`, `synapse` | Private access |
| **Synapse Workspace** | `synapse-logistics-depi19` | Linked to `adlslogisticsdepi19/synapse`; Managed Identity has `Storage Blob Data Contributor` |
| **Built-in Serverless SQL** | `Built-in` | `Online` · Pay per TB scanned ($0 idle) |
| **Apache Spark Pool** | `spark01` | `Small (4 vCores / 32 GB)` · 3 nodes · **Auto-pause enabled** |
| **Dedicated SQL Pool** | `lct_dw` | `DW100c` · **`Paused`** ($0/hr compute while paused) |

### Endpoints & Non-Secret Connection Info
- **ADLS Gen2 DFS Base URL:** `https://adlslogisticsdepi19.dfs.core.windows.net/`
- **Dedicated SQL Endpoint:** `synapse-logistics-depi19.sql.azuresynapse.net` (Database: `lct_dw`)
- **Serverless SQL Endpoint:** `synapse-logistics-depi19-ondemand.sql.azuresynapse.net`
- **Synapse Studio:** Open via Azure Portal → `synapse-logistics-depi19` → **Open Synapse Studio**

---

## 1. Resource Group & ADLS Gen2 (Data Lake) — Done
1. **Create Resource Group** (`rg-logistics-control-tower`). Note: Educational/student subscriptions may restrict regions via Azure Policy (`RequestDisallowedByAzure`); check `Policy -> Assignments -> Parameters` if blocked.
2. **Create Storage Account (`adlslogisticsdepi19`)**:
   - Region: `France Central` | Workload: `Big Data Analytics` | Performance: `Standard` | Redundancy: `LRS`.
   - **Advanced tab → Enable Hierarchical Namespace (`Data Lake Storage Gen2`)**: **Checked**.
3. **Create Containers**: `bronze`, `silver`, `gold`, `checkpoints`, `synapse`.

---

## 2. Synapse Workspace & Team Permissions
1. **Create Azure Synapse Analytics Workspace (`synapse-logistics-depi19`)** — Done:
   - Linked to `adlslogisticsdepi19` + container `synapse` in `France Central`.
   - Networking: public access enabled during dev.
2. **Storage & Studio Permissions**:
   - `adlslogisticsdepi19` → `Access Control (IAM)` → **`Storage Blob Data Contributor`** granted to `synapse-logistics-depi19` Managed Identity (**Done**).
   - **Pending (when teammates share emails):**
     - Grant **`Storage Blob Data Contributor`** on `adlslogisticsdepi19` to all 5 team members.
     - Inside **Synapse Studio → Manage → Access control**, grant **`Synapse Administrator`** (or SQL/Spark roles) to the team.

---

## 3. Compute Pools & Cost Control (Read Carefully!)
| Resource | Billing Model | Current Setup & Cost Rule |
|---|---|---|
| **Built-in Serverless SQL (`Built-in`)** | Pay per TB scanned (~$5/TB, 10 MB min) | `Online` — zero idle cost. Use for live `silver` views (`OPENROWSET`). |
| **Apache Spark Pool (`spark01`)** | Pay per vCore-hour while active | `Small (4 vCores)`, 3 nodes, **Auto-pause enabled**. |
| **Dedicated SQL Pool (`lct_dw`)** | **Pay per hour continuously while running (even idle!)** | `DW100c` — **`Paused`**. Only `Resume` when loading `daily_shipment_facts`, running `dbt --target synapse`, or demoing! |

---

## 4. Connecting `dbt-synapse` Locally
1. In Synapse Studio (`Manage -> SQL pools`), **`Resume`** `lct_dw` (wait ~1–2 minutes until `Online`).
2. In your local PowerShell session (never commit credentials):
   ```powershell
   $env:SYNAPSE_SERVER   = "synapse-logistics-depi19.sql.azuresynapse.net"
   $env:SYNAPSE_DB       = "lct_dw"
   $env:SYNAPSE_USER     = "<sql_admin_user>"
   $env:SYNAPSE_PASSWORD = "<sql_admin_password>"
   ```
3. Verify connectivity from the repo root:
   ```powershell
   .\.venv\Scripts\dbt.exe debug --project-dir dbt --profiles-dir dbt --target synapse
   ```
4. **Immediately after testing:** **`Pause`** `lct_dw` in Synapse Studio (`Manage -> SQL pools`).
