# Azure Synapse & ADLS Gen2 Setup Checklist (Role 4)

> Goal: Set up the shared Lakehouse + Warehouse for all 5 team members in Week 1 while keeping Azure credit burn near zero.

---

## 1. Resource Group & ADLS Gen2 (Data Lake)
1. **Create Resource Group** (e.g. `rg-lct-depi19`), pick one region (e.g. `East US` or `West Europe`) and keep **all** resources in the **same region** to avoid egress charges and latency.
2. **Create Storage Account (ADLS Gen2)**:
   - Performance: `Standard` | Redundancy: `LRS` (cheapest for dev/project).
   - **Advanced tab → Enable Hierarchical Namespace (`Data Lake Storage Gen2`)**: **MUST BE CHECKED** (cannot be enabled after creation).
3. **Create Containers** (confirm final names in Day 2 Data Contract):
   - `bronze` (raw batch CSV/JSON + streaming JSON drops)
   - `silver` (`shipment_events`, `shipment_status`, `last_ping` Delta tables)
   - `gold` (`daily_shipment_facts` Parquet/Delta)
   - `checkpoints` (Spark Structured Streaming `_chk/` folders)
   - `synapse` (default filesystem for Synapse Workspace)

---

## 2. Synapse Workspace
1. **Create Azure Synapse Analytics Workspace**:
   - Link to the ADLS Gen2 account above + select container `synapse`.
   - Set SQL Administrator login & password (save these securely — never commit to Git).
   - **Networking**: Allow Azure services and resources to access this workspace; add your team's client IPs (or `0.0.0.0 - 255.255.255.255` temporarily during dev if team IPs change often — tighten before demo if needed).
2. **Assign Permissions (Crucial for Team & Serverless/COPY INTO)**:
   - On the **Storage Account → Access Control (IAM)**:
     - Grant **`Storage Blob Data Contributor`** to the **Synapse Workspace Managed Identity**.
     - Grant **`Storage Blob Data Contributor`** to **all 5 team members** (so Simulator, Spark jobs, and SQL queries can read/write ADLS).
   - Inside **Synapse Studio → Manage → Access control**:
     - Grant **`Synapse Administrator`** (or **`Synapse SQL Administrator`** + **`Apache Spark Administrator`**) to the team members who run Spark/SQL.

---

## 3. Compute Pools & Cost Control (Read Carefully!)
| Resource | Billing Model | Setup & Cost Rule |
|---|---|---|
| **Built-in Serverless SQL** | Pay per TB scanned (~$5/TB, 10 MB min) | Always on, zero idle cost. Use for live `silver` views (`OPENROWSET`). |
| **Apache Spark Pool** | Pay per vCore-hour while active | Smallest node size (`Small 4 vCores`), min 3 nodes, enable **Auto-pause = 5–15 mins**. |
| **Dedicated SQL Pool** | **Pay per hour continuously while running (even idle!)** | Create at **DW100c** (minimum). **Pause immediately after creation!** |

> **Golden Rule:** Never leave the Dedicated SQL Pool running overnight. Only `Resume` it when loading `daily_shipment_facts`, running `dbt run --target synapse`, or during demos — then **`Pause`** immediately after.

---

## 4. Connecting `dbt-synapse` Locally
1. Resume the Dedicated SQL Pool and copy its **Dedicated SQL endpoint**:
   `<workspace-name>.sql.azuresynapse.net`
2. In your local PowerShell session (never commit credentials):
   ```powershell
   $env:SYNAPSE_SERVER   = "<workspace-name>.sql.azuresynapse.net"
   $env:SYNAPSE_DB       = "<dedicated_pool_name>"
   $env:SYNAPSE_USER     = "<sql_admin_user>"
   $env:SYNAPSE_PASSWORD = "<sql_admin_password>"
   ```
3. Verify connectivity from the repo root:
   ```powershell
   .\.venv\Scripts\dbt.exe debug --project-dir dbt --profiles-dir dbt --target synapse
   ```
4. When finished testing: **Pause the Dedicated SQL Pool** in Azure Portal / Synapse Studio.
