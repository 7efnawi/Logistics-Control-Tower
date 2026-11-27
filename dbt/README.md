# Logistics Control Tower — dbt Project (Role 4)

## Targets
- `local` (default): Uses **DuckDB** (`dbt-duckdb`) for offline/free development and testing.
- `synapse`: Connects to **Azure Synapse Dedicated SQL Pool** (`dbt-synapse` + ODBC Driver 18).

## Planned Marts (Pending Day 2 Data Contract)
1. `mart_on_time_delivery` — On-time delivery % across shipments.
2. `mart_port_dwell_time` — Dwell time at ports.
3. `mart_customs_time` — Customs clearance duration.
4. `mart_carrier_scorecards` — Monthly carrier scorecards by lane (min 3 months history).

## Local Commands (from repo root)
```powershell
.\.venv\Scripts\dbt.exe debug --project-dir dbt --profiles-dir dbt
.\.venv\Scripts\dbt.exe build --project-dir dbt --profiles-dir dbt --target local
```
