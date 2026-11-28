-- =============================================================================
-- TEMPLATE ONLY — Update container/account/paths after Day 2 Data Contract
-- Synapse Serverless SQL Pool: Live Views over Silver Delta tables
-- =============================================================================

-- Example: Create dedicated database / schema for live views
-- CREATE DATABASE lct_live;
-- GO
-- USE lct_live;
-- GO

-- Live shipment status view (updated continuously by Spark Structured Streaming)
/*
CREATE OR ALTER VIEW dbo.vw_shipment_status_live AS
SELECT *
FROM OPENROWSET(
    BULK 'https://adlslogisticsdepi19.dfs.core.windows.net/silver/shipment_status/',
    FORMAT = 'DELTA'
) AS rows;
GO
*/

-- Live last GPS ping view (for silent > 6h exception detection)
/*
CREATE OR ALTER VIEW dbo.vw_last_ping_live AS
SELECT *
FROM OPENROWSET(
    BULK 'https://adlslogisticsdepi19.dfs.core.windows.net/silver/last_ping/',
    FORMAT = 'DELTA'
) AS rows;
GO
*/
