-- =============================================================================
-- TEMPLATE ONLY — Update schema & loading mechanism after Day 2 Data Contract
-- Synapse Dedicated SQL Pool: Daily Shipment Facts load (COPY INTO / CTAS)
-- =============================================================================

-- Target staging/fact table in Dedicated SQL Pool (example structure)
/*
IF OBJECT_ID('dbo.daily_shipment_facts', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.daily_shipment_facts (
        -- Columns to be finalized in Day 2 Data Contract
        shipment_id NVARCHAR(64) NOT NULL,
        fact_date DATE NOT NULL
    )
    WITH (
        DISTRIBUTION = HASH(shipment_id),
        CLUSTERED COLUMNSTORE INDEX
    );
END;
GO

COPY INTO dbo.daily_shipment_facts
FROM 'https://adlslogisticsdepi19.dfs.core.windows.net/gold/daily_shipment_facts/*.parquet'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY = 'Managed Identity')
);
GO
*/
