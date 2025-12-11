CREATE OR REFRESH STREAMING LIVE TABLE gold_dev.default.DimDate
COMMENT "Date Dimension"
AS
SELECT 
    CAST(date_col AS DATE) AS Date,
    DATE_FORMAT(date_col, "ddMMyyyy") AS DateKey,
    YEAR(date_col) AS YEAR,
    MONTH(date_col) AS MONTH,
    DATE_FORMAT(date_col, "MMM") AS MONTHNAME,
    DAY(date_col) AS DAY
FROM (
    -- UKHPI stream
    SELECT DISTINCT Date
    FROM STREAM(silver_dev.default.UKHPI_Data_Silver With Watermark As Date Delay Interval 1 Day)
    WHERE Date IS NOT NULL

    UNION

    -- Bank of England stream
    SELECT DISTINCT Date AS date_col
    FROM STREAM(silver_dev.default.BoE_Database_Silver)
    WHERE Date IS NOT NULL
) d(date_col);


-- CREATE OR REFRESH LIVE TABLE gold_dev.default.DimRegion (
--   RegionId BIGINT GENERATED ALWAYS AS IDENTITY,
--   RegionName STRING,
--   AREACODE String
-- )
-- AS SELECT
--   DISTINCT RegionName,
--   AreaCode
-- FROM silver_dev.default.UKHPI_Data_Silver
-- WHERE RegionName IS NOT NULL AND AreaCode IS NOT NULL;

-- CREATE OR REFRESH LIVE TABLE gold_dev.default.DimLocation (
--   LocationId BIGINT GENERATED ALWAYS AS IDENTITY,
--   Postcode STRING,
--   PAON STRING,
--   SAON STRING,
--   District STRING,
--   TownCity STRING,
--   County STRING
-- )
-- AS SELECT 
--   DISTINCT Postcode,
--   PAON,
--   SAON,
--   District,
--   TownCity,
--   County
-- FROM silver_dev.default.Price_Paid_Data_Silver
-- WHERE Postcode IS NOT NULL AND TownCity IS NOT NULL;

-- CREATE OR REFRESH LIVE TABLE gold_dev.default.DimPropertyType (
--   PropertyTypeId BIGINT GENERATED ALWAYS AS IDENTITY,
--   PropertyCategory STRING,
--   IsNew BOOLEAN,
--   Duration STRING
-- )
-- AS SELECT
--   DISTINCT PropertyType as PropertyCategory,
--   OldNew as IsNew, 
--   Duration
-- FROM silver_dev.default.Price_Paid_Data_Silver
-- WHERE PropertyType IS NOT NULL AND OldNew IS NOT NULL AND Duration IS NOT NULL

