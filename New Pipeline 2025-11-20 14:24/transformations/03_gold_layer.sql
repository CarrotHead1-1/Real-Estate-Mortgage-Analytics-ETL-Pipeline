
CREATE OR REFRESH STREAMING LIVE TABLE gold_dev.default.DimDate
COMMENT "Date Dimension"
AS
SELECT 
    CAST(date_col AS DATE) AS Date,
    DATE_FORMAT(date_col, "ddMMyyyy") as DateKey,
    YEAR(date_col) as YEAR,
    MONTH(date_col) as MONTH,
    DATE_FORMAT(date_col, "MMM") as MONTHNAME,
    DAY(date_col) as DAY
FROM (
    SELECT DISTINCT Date FROM silver_dev.default.UKHPI_Data_Silver
    UNION 
    SELECT DISTINCT DateOfTransaction FROM silver_dev.default.Price_Paid_Data_Silver
    UNION 
    SELECT DISTINCT Date FROM silver_dev.default.BoE_Database_Silver
) d(date_col);

CREATE OR REFRESH LIVE TABLE gold_dev.default.DimRegion (
  RegionId BIGINT GENERATED ALWAYS AS IDENTITY,
  RegionName STRING,
  AREACODE String
)
AS SELECT
  DISTINCT RegionName,
  AreaCode
FROM silver_dev.default.UKHPI_Data_Silver
WHERE RegionName IS NOT NULL AND AreaCode IS NOT NULL;

