import dlt 
from pyspark.sql.functions import to_date, col, current_timestamp, when, count, trim, regexp_replace

@dlt.table(
    name = "silver_dev.default.BoE_Database_Silver",
    table_properties = {"quality": "silver"}
)

#create expectations and validation
@dlt.expect("valid_date", "Date IS NOT NULL")
@dlt.expect("valid_data", "Monthly_Approvals IS NOT NULL")


def BoE_Database_Silver():
    df = dlt.readStream("bronze_dev.default.BoE_Database_Bronze")

    #rename columns, cast values
    df_clean = (
        df.withColumn("Date", to_date("Date", "dd MMM yy"))
        .withColumnRenamed("Monthly_number_of_total_sterling_approvals_for_house_purchase_to_individuals_seasonally_adjusted_______________a______________LPMVTVX","Monthly_Approvals")
        .withColumn("Monthly_Approvals", col("Monthly_Approvals").cast("int"))
        .select("Date", "Monthly_Approvals", "_ingest_file")
    )

    #add metadata
    df_clean = (
        df_clean.withColumn("silver_ingest_timestamp", current_timestamp())
        .withColumn("silver_source_file", col("_ingest_file"))
    )

    return df_clean

@dlt.table(
    name = "silver_dev.default.UKHPI_Data_Silver_Missing_Data",
    table_properties = {"quality": "silver"}
)
def UKHPI_Data_Silver_Missing_Data():
    df = dlt.read("bronze_dev.default.UKHPI_Data_Bronze")
    return df.select([count(when(col(c).isNull(), c)).alias(c+"_missing_count") for c in df.columns])

@dlt.table(
    name = "silver_dev.default.UKHPI_Data_Silver",
    table_properties = {"quality": "silver"},
    partition_cols = ["RegionName"]
)

# #create expectations
@dlt.expect("valid_date", "Date IS NOT NULL")
@dlt.expect("valid_region_name", "RegionName IS NOT NULL")
@dlt.expect("valid_avg_price", "AveragePrice IS NOT NULL AND AveragePrice >= 0")
@dlt.expect("valid_index", "Index IS NOT NULL AND Index >= 0")
@dlt.expect("valid_sale_vol", "SalesVolume IS NOT NULL AND SalesVolume >= 0")
@dlt.expect("valid_new_price", "NewPrice IS NOT NULL AND NewPrice >= 0")
@dlt.expect("valid_new_index", "NewIndex IS NOT NULL AND NewIndex >= 0")
@dlt.expect("valid_new_sales_vol", "NewSalesVolume IS NOT NULL AND NewSalesVolume >= 0")
@dlt.expect("valid_old_price", "OldPrice IS NOT NULL AND OldPrice >= 0")
@dlt.expect("valid_old_index", "OldIndex IS NOT NULL AND OldIndex >= 0")
@dlt.expect("valid_old_sales_vol", "OldSalesVolume IS NOT NULL AND OldSalesVolume >= 0")


def UKHPI_Data_Silver():
    df = dlt.readStream("bronze_dev.default.UKHPI_Data_Bronze")

    #drop duplicates
    df = df.dropDuplicates(["Date", "RegionName"])
    
    #convert data column and cast numerical values to double
    df = df.withColumn("Date", to_date("Date", "dd/MM/yyyy"))
    
    columns_numeric = [
        "AveragePrice","Index","IndexSA","1m_Change","12m_Change","AveragePriceSA","SalesVolume","DetachedPrice","DetachedIndex","Detached1m_Change","Detached12m_Change","SemiDetachedPrice","SemiDetachedIndex",	"SemiDetached1m_Change","SemiDetached12m_Change","TerracedPrice","TerracedIndex","Terraced1m_Change","Terraced12m_Change","FlatPrice","FlatIndex","Flat1m_Change","Flat12m_Change","CashPrice","CashIndex","Cash1m_Change",	"Cash12m_Change","CashSalesVolume","MortgagePrice","MortgageIndex","Mortgage1m_Change","Mortgage12m_Change","MortgageSalesVolume","FTBPrice","FTBIndex","FTB1m_Change","FTB12m_Change","FOOPrice","FOOIndex","FOO1m_Change","FOO12m_Change","NewPrice","NewIndex","New1m_Change","New12m_Change","NewSalesVolume","OldPrice","OldIndex", "Old1m_Change","Old12m_Change","OldSalesVolume"
    ]

    for column in columns_numeric:
        df = df.withColumn(column, col(column).cast("double"))

    #fill missing values in needed columns 
    df = df.fillna(0, subset = [
        "Index","AveragePrice","IndexSA","SalesVolume","NewPrice","NewIndex","NewSalesVolume","OldPrice","OldIndex", "OldSalesVolume"
        ])

    #add meta data
    df = (
        df.withColumn("silver_ingest_timestamp", current_timestamp())
        .withColumn("silver_source_file", col("_ingest_file"))
    )

    return df


# @dlt.table(
#     name = "silver_dev.default.Price_Paid_Data_Silver",
#     table_properties = {"quality": "silver"},
# )

# #add expectations and validations
# @dlt.expect("valid_price", "Price IS NOT NULL AND Price >= 0")
# @dlt.expect("valid_date", "DateOfTransaction IS NOT NULL")
# @dlt.expect("valid_postcode", "Postcode IS NOT NULL")

# def Price_Paid_Data_Silver():
#     df = dlt.readStream("bronze_dev.default.Price_Paid_Data_Bronze")

#     #select the first 16 columns 
#     df = df.select(df.columns[:16] + ["_ingest_file"])
    
#     #add new column headers
#     df = df.toDF(
#         "TransactionId", "Price", "DateOfTransaction","Postcode","PropertyType","OldNew","Duration","PAON","SAON","Street","Locality","TownCity","District","County","PPD","RecordStatus", "_ingest_file"
#     )

#     #cast column types and clean data
#     df_cleaned = (
#         df.withColumn("TransactionId", regexp_replace(col("TransactionId"), "[{}]", ""))
#         .withColumn("Price", col("Price").cast("int"))
#         .withColumn("DateOfTransaction", to_date("DateOfTransaction", "yyyy-MM-dd"))
#         .withColumn("Postcode", trim(col("Postcode")))
#         .withColumn("PAON", trim(col("PAON")))
#         .withColumn("SAON", trim(col("SAON")))
#         .withColumn("Street", trim(col("Street")))
#         .withColumn("Locality", trim(col("Locality")))
#         .withColumn("TownCity", trim(col("TownCity")))
#         .withColumn("District", trim(col("District")))
#         .withColumn("County", trim(col("County")))
#         .withColumn("PPD", trim(col("PPD")))
#         .withColumn("RecordStatus", trim(col("RecordStatus")))
#     )

#     #drop duplicates
#     df_cleaned = df_cleaned.dropDuplicates(["TransactionId"])

#     #add metadata
#     df_cleaned = (
#         df_cleaned.withColumn("silver_ingest_timestamp", current_timestamp())
#         .withColumn("silver_source_file", col("_ingest_file"))
#     )

#     return df_cleaned
