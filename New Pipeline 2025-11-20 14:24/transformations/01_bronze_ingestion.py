import dlt 
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType
import re


#clean columns names 
def clean_column_names(df):
    new_columns = [re.sub(r'[^a-zA-Z0-9_]', '_', col.strip()) for col in df.columns]
    # new_columns = [
    #     col.replace(' ', '_')
    #     .replace('[', '')
    #     .replace(']', '')
    #     .replace(',', '')
    #     .replace(';', '')
    #     .replace('{', '')
    #     .replace('}', '')
    #     .replace('(', '')
    #     .replace(')', '')
    #     .replace('\n', '')
    #     .replace('\t', '')
    #     .replace('=', '')
    #     for col in df.columns
    # ]
    return df.toDF(*new_columns)

@dlt.table(
    name = "bronze_dev.default.BoE_Database_Bronze",
    table_properties = {
        "quality": "bronze"
    }
)
def BoE_Db_raw():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("s3://real-estate-project-area/BoE_Database/")
    )
    df = (
        df.withColumn("_ingest_file", df["_metadata"]["file_path"])
        .withColumn("_ingest_timestamp", current_timestamp())
    )
    return clean_column_names(df)

@dlt.table(
    name = "bronze_dev.default.UKHPI_Data_Bronze",
    table_properties = {"quality": "bronze"}
)
def UKHPI_Data_raw():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("s3://real-estate-project-area/UK_HPI/")
    )
    df = (
        df.withColumn("_ingest_file", df["_metadata"]["file_path"])
        .withColumn("_ingest_timestamp", current_timestamp())
    )
    return clean_column_names(df)

# @dlt.table(
#     name = "bronze_dev.default.Price_Paid_Data_Bronze",
#     table_properties = {"quality": "bronze"}
# )
# def Price_Paid_Data_raw():
#     df = (
#         spark.readStream.format("cloudFiles")
#         .option("cloudFiles.format", "csv")
#         .option("header", "false")             
#         .option("delimiter", ",")
#         .option("quote", "\"")                 
#         .option("escape", "\"")
#         .option("cloudFiles.inferColumnTypes", "false")

#         #enfore a schema to ensure 
#         .schema(
#             StructType([
#                 StructField("_c0", StringType()),
#                 StructField("_c1", StringType()),
#                 StructField("_c2", StringType()),
#                 StructField("_c3", StringType()),
#                 StructField("_c4", StringType()),
#                 StructField("_c5", StringType()),
#                 StructField("_c6", StringType()),
#                 StructField("_c7", StringType()),
#                 StructField("_c8", StringType()),
#                 StructField("_c9", StringType()),
#                 StructField("_c10", StringType()),
#                 StructField("_c11", StringType()),
#                 StructField("_c12", StringType()),
#                 StructField("_c13", StringType()),
#                 StructField("_c14", StringType()),
#                 StructField("_c15", StringType()),
#             ])
#         )
#         .load("s3://real-estate-project-area/Price_Paid_Data/")
#     )
#     df = (
#         df.withColumn("_ingest_file", df["_metadata"]["file_path"])
#         .withColumn("_ingest_timestamp", current_timestamp())
#     )
#     return df