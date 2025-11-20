import dlt 

#clean columns names 
def clean_column_names(df):
    # Replace invalid characters with underscores and strip whitespace
    new_columns = [
        col.replace(' ', '_')
        .replace('[', '')
        .replace(']', '')
        .replace(',', '')
        .replace(';', '')
        .replace('{', '')
        .replace('}', '')
        .replace('(', '')
        .replace(')', '')
        .replace('\n', '')
        .replace('\t', '')
        .replace('=', '')
        for col in df.columns
    ]
    return df.toDF(*new_columns)

@dlt.table(
    name = "bronze_dev.default.BoE_Database_Bronze",
    table_properties = {
        "quality": "landing"
    }
)

def BoE_Db_raw():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .load("s3://real-estate-project-area/BoE_Database/")
    )
    return clean_column_names(df)