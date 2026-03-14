
from pyspark.sql.functions import col, lit, to_date
from pyspark.sql.types import *

def enforce_schema(df, schema_cfg):
    for c in schema_cfg["columns"]:
        col_name = c["name"]
        data_type = c["type"]

        if data_type == "string":
            df = df.withColumn(col_name, col(col_name).cast(StringType()))
        elif data_type == "int":
            df = df.withColumn(col_name, col(col_name).cast(IntegerType()))
        elif data_type == "double":
            df = df.withColumn(col_name, col(col_name).cast(DoubleType()))
        elif data_type == "float":
            df = df.withColumn(col_name, col(col_name).cast(FloatType()))
        elif data_type == "date":
            df = df.withColumn(col_name, to_date(col(col_name), "dd-MM-yyyy"))
        elif data_type == "timestamp":
            df = df.withColumn(col_name, col(col_name).cast(TimestampType()))

    return df
 