from pyspark.sql.functions import col, lit

def enforce_schema(df, schema_dict, drop_extra_cols=True):
    if not schema_dict:
        print("No schema config provided")
        return df

    for column_name, dtype in schema_dict.items():
        if column_name in df.columns:
            df = df.withColumn(
                column_name,
                col(column_name).cast(dtype)
            )
        else:
            df = df.withColumn(
                column_name,
                lit(None).cast(dtype)
            )

    if drop_extra_cols:
        df = df.select(*schema_dict.keys())
    return df
 