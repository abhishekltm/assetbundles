def apply_column_mapping(df, columns_dict):
    for src, tgt in columns_dict.items():
        if src in df.columns:
            df = df.withColumnRenamed(src, tgt)
    return df