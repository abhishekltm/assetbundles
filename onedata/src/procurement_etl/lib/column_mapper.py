def apply_column_mapping(df, columns_dict):

    if columns_dict is None:
        print("No mapping provided")
        return df

    for src_col, tgt_col in columns_dict.items():
        if src_col in df.columns:
            df = df.withColumnRenamed(src_col, tgt_col)
    return df