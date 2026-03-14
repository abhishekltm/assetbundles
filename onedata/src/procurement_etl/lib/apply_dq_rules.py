from pyspark.sql.functions import expr, lit, current_timestamp

def apply_dq_rules(df, dq_rules):
    if not dq_rules:
        return df, df.limit(0)

    condition = None

    for r in dq_rules:
        rule_expr = expr(r["rule"])
        if condition is None:
            condition = rule_expr
        else:
            condition = condition & rule_expr
    valid_df = df.filter(condition)
    invalid_df = df.filter(~condition) \
                   .withColumn("dq_failed", lit("FAILED")) \
                   .withColumn("quarantine_time", current_timestamp())
    return valid_df, invalid_df
 