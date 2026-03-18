from pyspark.sql.functions import *

def dq_split(df, dq_rules, reason_col="reject_reason"):
    if not dq_rules:
        return df, None
    
    reject_exprs = None
    for rule in dq_rules:
        rule_condition = f"NOT ({rule['rule']})"

        if reject_exprs is None:
            reject_exprs = when(expr(rule_condition), lit(rule["name"]))
        else:
            reject_exprs = reject_exprs.when(expr(rule_condition), lit(rule["name"]))
    df = df.withColumn(reason_col, reject_exprs)

    valid_df = df.filter(col(reason_col).isNull()).drop(reason_col)
    invalid_df = df.filter(col(reason_col).isNotNull())

    return valid_df, invalid_df
 