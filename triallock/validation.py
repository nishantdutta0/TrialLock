from triallock.config import SCHEMAS

def validate_entity(spark_df, entity):
    schema = SCHEMAS.get(entity, [])
    required = [item["name"] for item in schema if item.get("required")]
    missing = []
    if spark_df is not None:
        from pyspark.sql import functions as F
        for col in required:
            if col in spark_df.columns:
                null_or_empty = spark_df.filter((F.col(col).isNull()) | (F.col(col) == "") | (F.col(col) == "NULL")).count()
                if null_or_empty > 0:
                    missing.append({"field": col, "missing_count": null_or_empty})
    return {"entity": entity, "required": required, "missing": missing}
