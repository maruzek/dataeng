# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3fec1da3-65e7-44e4-8710-af83bddae261",
# META       "default_lakehouse_name": "lh_team_15",
# META       "default_lakehouse_workspace_id": "97bc5e7e-6522-4ebe-b15b-49cf2be64065",
# META       "known_lakehouses": [
# META         {
# META           "id": "3fec1da3-65e7-44e4-8710-af83bddae261"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window

def make_silver(bronze_name, fix=None):
    df = spark.table(bronze_name)

    # 1) názvy sloupců: malá písmena s podtržítky
    df = df.toDF(*[c.lower() for c in df.columns])
    if "loaded_at" in df.columns and "_loaded_at" not in df.columns:
        df = df.withColumnRenamed("loaded_at", "_loaded_at")

    # 2) texty: oříznout mezery, prázdný text -> NULL
    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, F.when(F.trim(c) == "", None).otherwise(F.trim(c)))

    # 3) datové typy: data jako datum/čas, částky jako desetinná čísla
    for c, t in df.dtypes:
        if t == "string" and c.endswith("_at"):
            df = df.withColumn(c, F.to_timestamp(c))
        elif t == "string" and c.endswith("_date"):
            df = df.withColumn(c, F.to_date(c))
        elif t in ("string", "double", "float") and any(k in c for k in ("amount", "surcharge", "cost", "price")):
            df = df.withColumn(c, F.col(c).cast("decimal(10,2)"))

    # 4) nesmyslné hodnoty: datum po čase načtení (v budoucnosti) -> NULL
    for c, t in df.dtypes:
        if t in ("date", "timestamp") and not c.startswith("_") and "valid" not in c:
            df = df.withColumn(c, F.when(F.col(c) > F.col("_loaded_at"), None).otherwise(F.col(c)))

    # 5) opravy specifické pro tabulku
    if fix:
        df = fix(df)

    # 6) chybějící klíč -> řádek vyřadit; duplicity podle klíče -> nechat nejnovější
    key = df.columns[0]
    order_col = "created_at" if "created_at" in df.columns else "_loaded_at"
    df = df.filter(F.col(key).isNotNull())
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc_nulls_last())
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

    # 7) zápis (overwrite = opakované spuštění nevytvoří duplicity)
    silver_name = bronze_name.replace("bronze_", "silver_")
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_name)
    print(f"{silver_name}: {spark.table(silver_name).count()} řádků (klíč {key})")


def fix_cestujici(df):
    # datum narození před rokem 1910 je nesmysl (např. 1900-01-03) -> NULL
    df = df.withColumn("person_birthdate",
        F.when(F.col("person_birthdate") < "1910-01-01", None).otherwise(F.col("person_birthdate")))
    # jména začínající pomlčkou apod. ("- TETZNER") -> odstranit nepísmenné znaky na začátku
    for c in ("person_full_name", "person_firstname", "person_surname"):
        df = df.withColumn(c, F.regexp_replace(c, r"^[^\p{L}]+", ""))
    return df

def fix_podnet(df):
    # čas incidentu je uložen jako 1970-01-01 hh:mm -> ponechat jen čas
    return df.withColumn("incident_time", F.date_format("incident_time", "HH:mm:ss"))

FIXES = {"bronze_cestujici": fix_cestujici, "bronze_podnet_cestujiciho": fix_podnet}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tables = sorted(t.name for t in spark.catalog.listTables() if t.name.startswith("bronze_"))
for t in tables:
    make_silver(t, FIXES.get(t))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
