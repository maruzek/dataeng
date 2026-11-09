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

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Explicitní schéma Silver: sloupec v Bronze -> cílový datový typ.
# První sloupec je klíč tabulky. Názvy v Silver = malá písmena.
SCHEMAS = {
    "bronze_cestujici": {
        "PERSON_SK": "bigint", "PERSON_CRM_ID": "bigint", "PERSON_FULL_NAME": "string",
        "PERSON_FIRSTNAME": "string", "PERSON_SURNAME": "string", "PERSON_BIRTHDATE": "date",
        "PERSON_REGISTRATION": "string", "PERSON_ADDRESS": "string", "DOCUMENT_TYPE": "string",
        "DOCUMENT_ID": "string", "CREATED_AT": "timestamp"},
    "bronze_claim_reklamace_pokuty": {
        "CLAIM_SK": "bigint", "CLAIM_ID": "string", "INSPECTION_CASE_SK": "bigint",
        "SUBMITTED_AT": "timestamp", "SUBMISSION_CHANNEL": "string", "CLAIM_REASON": "string",
        "CLAIM_STATUS": "string", "RESOLVED_AT": "timestamp", "RESOLUTION": "string",
        "SURCHARGE_BEFORE": "decimal(10,2)", "SURCHARGE_AFTER": "decimal(10,2)",
        "REFUND_AMOUNT": "decimal(10,2)", "CREATED_AT": "timestamp"},
    "bronze_collection_vymahani": {
        "COLLECTION_ACTION_SK": "bigint", "COLLECTION_ACTION_ID": "string", "RECEIVABLE_SK": "bigint",
        "ACTION_TYPE": "string", "ACTION_DATE": "date", "AMOUNT_AT_ACTION": "decimal(10,2)",
        "ADDITIONAL_COST": "decimal(10,2)", "EXTERNAL_PARTY_ID": "string", "REFERENCE_NO": "string",
        "STATUS": "string", "CREATED_AT": "timestamp"},
    "bronze_platba": {
        "PAYMENT_SK": "bigint", "PAYMENT_ID": "string", "RECEIVABLE_SK": "bigint",
        "PAYMENT_TIMESTAMP": "timestamp", "PAYMENT_AMOUNT": "decimal(10,2)", "PAYMENT_METHOD": "string",
        "PAYMENT_CHANNEL": "string", "VARIABLE_SYMBOL": "string", "PAYMENT_STATUS": "string",
        "TRANSACTION_REFERENCE": "string", "CREATED_AT": "timestamp"},
    "bronze_podnet_cestujiciho": {
        "SUBMISSION_SK": "bigint", "SUBMISSION_ID": "string", "FIRST_NAME": "string",
        "LAST_NAME": "string", "CONTACT_EMAIL": "string", "SUBMISSION_TYPE": "string",
        "SUBMITTED_AT": "timestamp", "SUBMISSION_CHANNEL": "string", "INCIDENT_DATE": "date",
        "INCIDENT_TIME": "time", "TRANSPORT_MODE": "string", "LINE_REPORTED": "string",
        "LINE_ID": "string", "STOP_REPORTED": "string", "STOP_ID": "string",
        "STOP_MATCH_STATUS": "string", "CATEGORY": "string", "SUBCATEGORY": "string",
        "SUBMISSION_TEXT": "string", "STATUS": "string", "RESOLVED_AT": "timestamp",
        "RESOLUTION_TYPE": "string", "CREATED_AT": "timestamp"},
    "bronze_pohledavka": {
        "RECEIVABLE_SK": "bigint", "RECEIVABLE_ID": "string", "INSPECTION_CASE_SK": "bigint",
        "SURCHARGE_ORIGINAL": "decimal(10,2)", "SURCHARGE_DUE": "decimal(10,2)", "ISSUE_DATE": "date",
        "DISCOUNT_DEADLINE": "date", "DUE_DATE": "date", "STATUS": "string",
        "OUTSTANDING_AMOUNT": "decimal(10,2)", "CREATED_AT": "timestamp"},
    "bronze_prepravni_kontrola": {
        "INSPECTION_CASE_SK": "bigint", "CASE_ID": "string", "ZOPPK_NUMBER": "string",
        "INSPECTION_DATE": "date", "INSPECTION_TIME": "time", "INSPECTOR_SK": "bigint",
        "PERSON_ID": "bigint", "TRANSPORT_MODE": "string", "LINE_ID": "string",
        "STATION_STOP_NAME": "string", "IDENTIFICATION_PROVIDED": "boolean",
        "POLICE_ASSISTANCE": "boolean", "PAID_ON_SITE": "boolean", "CASE_STATUS": "string",
        "CREATED_AT": "timestamp"},
    "bronze_refund": {
        "REFUND_SK": "bigint", "REFUND_ID": "string", "CLAIM_SK": "bigint", "PAYMENT_SK": "bigint",
        "REFUND_AMOUNT": "decimal(10,2)", "REFUND_METHOD": "string", "REFUND_STATUS": "string",
        "REFUND_TIMESTAMP": "timestamp", "TRANSACTION_REFERENCE": "string", "CREATED_AT": "timestamp"},
    "bronze_revizor": {
        "INSPECTOR_SK": "bigint", "BADGE_NO": "string", "EMPLOYEE_NO": "string",
        "VALID_FROM": "date", "VALID_TO": "date", "ORGANISATIONAL_UNIT": "string",
        "EMPLOYMENT_STATUS": "string", "IS_CURRENT": "boolean"},
}

TRUE_VALUES = ["Y", "YES", "TRUE", "T", "1", "A", "ANO"]
FALSE_VALUES = ["N", "NO", "FALSE", "F", "0", "NE"]
NO_FUTURE_CHECK = {"valid_from", "valid_to", "due_date", "discount_deadline"}  # mohou legitimně ležet v budoucnosti
NAME_COLUMNS = {"person_full_name", "person_firstname", "person_surname", "first_name", "last_name"}
MIN_BIRTHDATE = "1910-01-01"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def convert(c, target, source_type):
    """Explicitní převod sloupce c na cílový typ. Neúspěšný převod vrací NULL."""
    col = F.col(c)
    if target == "string":
        s = F.trim(col.cast("string"))
        return F.when(s == "", F.lit(None)).otherwise(s)
    if target == "boolean":
        s = F.upper(F.trim(col.cast("string")))
        return F.when(s.isin(TRUE_VALUES), F.lit(True)).when(s.isin(FALSE_VALUES), F.lit(False))
    if target == "time":  # Spark nemá typ TIME -> text HH:mm:ss
        ts = col if source_type == "timestamp" else F.expr(f"try_cast(`{c}` AS timestamp)")
        return F.date_format(ts, "HH:mm:ss")
    return F.expr(f"try_cast(`{c}` AS {target})")


def issue(cond, column, original, rule, action):
    """Záznam pro karanténu, pokud platí podmínka."""
    return F.when(cond, F.struct(
        F.lit(column).alias("column_name"),
        original.alias("original_value"),
        F.lit(rule).alias("dq_rule"),
        F.lit(action).alias("action")))


def process(bronze_name, spec):
    bronze = spark.table(bronze_name)
    src_types = dict(bronze.dtypes)

    extra = set(bronze.columns) - set(spec) - {"LOADED_AT"}
    if extra:
        print(f"POZOR {bronze_name}: sloupce mimo definované schéma: {extra}")

    # původní řádek jako JSON = dohledatelný původ záznamu v karanténě
    df = (bronze
          .withColumn("_record_json", F.to_json(F.struct(*bronze.columns)))
          .withColumnRenamed("LOADED_AT", "_loaded_at"))

    issues, out_cols = [], []
    for src, target in spec.items():
        dst = src.lower()
        raw = F.col(src)
        original = raw.cast("string")
        v = convert(src, target, src_types[src])

        # 1) převod typu / prázdný text
        if target == "string":
            issues.append(issue(F.trim(original) == "", dst, original, "PRAZDNY_TEXT", "NAHRAZENO_NULL"))
        else:
            issues.append(issue(raw.isNotNull() & (F.trim(original) != "") & v.isNull(),
                                dst, original, "NEPLATNY_TYP", "NAHRAZENO_NULL"))

        # 2) datum/čas v budoucnosti (po čase načtení)
        if target in ("date", "timestamp") and dst not in NO_FUTURE_CHECK:
            cond = v > F.col("_loaded_at")
            issues.append(issue(cond, dst, original, "DATUM_V_BUDOUCNOSTI", "NAHRAZENO_NULL"))
            v = F.when(cond, F.lit(None)).otherwise(v)

        # 3) nereálné datum narození
        if dst == "person_birthdate":
            cond = v < F.lit(MIN_BIRTHDATE).cast("date")
            issues.append(issue(cond, dst, original, "NEREALNE_DATUM_NAROZENI", "NAHRAZENO_NULL"))
            v = F.when(cond, F.lit(None)).otherwise(v)

        # 4) záporná částka
        if target.startswith("decimal"):
            cond = v < 0
            issues.append(issue(cond, dst, original, "ZAPORNA_CASTKA", "NAHRAZENO_NULL"))
            v = F.when(cond, F.lit(None)).otherwise(v)

        # 5) jméno začínající nepísmennými znaky ("- TETZNER")
        if dst in NAME_COLUMNS:
            cond = v.rlike(r"^[^\p{L}]")
            cleaned = F.regexp_replace(v, r"^[^\p{L}]+", "")
            issues.append(issue(cond, dst, original, "NEPLATNE_ZNAKY_VE_JMENU", "UPRAVENO"))
            v = F.when(cond, F.when(cleaned == "", F.lit(None)).otherwise(cleaned)).otherwise(v)

        out_cols.append(v.alias(dst))

    key = list(spec)[0].lower()
    order_col = "created_at" if "CREATED_AT" in spec else "_loaded_at"

    df = df.select(*out_cols, "_loaded_at", "_record_json",
                   F.filter(F.array(*issues), lambda x: x.isNotNull()).alias("_issues"))

    # řádková pravidla: chybějící klíč, duplicita klíče (ponechá se nejnovější)
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc_nulls_last(),
                                        F.col("_loaded_at").desc(), F.col("_record_json"))
    df = df.withColumn("_reject", F.when(F.col(key).isNull(), "CHYBI_KLIC")
                                   .when(F.row_number().over(w) > 1, "DUPLICITA_KLICE"))

    # zápis Silver
    silver_name = bronze_name.replace("bronze_", "silver_")
    (df.filter(F.col("_reject").isNull())
       .select(*[s.lower() for s in spec], "_loaded_at")
       .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_name))

    # záznamy pro karanténu
    base = [F.lit(bronze_name).alias("source_table"), F.col(key).cast("string").alias("record_key")]
    q_values = (df.select(*base, F.explode("_issues").alias("i"), "_loaded_at", "_record_json")
                  .select("source_table", "record_key", "i.column_name", "i.original_value",
                          "i.dq_rule", "i.action", "_loaded_at", "_record_json"))
    q_rows = (df.filter(F.col("_reject").isNotNull())
                .select(*base,
                        F.lit(None).cast("string").alias("column_name"),
                        F.lit(None).cast("string").alias("original_value"),
                        F.col("_reject").alias("dq_rule"),
                        F.lit("VYRAZEN_RADEK").alias("action"),
                        "_loaded_at", "_record_json"))
    return q_values.unionByName(q_rows)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

quarantine = None
for bronze_name, spec in SCHEMAS.items():
    q = process(bronze_name, spec)
    quarantine = q if quarantine is None else quarantine.unionByName(q)

(quarantine.withColumn("_quarantined_at", F.current_timestamp())
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_karantena"))

for bronze_name in SCHEMAS:
    s = bronze_name.replace("bronze_", "silver_")
    print(f"{s}: {spark.table(s).count()} řádků")

(spark.table("silver_karantena")
    .groupBy("source_table", "column_name", "dq_rule", "action").count()
    .orderBy("source_table", "column_name").show(200, truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
