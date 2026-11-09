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
# META     },
# META     "mirrored_db": {
# META       "known_mirrored_dbs": [
# META         {
# META           "id": "efca03d8-ca80-4f51-83bd-7b3baed3c015"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import com.microsoft.sqlserver.jdbc.spark
from pyspark.sql.functions import current_timestamp

# připojení ke zdrojové databázi
url = "jdbc:sqlserver://wosfckz7iqdejdfedecwu6neiq-rn6ftjrgxlbufhaldgmeqjfm2q.database.fabric.microsoft.com:1433;database=SQL_4IT433-efca03d8-ca80-4f51-83bd-7b3baed3c015;"

tables = [
    "CESTUJICI", 
    "CLAIM_REKLAMACE_POKUTY", 
    "COLLECTION_VYMAHANI", 
    "PLATBA", 
    "PODNET_CESTUJICIHO", 
    "POHLEDAVKA", 
    "PREPRAVNI_KONTROLA", 
    "REFUND", 
    "REVIZOR"
]

for table in tables:
    # načtení ze zdroje
    df_source = spark.read.option("url", url).mssql(f"dbo.{table}")
    
    # přidání loaded_at
    df_bronze = df_source.withColumn("LOADED_AT", current_timestamp())
    
    # uložení pod názvem bronze_x
    bronze_table_name = f"bronze_{table.lower()}"
    df_bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(bronze_table_name)
    
    print(f"✅ tabulka {table} uložena: {bronze_table_name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
