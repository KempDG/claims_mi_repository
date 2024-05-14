# Databricks notebook source
# MAGIC %md
# MAGIC ## AAS Claim Summary Archive Routine
--THIS LINE IS A TEST

# COMMAND ----------

from pyspark.sql.functions import current_date

# Get the current date
current_date = current_date()

# Extract year, month, and day from the current date using SQL statements
year = spark.sql("SELECT YEAR(CURRENT_DATE()) AS year").first()[0]
month = spark.sql("SELECT MONTH(CURRENT_DATE()) AS month").first()[0]
day = spark.sql("SELECT DAY(CURRENT_DATE()) AS day").first()[0]

# Set the variable ArchiveDateClaims
archive_date_claims = f"`ARC_AAS_CLAIM_SUMMARY_{year}_{month}_{day}`"

# Drop the table using Spark API
spark.sql(f"""
        DROP TABLE IF EXISTS ICE_AAS.{archive_date_claims}
        """)

# Create delta live table using DLT
spark.sql(f"""
        CREATE TABLE ICE_AAS.{archive_date_claims} USING DELTA
        AS (SELECT * FROM ICE_AAS.TBL_AAS_CLAIM_SUMMARY)
        """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## AAS Complaints Archive Routine

# COMMAND ----------

from pyspark.sql.functions import current_date

# Get the current date
current_date = current_date()

# Extract year, month, and day from the current date using SQL statements
year = spark.sql("SELECT YEAR(CURRENT_DATE()) AS year").first()[0]
month = spark.sql("SELECT MONTH(CURRENT_DATE()) AS month").first()[0]
day = spark.sql("SELECT DAY(CURRENT_DATE()) AS day").first()[0]

# Set the variable ArchiveDateClaims
archive_date_claims = f"`ARC_AAS_COMPLAINTS_{year}_{month}_{day}`"

# Drop the table using Spark API
spark.sql(f"""
        DROP TABLE IF EXISTS ICE_AAS.{archive_date_claims}
        """)

# Create delta live table using DLT
spark.sql(f"""
        CREATE TABLE ICE_AAS.{archive_date_claims} USING DELTA
        AS (SELECT * FROM ICE_AAS.TBL_AAS_COMPLAINTS)
        """)
