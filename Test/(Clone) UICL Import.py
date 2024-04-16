# Databricks notebook source
# MAGIC %md
# MAGIC ##1. UICL DIM_CLAIM IMPORT

# COMMAND ----------

dim_claim = (spark.read
  .format("sqlserver")
  .option("host", "10.200.13.45")
  .option("port", "1433")
  .option("user", "claims_mi_db")
  .option("password", "U5!bu$168Tse")
  .option("database", "IInsurer_ICE_Prod_Reporting")
  .option("dbtable", "dbo.EXP_DIM_CLAIM")
  .option("trustServerCertificate", True)
  .load()


  .write\
  .format("delta")\
  .mode("overwrite")\
  .option("overwriteSchema", True)\
  .saveAsTable("HIVE_METASTORE.ICE_UICL.DIM_CLAIM")

)

# COMMAND ----------

# MAGIC %md
# MAGIC ##2. UICL DIM_CLAIM_PARTY IMPORT

# COMMAND ----------

dim_claim_party = (spark.read
  .format("sqlserver")
  .option("host", "10.200.13.45")
  .option("port", "1433")
  .option("user", "claims_mi_db")
  .option("password", "U5!bu$168Tse")
  .option("database", "IInsurer_ICE_Prod_Reporting")
  .option("dbtable", "dbo.EXP_DIM_CLAIM_PARTY")
  .option("trustServerCertificate", True)
  .load()


  .write\
  .format("delta")\
  .mode("overwrite")\
  .option("overwriteSchema", True)\
  .saveAsTable("HIVE_METASTORE.ICE_UICL.DIM_CLAIM_PARTY")

)

# COMMAND ----------

# MAGIC %md
# MAGIC ##3. UICL TRN_CLAIM IMPORT

# COMMAND ----------

trn_claim = (spark.read
  .format("sqlserver")
  .option("host", "10.200.13.45")
  .option("port", "1433")
  .option("user", "claims_mi_db")
  .option("password", "U5!bu$168Tse")
  .option("database", "IInsurer_ICE_Prod_Reporting")
  .option("dbtable", "dbo.EXP_TRN_CLAIM")
  .option("trustServerCertificate", True)
  .load()


  .write\
  .format("delta")\
  .mode("overwrite")\
  .option("overwriteSchema", True)\
  .saveAsTable("HIVE_METASTORE.ICE_UICL.TRN_CLAIM")

)

# COMMAND ----------

# MAGIC %md
# MAGIC ##4. UICL TRN_CLAIM_PARTY IMPORT

# COMMAND ----------

trn_claim_party = (spark.read
  .format("sqlserver")
  .option("host", "10.200.13.45")
  .option("port", "1433")
  .option("user", "claims_mi_db")
  .option("password", "U5!bu$168Tse")
  .option("database", "IInsurer_ICE_Prod_Reporting")
  .option("dbtable", "dbo.EXP_TRN_CLAIM_PARTY")
  .option("trustServerCertificate", True)
  .load()


  .write\
  .format("delta")\
  .mode("overwrite")\
  .option("overwriteSchema", True)\
  .saveAsTable("HIVE_METASTORE.ICE_UICL.TRN_CLAIM_PARTY")

)
