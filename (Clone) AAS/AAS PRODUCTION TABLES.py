# Databricks notebook source
# MAGIC %md
# MAGIC ## AAS Production Tables Routine
# MAGIC The below runs the production table scripts in dependancy order.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run AAS Claim Summary

# COMMAND ----------

# MAGIC %run /Users/david.kemp@theaa.com/AAS/TBL_AAS_CLAIM_SUMMARY

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run AAS Deployments

# COMMAND ----------

# MAGIC %run /Users/david.kemp@theaa.com/AAS/TBL_AAS_DEPLOYMENTS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run AAS Payments

# COMMAND ----------

# MAGIC %run /Users/david.kemp@theaa.com/AAS/TBL_AAS_PAYMENTS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run AAS Complaints

# COMMAND ----------

# MAGIC %run /Users/david.kemp@theaa.com/AAS/TBL_AAS_COMPLAINTS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run AAS Handlers

# COMMAND ----------

# MAGIC %run /Users/david.kemp@theaa.com/AAS/TBL_AAS_HANDLERS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run AAS Third Parties

# COMMAND ----------

# MAGIC %run /Users/david.kemp@theaa.com/AAS/TBL_AAS_THIRD_PARTIES

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run AAS Recovery Basic

# COMMAND ----------

# MAGIC %run /Users/david.kemp@theaa.com/AAS/TBL_AAS_RECOVERY_BASIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run AAS Reserved

# COMMAND ----------

# MAGIC %run /Users/david.kemp@theaa.com/AAS/TBL_AAS_RESERVED
