# Databricks notebook source
# MAGIC %md
# MAGIC ## ICE_AAS.TBL_AAS_RESERVED

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS ICE_AAS.TBL_AAS_RESERVED;
# MAGIC
# MAGIC CREATE TABLE ICE_AAS.TBL_AAS_RESERVED AS (
# MAGIC
# MAGIC SELECT 
# MAGIC
# MAGIC DCLAIM.CLAIM_id As ClaimID
# MAGIC 	   ,DCLAIM.claim_number as ClaimNumber
# MAGIC 	   ,TCLAIM.position_code
# MAGIC 	   ,TCLAIM.status_code
# MAGIC 	  ,DCLAIM.notification_date AS NotificationDate
# MAGIC ,DCLAIM.incident_type_description AS Peril
# MAGIC ,DCLAIM.liability_decision AS LiabilityDecision
# MAGIC       ,DJOB.job_Id as JobID
# MAGIC 	  ,DJOB.party_type as PartyType
# MAGIC 	  ,DJOB.job_type as JobType
# MAGIC 	  ,DJOB.cntct_party_id
# MAGIC 	  ,DCP.TITLE+' '+DCP.FORENAME+' '+DCP.SURNAME AS PartyName
# MAGIC 	  ,CASE 
# MAGIC 		WHEN party_type = 'First Party' AND job_type = 'MOTOR' THEN 'IVD'
# MAGIC 		WHEN party_type = 'Third Party' AND job_type = 'MOTOR' THEN 'TPVD' 
# MAGIC 		WHEN party_type = 'Third Party' AND job_type = 'PI' THEN 'PI' 
# MAGIC 		WHEN party_type = 'Third Party' AND job_type = 'GENERIC' THEN 'TPPD'
# MAGIC 		ELSE NULL 
# MAGIC 		END AS SubClaim
# MAGIC 		,HOD.HOD_DESCRIPTION as HODDescription
# MAGIC 		,RESTRAN.RESERVE_AMOUNT as ReserveAmount
# MAGIC
# MAGIC
# MAGIC   FROM hive_metastore.ice_aas.ice_trn_reserve RESTRAN
# MAGIC
# MAGIC   LEFT JOIN  hive_metastore.ice_aas.ice_dim_reserve RESDIM on RESTRAN.RESERVE_ID = RESDIM.RESERVE_ID AND RESDIM.CURRENT_FLAG = 'Y'
# MAGIC   LEFT JOIN hive_metastore.ice_aas.ice_dim_head_of_damage  HOD ON RESTRAN.HEAD_OF_DAMAGE_KEY = HOD.HEAD_OF_DAMAGE_KEY
# MAGIC   LEFT JOIN hive_metastore.ice_aas.ice_dim_claim  DCLAIM ON RESTRAN.CLAIM_ID = DCLAIM.CLAIM_ID AND DCLAIM.CURRENT_FLAG = 'Y'
# MAGIC   LEFT JOIN hive_metastore.ice_aas.ice_trn_claim TCLAIM ON TCLAIM.CLAIM_ID = DCLAIM.claim_id AND TCLAIM.CURRENT_FLAG = 'Y'
# MAGIC   LEFT JOIN hive_metastore.ice_aas.ice_dim_job DJOB ON RESTRAN.JOB_ID = DJOB.JOB_ID AND DJOB.CURRENT_FLAG = 'Y'
# MAGIC   LEFT JOIN  hive_metastore.ice_aas.ice_dim_claim_party DCP ON DCP.PARTY_ID = DJOB.cntct_party_id AND DCP.CURRENT_FLAG = 'Y' 
# MAGIC
# MAGIC
# MAGIC
# MAGIC   Where RESTRAN.CURRENT_FLAG = 'Y'
# MAGIC )
# MAGIC
# MAGIC
# MAGIC
