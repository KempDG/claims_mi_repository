# Databricks notebook source
# MAGIC %md
# MAGIC ## ICE_AAS.TBL_AAS_COMPLAINTS

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ICE_AAS.TBL_AAS_COMPLAINTS;
# MAGIC
# MAGIC CREATE TABLE ICE_AAS.TBL_AAS_COMPLAINTS 
# MAGIC AS (
# MAGIC
# MAGIC SELECT 
# MAGIC   'AAS' AS System,
# MAGIC   DCLAIM.claim_number,
# MAGIC   DCLAIM.NOTIFICATION_DATE,
# MAGIC   CASE 
# MAGIC 	  WHEN POLICY.POLICY_NUMBER IS NULL AND DCLAIM.CLAIM_NUMBER LIKE 'AAS%' 
# MAGIC         AND DCP.ORGANISATION_NAME IN ('AA Underwriting','AA Underwriting NM','AA Underwriting Smart','AA Silver','AA Gold','AA Platinum') 
# MAGIC         THEN 'AAUICL'
# MAGIC 	  WHEN POLICY.POLICY_NUMBER IS NULL AND DCLAIM.CLAIM_NUMBER LIKE 'AAS%' AND DCP.ORGANISATION_NAME IS NOT NULL THEN 'Panel' 
# MAGIC 	  WHEN POLICY.POLICY_NUMBER IS NULL AND DCLAIM.CLAIM_NUMBER LIKE 'MEM%' THEN 'Member'
# MAGIC 	  WHEN POLICY.POLICY_NUMBER IS NOT NULL AND DCLAIM.CLAIM_NUMBER LIKE 'AAS%' 
# MAGIC         AND POLICY.POLICY_NUMBER LIKE 'AAPMB%' THEN 'AAUICL'
# MAGIC 	  WHEN POLICY.POLICY_NUMBER IS NOT NULL AND DCLAIM.CLAIM_NUMBER LIKE 'AAS%' 
# MAGIC         AND POLICY.POLICY_NUMBER LIKE 'AAMOBR%' THEN 'AAUICL'
# MAGIC 	  WHEN POLICY.POLICY_NUMBER IS NOT NULL AND DCLAIM.CLAIM_NUMBER LIKE 'AAS%' 
# MAGIC         AND POLICY.POLICY_NUMBER NOT LIKE 'AAPMB%' THEN 'Panel' 
# MAGIC 	  WHEN POLICY.POLICY_NUMBER IS NOT NULL AND DCLAIM.CLAIM_NUMBER LIKE 'AAS%' 
# MAGIC         AND POLICY.POLICY_NUMBER NOT LIKE 'AAMOBR%' THEN 'AAUICL'
# MAGIC 	  WHEN POLICY.POLICY_NUMBER IS NOT NULL AND DCLAIM.CLAIM_NUMBER LIKE 'MEM%' THEN 'Member'
# MAGIC 	  ELSE NULL
# MAGIC    END AS Workstream,
# MAGIC   POLICY.policy_number,
# MAGIC   POLICY.inception_date,
# MAGIC   DCP.organisation_name AS Insurer,
# MAGIC   Subrogated AS ACCMAN,
# MAGIC   CASE WHEN datediff(day, TRANCOMPLAINT.created_date, resolution_date) <= 1 THEN 'Y' ELSE 'N' END AS ResolvedWithin24Hours,
# MAGIC   dimcomment.status,
# MAGIC   DCLAIM.Claim_key,
# MAGIC   reference,
# MAGIC   from_party_id,
# MAGIC   LTRIM(COMPLAINANT.TITLE + ' ' + COMPLAINANT.FORENAME + ' ' + COMPLAINANT.SURNAME) AS Complainant,
# MAGIC   COMPLAINANT.partytype_code,
# MAGIC   DIMCOMMENT.Complaint_id,
# MAGIC   ComplaintOnClaimCount,
# MAGIC   complaint_category,
# MAGIC   complaint_reason,
# MAGIC   DIMCOMMENT.justified,
# MAGIC   Description AS Complaint_Description,
# MAGIC   via_party_id,
# MAGIC   TRANCOMPLAINT.created_date,
# MAGIC   received_date,
# MAGIC   CAST(received_date AS Timestamp) AS ReceivedTime,
# MAGIC   acknowledged_date,
# MAGIC   client_notified_date,
# MAGIC   complaint_upheld_date,
# MAGIC   concluded_date,
# MAGIC   FORMAL_COMPLAINT_DATE,
# MAGIC   resolution_date,
# MAGIC   target_resolution_date,
# MAGIC   target_commitment_date,
# MAGIC   datediff(day, received_date, IFNULL(resolution_date, current_timestamp()))+1 AS days_open,
# MAGIC   compensation,
# MAGIC   CREATEDUSER.full_username AS CreatedBy,
# MAGIC   RESOLVEDUSER.full_username AS ResolvedBy,
# MAGIC   CONCLUDEDUSER.full_username AS ConcludedBy,
# MAGIC   HANDLERUSER.full_username AS Handler,
# MAGIC   COMPLAINT_FOS_OUTCOME,
# MAGIC   DIMCOMMENT.ORIGIN,
# MAGIC   resolution_description,
# MAGIC   complaint_resolution,
# MAGIC   type_description,
# MAGIC   complaint_item_description,
# MAGIC   comment,
# MAGIC   CASE WHEN datediff(day, TRANCOMPLAINT.created_date, resolution_date) <= 4 THEN 'Y' ELSE 'N' END AS ResolvedWithinInformalTime,
# MAGIC   complaint_notified_via AS NotifiedVia,
# MAGIC   CASE WHEN (DATEDIFF(day, received_date, resolution_date) 
# MAGIC        - (DATEDIFF(week, received_date, resolution_date) * 2) 
# MAGIC        - CASE WHEN DATE_PART('DOW', received_date) = 1 THEN 1 ELSE 0 END 
# MAGIC        + CASE WHEN DATE_PART('DOW', resolution_date) = 1 THEN 1 ELSE 0 END) <=3 THEN 'Y' ELSE 'N' END AS ResolvedWithinInformalPeriodNew,
# MAGIC   CASE WHEN dimcomment.status IN ('Resolved', 'Concluded') AND complaint_resolution <> 'Erroneous' 
# MAGIC         AND complaint_reason <> 'EOD' THEN 1 ELSE 0 END AS ResolvedTotalCount,
# MAGIC   CASE WHEN (DATEDIFF(day, received_date, resolution_date) 
# MAGIC        - (DATEDIFF(week, received_date, resolution_date) * 2) 
# MAGIC        - CASE WHEN DATE_PART('DOW', received_date) = 1 THEN 1 ELSE 0 END 
# MAGIC        + CASE WHEN DATE_PART('DOW', resolution_date) = 1 THEN 1 ELSE 0 END) <=3 
# MAGIC         AND complaint_resolution <> 'Erroneous' AND complaint_reason NOT IN ('EOD', 'Feedback') THEN 1 ELSE 0 END AS ResolvedInformCount
# MAGIC FROM ice_aas.ice_trn_complaint TRANCOMPLAINT    
# MAGIC LEFT JOIN ice_aas.ice_dim_claim DCLAIM ON TRANCOMPLAINT.CLAIM_ID = DCLAIM.CLAIM_ID AND DCLAIM.CURRENT_FLAG = 'Y'
# MAGIC LEFT JOIN ice_aas.ice_trn_claim TC ON TC.CLAIM_ID = DCLAIM.CLAIM_ID AND TC.CURRENT_FLAG = 'Y'
# MAGIC LEFT JOIN ice_aas.ice_dim_complaint DIMCOMMENT ON DIMCOMMENT.COMPLAINT_ID = TRANCOMPLAINT.COMPLAINT_ID 
# MAGIC   AND DIMCOMMENT.CURRENT_FLAG = 'Y'
# MAGIC LEFT JOIN ice_aas.ice_trn_complaint_item TCI ON TCI.complaint_id = TRANCOMPLAINT.complaint_id AND TCI.CURRENT_FLAG = 'Y' 
# MAGIC LEFT JOIN ice_aas.ice_dim_claim_party COMPLAINANT ON TRANCOMPLAINT.FROM_PARTY_ID = COMPLAINANT.PARTY_ID 
# MAGIC   AND COMPLAINANT.CURRENT_FLAG = 'Y' AND TRANCOMPLAINT.FROM_PARTY_KEY = COMPLAINANT.CLAIM_PARTY_KEY
# MAGIC LEFT JOIN ice_aas.ice_dim_user CREATEDUSER ON TRANCOMPLAINT.CREATED_BY_ID = CREATEDUSER.user_party_id 
# MAGIC   AND CREATEDUSER.CURRENT_FLAG = 'Y'
# MAGIC LEFT JOIN ice_aas.ice_dim_user RESOLVEDUSER ON TRANCOMPLAINT.RESOLVED_BY_ID = RESOLVEDUSER.user_party_id 
# MAGIC   AND RESOLVEDUSER.CURRENT_FLAG = 'Y'
# MAGIC LEFT JOIN ice_aas.ice_dim_user CONCLUDEDUSER ON TRANCOMPLAINT.CONCLUDED_BY_ID = CONCLUDEDUSER.user_party_id 
# MAGIC   AND CONCLUDEDUSER.CURRENT_FLAG = 'Y'
# MAGIC LEFT JOIN ice_aas.ice_dim_user HANDLERUSER ON TRANCOMPLAINT.HANDLER_ID = HANDLERUSER.user_party_id 
# MAGIC   AND HANDLERUSER.CURRENT_FLAG = 'Y'
# MAGIC LEFT JOIN ice_aas.ice_trn_claim_party TCP ON DCLAIM.CLAIM_ID = TCP.CLAIM_ID AND TCP.CURRENT_FLAG = 'Y' 
# MAGIC   AND TCP.NATURE = 'INSURER'
# MAGIC LEFT JOIN ice_aas.ice_dim_claim_party DCP ON TCP.CLAIM_PARTY_ID = DCP.PARTY_ID AND DCP.CURRENT_FLAG = 'Y'
# MAGIC LEFT JOIN ice_aas.ice_dim_policy POLICY ON TC.POLICY_ID = POLICY.POLICY_ID 
# MAGIC   AND POLICY.policy_snapshot_id = TC.policy_snapshot_id AND POLICY.CURRENT_FLAG = 'Y'
# MAGIC LEFT JOIN (
# MAGIC   SELECT ROW_NUMBER() OVER (PARTITION BY DIMCOMMENT.claim_id ORDER BY complaint_id ASC) AS ComplaintOnClaimCount,
# MAGIC     DIMCOMMENT.claim_id,
# MAGIC     claim_number,
# MAGIC     complaint_id
# MAGIC   FROM ice_aas.ice_trn_complaint DIMCOMMENT 
# MAGIC   INNER JOIN ice_aas.ice_dim_claim DCLAIM ON DCLAIM.claim_id = DIMCOMMENT.claim_id 
# MAGIC     AND DCLAIM.current_flag = 'y'
# MAGIC   WHERE DIMCOMMENT.CURRENT_FLAG = 'Y'
# MAGIC ) ClaimCount ON ClaimCount.Claim_id = DCLAIM.CLAIM_ID 
# MAGIC   AND ClaimCount.complaint_id = DIMCOMMENT.COMPLAINT_ID
# MAGIC )
