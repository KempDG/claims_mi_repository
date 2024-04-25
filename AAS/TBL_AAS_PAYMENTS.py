# Databricks notebook source
# MAGIC %md
# MAGIC ## ICE_AAS.TBL_AAS_PAYMENTS

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS ICE_AAS.TBL_AAS_PAYMENTS;
# MAGIC
# MAGIC CREATE TABLE ICE_AAS.TBL_AAS_PAYMENTS AS (
# MAGIC
# MAGIC SELECT 
# MAGIC DISTINCT
# MAGIC DC.CLAIM_ID,
# MAGIC DC.CLAIM_NUMBER,
# MAGIC DC.EXTERNAL_REFERENCE,
# MAGIC DC.NOTIFICATION_DATE,
# MAGIC DJ.party_type,
# MAGIC DJ.job_type,
# MAGIC acs.Workstream,
# MAGIC DP.PAYMENT_ID,
# MAGIC TP.INVOICE_ID,
# MAGIC TP.JOB_ID,
# MAGIC INV.JOB_COMPONENT_ID,
# MAGIC TP.CURRENT_FLAG,
# MAGIC tp.STATUS_ID,
# MAGIC TP.HOD_ID,
# MAGIC HOD.HOD_DESCRIPTION,
# MAGIC --ORGANISATION_NAME AS Party,
# MAGIC CASE WHEN DFS.Status_description = 'Stopped' THEN -TP.PAYMENT_TOTAL ELSE Tp.Payment_Total END AS Payment_Total,
# MAGIC CASE WHEN DFS.Status_description = 'Stopped' THEN -Tp.ALLOCATION_AMOUNT ELSE Tp.Allocation_amount END AS Allocation_Amount,
# MAGIC Deduction.Deduction,
# MAGIC --TP.ALLOCATION_AMOUNT,
# MAGIC --TP.PAYMENT_TOTAL,
# MAGIC TP.ISSUED_DATE,
# MAGIC TP.REQUESTED_DATE,
# MAGIC TP.APPROVED_DATE,
# MAGIC DP.CHEQUE_RUN_DATE,
# MAGIC DP.PAYMENT_METHOD,
# MAGIC HOD.HOD_DESCRIPTION as TRANS_CODE_DESCRIPTION,
# MAGIC DP.TRANSACTION_REASON,
# MAGIC CASE WHEN DP.PAYMENT_ID = 10341 THEN '8000035' ELSE DP.PAYEE_REFERENCE END AS Payee_Reference,
# MAGIC TP.PAYEE_PARTY_ID,
# MAGIC PAYMENTSTATUS.Status_description,
# MAGIC CASE 
# MAGIC WHEN DSP.Company_Name is not null then DSP.COMPANY_NAME
# MAGIC   WHEN DCP.ORGANISATION_NAME is not null then DCP.ORGANISATION_NAME
# MAGIC else  DCP.Forename + ' ' + DCP.SURNAME END AS Payee, 
# MAGIC CASE WHEN DSP.ServiceProvider_Job_comp_Type is not null then DSP.ServiceProvider_Job_comp_Type 
# MAGIC ELSE DCP.partytype_code end as PayeeType,
# MAGIC DSP.network_approved,
# MAGIC REQUESTEDUSER.full_username AS RequestedBy,
# MAGIC APPROVEDUSER.full_username AS ApprovedBy,
# MAGIC tp.INCLUDE_IN_TOTAL,
# MAGIC DP.TRANSTYPE_CODE,
# MAGIC DP.TRANSTYPE_CODE_ORIGINAL
# MAGIC
# MAGIC
# MAGIC  FROM hive_metastore.ice_aas.ice_trn_payment TP
# MAGIC  --PAYEE
# MAGIC --LEFT JOIN ICE_AccidentAssist_Live.dbo.EXP_DIM_CLAIM_PARTY DCPAYEE ON TP.PAYEE_KEY = DCPAYEE.CLAIM_PARTY_KEY 
# MAGIC   LEFT JOIN hive_metastore.ice_aas.ice_dim_payment DP ON TP.PAYMENT_ID = DP.PAYMENT_ID and DP.CURRENT_FLAG = 'Y'
# MAGIC   LEFT JOIN hive_metastore.ice_aas.ice_dim_head_of_damage HOD ON TP.HOD_ID = HOD.HOD_ID and HOD.CURRENT_FLAG = 'Y'
# MAGIC   INNER JOIN hive_metastore.ice_aas.ice_dim_claim DC ON TP.claim_id = DC.claim_id and dc.current_flag = 'Y'
# MAGIC   LEFT JOIN hive_metastore.ice_aas.ice_dim_job DJ ON TP.job_id = DJ.job_id and dj.current_flag = 'Y'
# MAGIC   LEFT JOIN hive_metastore.ice_aas.ice_dim_claim_party DCP ON TP.PAYEE_PARTY_ID = DCP.PARTY_ID AND DCP.CURRENT_FLAG = 'Y'
# MAGIC   LEFT JOIN  hive_metastore.ice_aas.ice_dim_financial_status DFS ON TP.STATUS_ID = DFS.STATUS_ID AND DFS.CURRENT_FLAG = 'Y'
# MAGIC   LEFT JOIN hive_metastore.ice_aas.tbl_aas_claim_summary ACS ON ACS.CLAIM_NUMBER = DC.claim_number
# MAGIC   
# MAGIC   LEFT JOIN 
# MAGIC   (
# MAGIC   select 
# MAGIC   serviceprovider_party_id,
# MAGIC   company_name,
# MAGIC   max(ServiceProvider_Job_comp_Type) AS ServiceProvider_Job_comp_Type,
# MAGIC   max(network_approved) as network_approved
# MAGIC   from 
# MAGIC  hive_metastore.ice_aas.ice_dim_serviceprovider
# MAGIC   WHERE CURRENT_FLAG = 'Y'
# MAGIC   group by serviceprovider_party_id,company_name
# MAGIC   ) DSP ON DCP.party_id = DSP.SERVICEPROVIDER_party_id 
# MAGIC
# MAGIC     Left join
# MAGIC   (
# MAGIC   select
# MAGIC   claim_id,
# MAGIC   payment_id,
# MAGIC   sum(allocation_amount) as allocation_amount,
# MAGIC   max(PAYMENT_CURRENCY_TOTAL) as PAYMENT_CURRENCY_TOTAL,
# MAGIC   sum(allocation_amount) - max(PAYMENT_CURRENCY_TOTAL) as Deduction
# MAGIC
# MAGIC   from hive_metastore.ice_aas.ice_trn_payment
# MAGIC   where CURRENT_FLAG = 'y'
# MAGIC  
# MAGIC
# MAGIC   group by
# MAGIC   claim_id,
# MAGIC   payment_id
# MAGIC   ) Deduction on Deduction.claim_id = TP.claim_id and Deduction.PAYMENT_ID = tp.PAYMENT_ID
# MAGIC   
# MAGIC   --GET PAYMENT STATUS
# MAGIC   LEFT JOIN hive_metastore.ice_aas.ice_dim_financial_status PAYMENTSTATUS ON TP.status_id = PAYMENTSTATUS.status_id
# MAGIC
# MAGIC   
# MAGIC   --WHO REEQUESTED PAYMENT
# MAGIC   LEFT JOIN hive_metastore.ice_aas.ice_dim_user REQUESTEDUSER ON TP.REQUESTED_BY_PARTY_ID = REQUESTEDUSER.user_party_id
# MAGIC   LEFT JOIN hive_metastore.ice_aas.ice_dim_user APPROVEDUSER ON TP.APPROVED_BY_PARTY_ID = APPROVEDUSER.user_party_id
# MAGIC
# MAGIC   --Add Invoice_ID for Job_Comp_ID so the payment can link to the job via the invoice
# MAGIC   LEFT JOIN hive_metastore.ice_aas.ice_trn_invoice INV ON INV.INVOICE_ID = TP.INVOICE_ID AND INV.CURRENT_FLAG = 'Y'
# MAGIC
# MAGIC   where TP.CURRENT_FLAG = 'Y'
# MAGIC
# MAGIC
# MAGIC )
# MAGIC
