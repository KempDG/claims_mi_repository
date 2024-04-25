# Databricks notebook source
# MAGIC %md
# MAGIC ## ICE_AAS.TBL_AAS_HANDLERS

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS ICE_AAS.TBL_AAS_HANDLERS;
# MAGIC
# MAGIC CREATE TABLE ICE_AAS.TBL_AAS_HANDLERS AS (
# MAGIC
# MAGIC Select DC.claim_number as ClaimNumber
# MAGIC 	  ,DCP.FORENAME + ' ' + DCP.SURNAME as Handler
# MAGIC 	  ,HT.handler_type_description as Team
# MAGIC 	  ,DateHandlerAssigned
# MAGIC 	  ,case when DateHandlerRemoved = '9999-12-31 23:59:59.000' then NULL else DateHandlerRemoved end as DateHandlerRemoved
# MAGIC 	  ,case when DateHandlerRemoved = '9999-12-31 23:59:59.000' then 'Current' else 'Replaced' end as HandlerStatus
# MAGIC
# MAGIC
# MAGIC from
# MAGIC ( 
# MAGIC Select A.claim_id
# MAGIC ,A.claim_party_id
# MAGIC ,A.EFF_START as DateHandlerAssigned
# MAGIC ,B.EFF_END as DateHandlerRemoved
# MAGIC ,A.handler_type_id
# MAGIC from ( 
# MAGIC SELECT DISTINCT  claim_id
# MAGIC       ,claim_party_id
# MAGIC       ,EFF_START
# MAGIC 	  ,Nature
# MAGIC 	  ,handler_type_id
# MAGIC   FROM ice_aas.ice_trn_claim_party
# MAGIC )  A
# MAGIC LEFT JOIN ( 
# MAGIC SELECT DISTINCT  claim_id
# MAGIC       ,claim_party_id
# MAGIC 	  ,EFF_START
# MAGIC       ,MIN(EFF_END) as EFF_END
# MAGIC   FROM ice_aas.ice_trn_claim_party
# MAGIC   group by 
# MAGIC    claim_id
# MAGIC       ,claim_party_id
# MAGIC 	  ,EFF_STart
# MAGIC   ) B ON A.claim_id = B.claim_id and A.claim_party_id = B.claim_party_id and A.EFF_START = B.EFF_START
# MAGIC   where Nature = 'CLAIM_HANDLER'
# MAGIC   ) Handlers
# MAGIC
# MAGIC LEFT JOIN ice_aas.ice_dim_claim_party DCP ON DCP.PARTY_ID = Handlers.claim_party_id and DCP.CURRENT_FLAG = 'Y'
# MAGIC LEFT JOIN ice_aas.ice_dim_handler_type HT ON HT.handler_type_id = Handlers.handler_type_id and HT.CURRENT_FLAG = 'Y'
# MAGIC LEFT JOIN ice_aas.ice_dim_claim DC ON DC.claim_id = Handlers.claim_id and DC.CURRENT_FLAG = 'Y'
# MAGIC
# MAGIC order by claim_number
# MAGIC
# MAGIC )
# MAGIC
# MAGIC
# MAGIC
