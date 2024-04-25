# Databricks notebook source
# MAGIC %md
# MAGIC ## ICE_AAS.TBL_AAS_RECOVERY_BASIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS ICE_AAS.TBL_AAS_RECOVERY_BASIC;
# MAGIC
# MAGIC CREATE TABLE ICE_AAS.TBL_AAS_RECOVERY_BASIC AS (
# MAGIC
# MAGIC select * from (
# MAGIC select distinct
# MAGIC 'AAS' as system,
# MAGIC dc.claim_number,
# MAGIC i.claim_id,
# MAGIC i.invoice_id,
# MAGIC i.invoice_line_id,
# MAGIC sc.status_code,
# MAGIC sc.position_code,
# MAGIC DATE_FORMAT(dc.notification_date,'yyyy-MM-dd') as notification_date,
# MAGIC ifnull(dc.tp_insurer,'NA') as tp_insurer,
# MAGIC deb.partytype_code,
# MAGIC case when deb.partytype_code='insurer' then deb.organisation_name
# MAGIC when deb.partytype_code='driver' then ifnull(deb.title,'') + ' ' + ifnull(deb.forename,'') + ' ' + ifnull(deb.surname,'')
# MAGIC when deb.partytype_code='person' then ifnull(deb.title,'') + ' ' + ifnull(deb.forename,'') + ' ' + ifnull(deb.surname,'')
# MAGIC when deb.partytype_code='organisation' then deb.organisation_name
# MAGIC when deb.partytype_code='company' then deb.companyname
# MAGIC when deb.partytype_code='service_provider' then deb.organisation_name
# MAGIC end as invoice_to,
# MAGIC ifnull(dc.liability_decision,'NA') as liability_decision,
# MAGIC dc.subrogated,
# MAGIC cs.workstream,
# MAGIC 'Invoice' as outlayrequestedmethod,
# MAGIC i.job_id,
# MAGIC job_sub_type_description as milestone,
# MAGIC i.invoice_to_id,
# MAGIC DATE_FORMAT(i.transaction_date,'yyyy-MM-dd') as recoveryrequesteddate,
# MAGIC i.transaction_date as recoveryrequesteddateraw,
# MAGIC --invoice_line_transaction_date,
# MAGIC i.gross_amt as recoverydemanded,
# MAGIC dic.invoice_category_description,
# MAGIC u.full_username as outlayrequestedby,
# MAGIC ch.charge_type_description,
# MAGIC di.type as invoicetype,
# MAGIC di.ledger_type,
# MAGIC ifnull(di.credit_note_reason,'NA') as creditnotereason,
# MAGIC di.status,
# MAGIC invoice_type as invoice_type_description,
# MAGIC
# MAGIC ifnull((
# MAGIC select
# MAGIC ag.full_username as recoveryhandler
# MAGIC from hive_metastore.ice_aas.ice_trn_claim_party  hp
# MAGIC inner join hive_metastore.ice_aas.ice_dim_user ag on hp.claim_party_id = ag.user_party_id
# MAGIC where handler_type_id = 4 and hp.current_flag = 'y' and ag.current_flag = 'y' and dc.claim_id = claim_id
# MAGIC order by hp.eff_start desc, hp.eff_end desc limit 1
# MAGIC ),'NA')
# MAGIC as recoveryhandler,
# MAGIC 1 as rowno,
# MAGIC
# MAGIC case when type='credit' and credit_note_reason in ('OUTLAY_AMENDMENT','OUTLAY_REDIRECTION') then i.gross_amt else 0 end as creditnoteser,
# MAGIC case when type='credit' and credit_note_reason not in ('OUTLAY_AMENDMENT','OUTLAY_REDIRECTION') then i.gross_amt else 0 end as creditnoteaban,
# MAGIC
# MAGIC case when type='credit' and credit_note_reason in ('OUTLAY_AMENDMENT','OUTLAY_REDIRECTION') then i.gross_amt else 0 end as crediterror,
# MAGIC case when type='credit' and credit_note_reason in ('OUTLAY_AMENDMENT','OUTLAY_REDIRECTION','DISCOUNT','COMSETTLEMENT') then i.gross_amt else 0 end as creditcleansed,
# MAGIC
# MAGIC 0 as negotiationscreen
# MAGIC
# MAGIC from hive_metastore.ice_aas.ice_trn_invoice i
# MAGIC inner join hive_metastore.ice_aas.ice_dim_invoice di on i.invoice_id=di.invoice_id and di.current_flag='Y'
# MAGIC inner join hive_metastore.ice_aas.ice_dim_claim dc on i.claim_id=dc.claim_id and dc.current_flag='Y'
# MAGIC inner join hive_metastore.ice_aas.ice_trn_claim sc on i.claim_id=sc.claim_id and sc.current_flag='Y'
# MAGIC inner join hive_metastore.ice_aas.ice_dim_user u on i.performed_by=u.user_party_id
# MAGIC inner join hive_metastore.ice_aas.ice_dim_charge_type ch on i.CHARGE_TYPE_KEY=ch.CHARGE_TYPE_KEY
# MAGIC left join hive_metastore.ice_aas.ice_trn_claim_party tpc on tpc.claim_id=i.claim_id and tpc.current_flag='Y'
# MAGIC left join hive_metastore.ice_aas.ice_dim_claim_party dcp on tpc.claim_party_id=dcp.party_id and dcp.current_flag='Y'
# MAGIC left join hive_metastore.ice_aas.ice_dim_claim_party deb on deb.party_id=i.invoice_to_id and deb.CURRENT_FLAG='Y' and i.current_flag='Y'
# MAGIC left join hive_metastore.ice_aas.ice_trn_claim tc on tc.claim_id=i.claim_id and tc.CURRENT_FLAG='Y'
# MAGIC left join hive_metastore.ice_aas.ice_dim_policy po on tc.policy_key=po.policy_key and po.current_flag='Y'
# MAGIC left join hive_metastore.ice_aas.ice_dim_invoice_category dic on i.invoice_category_id=dic.invoice_category_id and dic.current_flag='Y'
# MAGIC left join hive_metastore.ice_aas.tbl_aas_claim_summary cs on cs.claim_id=dc.claim_id
# MAGIC left join 
# MAGIC (
# MAGIC select distinct
# MAGIC job_id,
# MAGIC job_sub_type_description
# MAGIC from
# MAGIC hive_metastore.ice_aas.ice_dim_job
# MAGIC where job_sub_type_description is not null and current_flag='Y'
# MAGIC )mil on mil.job_id=i.job_id
# MAGIC
# MAGIC where i.current_flag='Y' 
# MAGIC and di.ledger_type='Sale' 
# MAGIC and di.status='AUTHORISED'
# MAGIC and invoice_line_effective='Y' 
# MAGIC and (dic.invoice_category_description='Outlay' or (dic.invoice_category_description='Recovery') or (dic.invoice_category_description='Tow Outlay') or (dic.invoice_category_description='Member Tow Outlay') or (DIC.Invoice_category_description = 'Tow/Repair Referral Outlay'))
# MAGIC )invoice
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC select *
# MAGIC from
# MAGIC (
# MAGIC select *, ROW_NUMBER() over (partition by claim_id order by recoveryrequesteddateraw) as rowno
# MAGIC
# MAGIC from
# MAGIC (
# MAGIC select distinct
# MAGIC 'AAS' as system,
# MAGIC dc.claim_number,
# MAGIC n.claim_id,
# MAGIC n.negotiation_id as invoice_id,
# MAGIC n.trn_negotiation_key as invoice_line_id,
# MAGIC sc.status_code,
# MAGIC sc.position_code,
# MAGIC DATE_FORMAT(dc.notification_date,'yyyy-MM-dd') as notification_date,
# MAGIC ifnull(dc.tp_insurer,'NA') as tp_insurer,
# MAGIC 'INSURER' as partytype_code,
# MAGIC ifnull(dc.tp_insurer,'NA') as invoice_to,
# MAGIC ifnull(dc.liability_decision,'NA') as liability_decision,
# MAGIC dc.subrogated,
# MAGIC case when po.policy_number is null and dc.claim_number like 'AAS%' and dcp.organisation_name in ('AA Underwriting','AA Underwriting NM','AA Underwriting Smart','AA Silver','AA Gold','AA Platinum') then 'AAUICL'
# MAGIC when po.policy_number is null and dc.claim_number like 'AAS%' and dcp.organisation_name is not null then 'Panel' 
# MAGIC when po.policy_number is null and dc.claim_number like 'MEM%' then 'Member'
# MAGIC when po.policy_number is not null and dc.claim_number like 'AAS%' and po.policy_number like 'AAPMB%' then 'AAUICL'
# MAGIC when po.policy_number is not null and dc.claim_number like 'AAS%' and po.policy_number like 'AAMOBR%' then 'AAUICL'
# MAGIC when po.policy_number is not null and dc.claim_number like 'AAS%' and po.policy_number not like 'AAPMB%' then 'Panel' 
# MAGIC when po.policy_number is not null and dc.claim_number like 'AAS%' and po.policy_number not like 'AAMOBR%' then 'AAUICL'
# MAGIC when po.policy_number is not null and dc.claim_number like 'MEM%' then 'Member'
# MAGIC else null
# MAGIC end as workstream,
# MAGIC 'Negotiation' as outlayrequestedmethod,
# MAGIC n.job_id,
# MAGIC ifnull(job_sub_type_description,'NA') as milestone,
# MAGIC (select party_id from hive_metastore.ice_aas.ice_dim_claim_party dcp 
# MAGIC left join  hive_metastore.ice_aas.ice_trn_claim_party tpc on tpc.claim_party_id = dcp.party_id and tpc.current_flag = 'y'
# MAGIC where dcp.partytype_code = 'INSURER' and dcp.organisation_name = dc.tp_insurer
# MAGIC order by dcp.audit_key desc limit 1
# MAGIC ) as invoice_to_id,
# MAGIC
# MAGIC DATE_FORMAT(n.transaction_date,'yyyy-MM-dd') as recoveryrequesteddate,
# MAGIC n.transaction_date as recoveryrequesteddateraw,
# MAGIC --NULL as invoice_line_transaction_date,
# MAGIC n.demand_amt as recoverydemanded,
# MAGIC 'Negotiation' as invoice_category_description,
# MAGIC u.full_username as outlayrequestedby,
# MAGIC ifnull(dj.serviceprovider_job_comp_type,'NA') as charge_type_description,
# MAGIC 'NEGOTIATION' as invoice_type,
# MAGIC 'Negotiation' as ledger_type,
# MAGIC 'NA' as creditnotereason,
# MAGIC
# MAGIC n.status,
# MAGIC ne.negotiation_type_description as invoice_type_description,
# MAGIC
# MAGIC ifnull((
# MAGIC select
# MAGIC ag.full_username as recoveryhandler
# MAGIC from hive_metastore.ice_aas.ice_trn_claim_party  hp
# MAGIC inner join hive_metastore.ice_aas.ice_dim_user ag on hp.claim_party_id = ag.user_party_id
# MAGIC where handler_type_id = 4 and hp.current_flag = 'y' and ag.current_flag = 'y' and dc.claim_id = claim_id
# MAGIC order by hp.eff_start desc, hp.eff_end desc limit 1
# MAGIC ),'NA')
# MAGIC as recoveryhandler,
# MAGIC 0 as creditnoteser,
# MAGIC 0 as creditnoteaban,
# MAGIC 0 as crediterror,
# MAGIC 0 as creditcleansed,
# MAGIC
# MAGIC 1 as negotiationscreen
# MAGIC
# MAGIC
# MAGIC
# MAGIC from hive_metastore.ice_aas.ice_trn_negotiation n
# MAGIC inner join hive_metastore.ice_aas.ice_dim_claim dc on n.claim_id=dc.claim_id and dc.current_flag='Y'
# MAGIC inner join hive_metastore.ice_aas.ice_trn_claim sc on n.claim_id=sc.claim_id and sc.current_flag='Y'
# MAGIC inner join hive_metastore.ice_aas.ice_dim_user u on n.handler_id=u.user_party_id
# MAGIC left join hive_metastore.ice_aas.ice_dim_negotiation ne on n.negotiation_key=ne.negotiation_key and ne.current_flag='Y'
# MAGIC left join hive_metastore.ice_aas.ice_trn_claim_party tpc on tpc.claim_id=n.claim_id and tpc.current_flag='Y'
# MAGIC left join hive_metastore.ice_aas.ice_dim_claim_party dcp on tpc.claim_party_id=dcp.party_id and dcp.current_flag='Y'
# MAGIC left join hive_metastore.ice_aas.ice_trn_claim tc on tc.claim_id=n.claim_id and tc.CURRENT_FLAG='Y'
# MAGIC left join hive_metastore.ice_aas.ice_dim_policy po on tc.policy_key=po.policy_key and po.current_flag='Y'
# MAGIC left join hive_metastore.ice_aas.ice_trn_job_component dj on dj.job_id=n.job_id and dj.current_flag='Y'
# MAGIC left join 
# MAGIC (
# MAGIC select distinct
# MAGIC job_id,
# MAGIC job_sub_type_description
# MAGIC from
# MAGIC hive_metastore.ice_aas.ice_dim_job
# MAGIC where job_sub_type_description is not null and current_flag='Y'
# MAGIC )mil on mil.job_id=n.job_id
# MAGIC
# MAGIC where demand_amt<>0 
# MAGIC and ne.negotiation_type_code='RECOVERY' 
# MAGIC and n.current_flag='Y' 
# MAGIC and n.status<>'Rejected'
# MAGIC )main
# MAGIC )main2 where rowno=1
# MAGIC
# MAGIC )
