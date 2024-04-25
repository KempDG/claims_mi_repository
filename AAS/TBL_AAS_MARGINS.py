# Databricks notebook source
# MAGIC %md
# MAGIC ## ICE_AAS.TBL_AAS_MARGINS

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TEMPORARY VARIABLE IF EXISTS RepairAverageforWIP;
# MAGIC DECLARE VARIABLE RepairAverageforWIP decimal (3,2);
# MAGIC SET VARIABLE RepairAverageforWIP = 
# MAGIC (Select 
# MAGIC sum(RepairMargin)/sum(RepairCost) AS AvgRepairUplift
# MAGIC from hive_metastore.ice_aas.tbl_aas_margins
# MAGIC WHERE RepairStatusSummary = 'Invoiced' and datediff(month,RepairDate,current_date()) <=3 );
# MAGIC
# MAGIC DROP TEMPORARY VARIABLE IF EXISTS HIREAVERAGEFORWIP;
# MAGIC DECLARE VARIABLE HIREAVERAGEFORWIP decimal (3,2);
# MAGIC SET VARIABLE HIREAVERAGEFORWIP = 
# MAGIC (Select 
# MAGIC sum(HireMargin)/sum(HireCost) AS AvgHireUplift
# MAGIC from hive_metastore.ice_aas.tbl_aas_margins
# MAGIC WHERE HireStatusSummary = 'Invoiced' and datediff(month,HireDate,current_date()) <=3 );
# MAGIC
# MAGIC DROP TEMPORARY VARIABLE IF EXISTS TOWAverageforWIP;
# MAGIC DECLARE VARIABLE TOWAverageforWIP decimal (5,2);
# MAGIC SET VARIABLE TOWAverageforWIP = 239.40;
# MAGIC
# MAGIC DROP TEMPORARY VARIABLE IF EXISTS RepairBudgetWIP;
# MAGIC DECLARE VARIABLE RepairBudgetWIP decimal (5,2);
# MAGIC SET VARIABLE RepairBudgetWIP = 850.00;
# MAGIC
# MAGIC DROP TEMPORARY VARIABLE IF EXISTS HireBudgetWIP;
# MAGIC DECLARE VARIABLE HireBudgetWIP decimal (5,2);
# MAGIC SET VARIABLE HireBudgetWIP = 625.00;
# MAGIC
# MAGIC DROP TEMPORARY VARIABLE IF EXISTS TOWBudgetWIP;
# MAGIC DECLARE VARIABLE TOWBudgetWIP decimal (5,2);
# MAGIC SET VARIABLE TOWBudgetWIP = 260.00;
# MAGIC
# MAGIC DROP TABLE IF EXISTS ICE_AAS.TBL_AAS_MARGINS;
# MAGIC
# MAGIC CREATE TABLE ICE_AAS.TBL_AAS_MARGINS AS (
# MAGIC
# MAGIC
# MAGIC SELECT
# MAGIC DISTINCT
# MAGIC CLAIM_Number,
# MAGIC position_code,
# MAGIC Status_code,
# MAGIC liability_decision,
# MAGIC Subrogated,
# MAGIC Workstream,
# MAGIC RepairsCompleted,
# MAGIC RepairPurchaseInvoicesRaised,
# MAGIC RepairPurchaseTotalInvoiceAmount,
# MAGIC RepairPurchaseLastInvoiceDate,
# MAGIC RepairSalesInvoicesRaised,
# MAGIC TotalRepairInvoiceRaisedAmount,
# MAGIC LastRepairInvoiceRaisedDate,
# MAGIC
# MAGIC CASE WHEN Status_code = 'Closed' AND TotalRepairInvoiceRaisedAmount > RepairCollected THEN TotalRepairInvoiceRaisedAmount - RepairCollected ELSE NULL END AS RepairMarginAbandoned,
# MAGIC
# MAGIC RepairAverageforWIP AS AppliedWIPRepairMargin,
# MAGIC
# MAGIC
# MAGIC CASE WHEN RepairCollectedUnits > 0 THEN 'Collected'
# MAGIC 	 WHEN RepairInvoicedUnits > 0 THEN 'Invoiced'
# MAGIC 	 WHEN RepairOutstandingUnits > 0 THEN 'WIP'
# MAGIC 	 WHEN RepairPurchaseInvoicesRaised > 0 AND RepairCost > 0 THEN 'WIP'
# MAGIC 	 WHEN RepairWIPEstimatedUnits > 0 AND RepairPurchaseInvoicesRaised > 0 AND RepairsCompleted = 'Repairs Complete' THEN 'WIP'
# MAGIC 	 WHEN RepairWIPEstimatedUnits > 0 AND RepairPurchaseInvoicesRaised = 0 AND RepairsCompleted = 'Repairs Complete' THEN 'WIP'
# MAGIC 	 WHEN RepairWIPEstimatedUnits > 0 AND RepairPurchaseInvoicesRaised = 0 AND RepairsCompleted is null THEN 'WIP'
# MAGIC 	 WHEN RepairWIPDeployedUnits > 0 THEN 'WIP'
# MAGIC 	 ELSE 'OTHER' END AS RepairStatusSummary,
# MAGIC
# MAGIC        CASE WHEN RepairCollectedUnits > 0 THEN 'Payment Received'
# MAGIC      WHEN RepairOutstandingUnits > 0 THEN 'Sales Invoice Outstanding'
# MAGIC      WHEN RepairInvoicedUnits > 0 THEN 'Sales Invoice Sent'
# MAGIC 	 
# MAGIC 	 WHEN RepairPurchaseInvoicesRaised > 0 AND RepairCost > 0 THEN 'Purchase Invoice Received (Awaiting Sales Invoice)'
# MAGIC 	 WHEN RepairWIPEstimatedUnits > 0 AND RepairPurchaseInvoicesRaised > 0 AND RepairsCompleted = 'Repairs Complete' THEN 'Job Complete - Awaiting Payment'
# MAGIC 	 WHEN RepairWIPEstimatedUnits > 0 AND RepairPurchaseInvoicesRaised = 0 AND RepairsCompleted = 'Repairs Complete' THEN 'Job Complete - Awaiting Purchase Invoice'
# MAGIC 	 WHEN RepairWIPEstimatedUnits > 0 AND RepairPurchaseInvoicesRaised = 0 AND RepairsCompleted is null THEN 'Estimate Received'
# MAGIC 	 WHEN RepairWIPDeployedUnits > 0 THEN 'Deployed'
# MAGIC 	 ELSE 'OTHER' END AS RepairStatus, 
# MAGIC
# MAGIC --REPAIR INVOICE DATES 
# MAGIC CASE WHEN RepairOutstandingUnits > 0 THEN RepairOutstandingDate 
# MAGIC 	 WHEN RepairWIPEstimatedUnits > 0 THEN Notification_Date
# MAGIC 	 WHEN RepairWIPDeployedUnits > 0 THEN Notification_Date
# MAGIC 	 WHEN RepairCostUnits > 0 AND RepairPurchaseInvoicesRaised > 0 AND RepairOutstandingUnits = 0 AND RepairInvoicedUnits = 0 THEN Notification_Date
# MAGIC 	 ELSE RepairInvoicedDate END AS RepairDate,
# MAGIC
# MAGIC CASE WHEN RepairWIPEstimatedUnits > 0 THEN RepairWIPEstimated * RepairAverageforWIP
# MAGIC 	 WHEN RepairWIPDeployedUnits > 0 THEN RepairWIPDeployed  * RepairAverageforWIP
# MAGIC 	 WHEN RepairCostUnits > 0 AND RepairOutstandingUnits = 0 AND RepairInvoicedUnits = 0 AND RepairCollectedUnits = 0 THEN RepairCost * RepairAverageForWIP
# MAGIC ELSE
# MAGIC RepairCollected+RepairInvoiced+RepairOutstanding - RepairCost END AS RepairMargin,
# MAGIC
# MAGIC
# MAGIC RepairCollected,
# MAGIC RepairInvoiced,
# MAGIC RepairOutstanding,
# MAGIC CASE WHEN RepairCost = 0 THEN RepairWIPEstimated + RepairWIPDeployed ELSE RepairCost END AS RepairCost,
# MAGIC RepairWIPEstimated,
# MAGIC RepairWIPDeployed,
# MAGIC
# MAGIC RepairCostUnits,
# MAGIC RepairOutstandingUnits,
# MAGIC RepairInvoicedUnits,
# MAGIC RepairCollectedUnits,
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC CASE WHEN Status_code = 'Closed' AND HireInvoiced > HireCollected THEN HireInvoiced - HireCollected ELSE NULL END AS HireMarginAbandoned,
# MAGIC
# MAGIC HireAverageforWIP AS AppliedWIPHireMargin,
# MAGIC
# MAGIC CASE WHEN HireCollectedUnits > 0 THEN 'Collected'
# MAGIC      WHEN HireInvoicedUnits > 0 THEN 'Invoiced'
# MAGIC      WHEN HireOutstandingUnits > 0 THEN 'WIP'
# MAGIC 	 WHEN HireWIPUnits > 0 THEN 'WIP'
# MAGIC 	 ELSE 'OTHER' END AS HireStatusSummary, 
# MAGIC
# MAGIC
# MAGIC CASE WHEN HireCollectedUnits > 0 THEN 'Payment Received'
# MAGIC      WHEN HireOutstandingUnits > 0 THEN 'Sales Invoice Outstanding'
# MAGIC      WHEN HireInvoicedUnits > 0 THEN 'Sales Invoiced Sent'
# MAGIC 	 WHEN HireWIPUnits > 0 THEN 'Deployed'
# MAGIC 	 ELSE 'OTHER' END AS HireStatus, 
# MAGIC
# MAGIC --HIRE INVOICE DATES 
# MAGIC CASE WHEN HireOutstandingUnits > 0 THEN HireOutstandingDate 
# MAGIC 	 WHEN HireWIPUnits > 0 THEN Notification_Date
# MAGIC 	 ELSE HireInvoicedDate END AS HireDate,
# MAGIC
# MAGIC CASE WHEN HireWIPUnits > 0 THEN HireWIP * HireAverageforWIP
# MAGIC ELSE
# MAGIC HireCollected+HireInvoiced+HireOutstanding - HireCost END AS HireMargin,
# MAGIC
# MAGIC
# MAGIC HireCollected,
# MAGIC HireInvoiced,
# MAGIC HireOutstanding,
# MAGIC CASE WHEN HireCost = 0 THEN HireWIP ELSE HireCost END AS HireCost,
# MAGIC
# MAGIC HireWIP,
# MAGIC
# MAGIC TowCollectedUnits,
# MAGIC TowInvoiceRaisedUnits,
# MAGIC TowOutstandingUnits,
# MAGIC TowPurchaseInvoiceUnits,
# MAGIC TowCompletedUnits,
# MAGIC TowWIPDeployedUnits,
# MAGIC
# MAGIC
# MAGIC
# MAGIC CASE WHEN TowCollectedUnits > 0 THEN 'Collected'
# MAGIC      WHEN TowInvoiceRaisedUnits > 0 THEN 'Invoiced'
# MAGIC 	 WHEN TowOutstandingUnits > 0 THEN 'WIP'
# MAGIC 	 WHEN TowPurchaseInvoiceUnits > 0 THEN 'WIP'
# MAGIC 	 WHEN TowCompletedUnits > 0 THEN 'WIP'
# MAGIC      WHEN TowWIPDeployedUnits > 0 THEN 'WIP'
# MAGIC 	 ELSE 'OTHER' END AS TowStatusSummary,
# MAGIC
# MAGIC CASE WHEN TowCollectedUnits > 0 THEN 'Collected'
# MAGIC      WHEN TowInvoiceRaisedUnits > 0 THEN 'Sales Invoice Raised'
# MAGIC 	 WHEN TowOutstandingUnits > 0 THEN 'Sales Invoice Outstanding'
# MAGIC 	 WHEN TowPurchaseInvoiceUnits > 0 THEN 'Purchase Invoice Received (Awaiting Sales Invoice)'
# MAGIC 	 WHEN TowCompletedUnits > 0 THEN 'Job Complete - Awaiting Purchase Invoice'
# MAGIC      WHEN TowWIPDeployedUnits > 0 THEN 'Deployed'
# MAGIC 	 ELSE 'OTHER' END AS TowStatus, 
# MAGIC
# MAGIC
# MAGIC
# MAGIC CASE 
# MAGIC 	 WHEN TowCompletedUnits > 0 THEN Notification_Date
# MAGIC      WHEN TowWIPDeployedUnits > 0 THEN Notification_Date
# MAGIC 	 WHEN TowPurchaseInvoiceUnits > 0 THEN TowPurchaseLastInvoiceDate
# MAGIC 	 ELSE TowInvoicedDate
# MAGIC END AS TowDate,
# MAGIC
# MAGIC TowAverageForWIP AS AppliedWIPTowMargin,
# MAGIC
# MAGIC CASE 
# MAGIC 	 WHEN TowWIPDeployedUnits > 0 AND TowOutstandingUnits = 0 AND TowInvoiceRaisedUnits = 0 AND TowCollectedUnits = 0 THEN TowAverageForWIP
# MAGIC 	 WHEN TowCompletedUnits > 0 AND TowOutstandingUnits = 0 AND TowInvoiceRaisedUnits = 0 AND TowCollectedUnits = 0 THEN TowAverageForWIP
# MAGIC 	 WHEN TowPurchaseInvoiceUnits > 0  AND TowOutstandingUnits = 0 AND TowInvoiceRaisedUnits = 0 AND TowCollectedUnits = 0 THEN TowAverageForWIP
# MAGIC ELSE TowCollected+TowOutstanding - TowCost
# MAGIC END  AS TowMargin,
# MAGIC
# MAGIC TowCollected,
# MAGIC TowOutstanding,
# MAGIC TowCost,
# MAGIC TowPurchaseInvoicesRaised,
# MAGIC TowPurchaseTotalInvoiceAmount,
# MAGIC TowPurchaseLastInvoiceDate
# MAGIC
# MAGIC
# MAGIC from
# MAGIC (
# MAGIC select 
# MAGIC
# MAGIC A.CLAIM_NUMBER,
# MAGIC a.position_code,
# MAGIC A.status_code,
# MAGIC A.liability_decision,
# MAGIC A.Subrogated,
# MAGIC A.Notification_date,
# MAGIC Workstream,
# MAGIC
# MAGIC RepairPurchaseInvoicesRaised,
# MAGIC RepairPurchaseTotalInvoiceAmount,
# MAGIC RepairPurchaseLastInvoiceDate,
# MAGIC
# MAGIC RepairSalesInvoicesRaised,
# MAGIC TotalRepairInvoiceRaisedAmount,
# MAGIC LastRepairInvoiceRaisedDate,
# MAGIC
# MAGIC --REPAIR UNITS
# MAGIC CASE WHEN PPRepair > 0 
# MAGIC --AND RepairSalesInvoicesRaised >=1  
# MAGIC AND RPRepair > 0 then 1 else 0 END AS RepairCollectedUnits,
# MAGIC CASE WHEN PPRepair > 0 AND RepairSalesInvoicesOutstanding >=1 then 1 else 0 END AS RepairOutstandingUnits,
# MAGIC CASE WHEN PPRepair > 0 AND RepairSalesInvoicesRaised >=1 AND RRRepair > 0 then 1 else 0 END AS RepairInvoicedUnits,
# MAGIC CASE WHEN PPRepair > 0 AND RepairPurchaseInvoicesRaised >=1 then 1 else 0 END AS RepairCostUnits,
# MAGIC CASE WHEN PPREPAIR = 0 AND PRREPAIR > 0 AND PRREPAIR <> 1300.81 and subrogated = 1 then 1 else 0 end as RepairWIPEstimatedUnits,
# MAGIC CASE WHEN PPREPAIR = 0 AND PRREPAIR > 0 AND PRREPAIR = 1300.81 and subrogated = 1 then 1 else 0 end as RepairWIPDeployedUnits,
# MAGIC
# MAGIC --REPAIR MARGINS
# MAGIC CASE WHEN PPRepair > 0 
# MAGIC --AND (RepairSalesInvoicesRaised + RepairSalesInvoicesOutstanding) >=1 
# MAGIC then RPRepair else 0 END AS RepairCollected,
# MAGIC CASE WHEN PPRepair > 0 AND RepairSalesInvoicesOutstanding >=1 then RRRepair else 0 END AS RepairOutstanding,
# MAGIC CASE WHEN PPRepair > 0 AND RepairSalesInvoicesRaised >=1 then RRRepair else 0 END AS RepairInvoiced,
# MAGIC CASE WHEN PPRepair > 0 
# MAGIC --AND (RepairSalesInvoicesRaised + RepairSalesInvoicesOutstanding)  >=1
# MAGIC  then PPRepair else 0 END AS RepairCost,
# MAGIC CASE WHEN PPREPAIR = 0 AND PRREPAIR > 0 AND PRREPAIR <> 1300.81 and subrogated = 1 then PRREPAIR else 0 end as RepairWIPEstimated,
# MAGIC CASE WHEN PPREPAIR = 0 AND PRREPAIR > 0 AND PRREPAIR = 1300.81 and subrogated = 1 then PRREPAIR else 0 end as RepairWIPDeployed,
# MAGIC
# MAGIC --REPAIR INVOICE DATES 
# MAGIC CASE WHEN PPRepair > 0 AND (RepairSalesInvoicesRaised + RepairSalesInvoicesOutstanding) >=1 then LastRepairInvoiceRaisedDate else NULL END AS RepairInvoicedDate,
# MAGIC CASE WHEN PPRepair > 0 AND RepairSalesInvoicesOutstanding >=1 then LastRepairInvoiceOutstandingDate else null END AS RepairOutstandingDate,
# MAGIC
# MAGIC --Hire UNITS
# MAGIC CASE WHEN PPHire > 0 AND HireSalesInvoicesRaised >=1  AND RPHire > 0 then 1 else 0 END AS HireCollectedUnits,
# MAGIC CASE WHEN PPHire > 0 AND HireSalesInvoicesOutstanding >=1 then 1 else 0 END AS HireOutstandingUnits,
# MAGIC CASE WHEN PPHire > 0 AND HireSalesInvoicesRaised >=1 AND RRHire > 0 then 1 else 0 END AS HireInvoicedUnits,
# MAGIC CASE WHEN PPHire > 0 AND (HireSalesInvoicesRaised + HireSalesInvoicesOutstanding)  >=1 AND PPHire > 0 then 1 else 0 END AS HireCostUnits,
# MAGIC CASE WHEN PPHire = 0 AND PRHire > 0  then 1 else 0 end as HireWIPUnits,
# MAGIC
# MAGIC
# MAGIC --HIRE MARGINS
# MAGIC CASE WHEN PPHIRE > 0 AND HireSalesInvoicesRaised >=1 then RPHIRE else 0 END AS HireCollected,
# MAGIC CASE WHEN PPHIRE > 0 AND HireSalesInvoicesOutstanding >=1 then RRHIRE else 0 END AS HireOutstanding,
# MAGIC CASE WHEN PPHIRE > 0 AND HireSalesInvoicesRaised >=1 then RRHIRE else 0 END AS HireInvoiced,
# MAGIC CASE WHEN PPHIRE > 0 AND (HireSalesInvoicesRaised + HireSalesInvoicesOutstanding) >=1 then PPHIRE else 0 END AS HireCost,
# MAGIC CASE WHEN PPHire = 0 AND PRHire > 0  then PRHire else 0 end as HireWIP,
# MAGIC
# MAGIC --Hire INVOICE DATES 
# MAGIC CASE WHEN PPHire > 0 AND (HireSalesInvoicesRaised + HireSalesInvoicesOutstanding) >=1 then LastHireInvoiceRaisedDate else NULL END AS HireInvoicedDate,
# MAGIC CASE WHEN PPHire > 0 AND HireSalesInvoicesOutstanding >=1 then LastHireInvoiceOutstandingDate else null END AS HireOutstandingDate,
# MAGIC
# MAGIC CASE WHEN TowSalesInvoicesRaised >=1 AND RRTow > 0 then 1 else 0 END AS TowInvoicedUnits,
# MAGIC CASE WHEN TowSalesInvoicesRaised >=1 AND PPTow > 0 then 1 else 0 END AS TowCostUnits,
# MAGIC
# MAGIC --TOW MARGINS
# MAGIC RPTOW AS TowCollected,
# MAGIC RRTOW AS TowOutstanding,
# MAGIC PPTOW AS TowCost,
# MAGIC
# MAGIC TowPurchaseInvoicesRaised,
# MAGIC TowPurchaseTotalInvoiceAmount,
# MAGIC TowPurchaseLastInvoiceDate,
# MAGIC
# MAGIC
# MAGIC --TOW INVOICE DATES 
# MAGIC CASE WHEN PPTow > 0 AND TowSalesInvoicesRaised >=1 then LastTowInvoiceRaisedDate else NULL END AS TowInvoicedDate,
# MAGIC LastTowInvoiceRaisedDate,
# MAGIC RepairsCompleted,
# MAGIC
# MAGIC --REPAIR UNITS
# MAGIC CASE when RPTow > 0 then 1 else 0 END AS TowCollectedUnits,
# MAGIC CASE WHEN TowSalesInvoicesRaised >=1 then 1 else 0 END AS TowInvoiceRaisedUnits,
# MAGIC CASE WHEN TowSalesInvoicesOutstanding >=1 then 1 else 0 END AS TowOutstandingUnits,
# MAGIC CASE WHEN Towstatus = 'Completed' and TowPurchaseInvoicesRaised >=1 then 1 else 0 end as TowPurchaseInvoiceUnits,
# MAGIC CASE WHEN Towstatus = 'Completed' and TowPurchaseInvoicesRaised = 0 AND PPTOW = 0  then 1 else 0 end as TowCompletedUnits,
# MAGIC CASE WHEN towstatus = 'Deployed' AND PPTOW = 0 then 1 else 0 end as TowWIPDeployedUnits
# MAGIC
# MAGIC from
# MAGIC (
# MAGIC select 
# MAGIC
# MAGIC F.*,
# MAGIC
# MAGIC ifnull(SALESINVOICE.SalesInvoicesRaised,0) AS SalesInvoicesRaised,
# MAGIC ifnull(SALESINVOICE.TotalInvoiceAmount,0) AS TotalInvoiceAmount,
# MAGIC SALESINVOICE.LastInvoiceDate AS LastInvoiceDate,
# MAGIC
# MAGIC --REPAIR INVOICES
# MAGIC
# MAGIC ifnull(REPAIRPURCHASEINVOICE.PurchaseInvoicesRaised,0) AS RepairPurchaseInvoicesRaised,
# MAGIC ifnull(REPAIRPURCHASEINVOICE.TotalInvoiceAmount,0) AS RepairPurchaseTotalInvoiceAmount,
# MAGIC REPAIRPURCHASEINVOICE.LastInvoiceDate AS RepairPurchaseLastInvoiceDate,
# MAGIC
# MAGIC
# MAGIC ifnull(RepairSALESINVOICEOUTSTANDING.RepairSalesInvoicesOutstanding,0) AS RepairSalesInvoicesOutstanding,
# MAGIC ifnull(RepairSALESINVOICEOUTSTANDING.TotalRepairInvoiceOutstandingAmount,0) AS TotalRepairInvoiceOutstandingAmount,
# MAGIC RepairSALESINVOICEOUTSTANDING.LastRepairInvoiceOutstandingDate AS LastRepairInvoiceOutstandingDate,
# MAGIC
# MAGIC ifnull(CASE WHEN RepairSALESINVOICERAISED.RepairSalesInvoicesRaised is null then RepairSALESINVOICERAISEDOUTLAY.RepairSalesInvoicesRaised else RepairSALESINVOICERAISED.RepairSalesInvoicesRaised end,0) AS RepairSalesInvoicesRaised,
# MAGIC ifnull(CASE WHEN RepairSALESINVOICERAISED.TotalRepairInvoiceRaisedAmount is null then RepairSALESINVOICERAISEDOUTLAY.TotalRepairInvoiceRaisedAmount else RepairSALESINVOICERAISED.TotalRepairInvoiceRaisedAmount end,0) AS TotalRepairInvoiceRaisedAmount,
# MAGIC case when RepairSALESINVOICERAISED.LastRepairInvoiceRaisedDate is null then RepairSALESINVOICERAISEDOUTLAY.LastRepairInvoiceRaisedDate else RepairSALESINVOICERAISED.LastRepairInvoiceRaisedDate end AS LastRepairInvoiceRaisedDate,
# MAGIC
# MAGIC --HIRE INVOICES
# MAGIC ifnull(HireSalesInvoicesOutstanding,0) AS HireSalesInvoicesOutstanding,
# MAGIC ifnull(TotalHireInvoiceOutstandingAmount,0) AS TotalHireInvoiceOutstandingAmount,
# MAGIC LastHireInvoiceOutstandingDate AS LastHireInvoiceOutstandingDate ,
# MAGIC
# MAGIC ifnull(HireSalesInvoicesRaised,0) AS HireSalesInvoicesRaised,
# MAGIC ifnull(TotalHireInvoiceRaisedAmount,0) AS TotalHireInvoiceRaisedAmount,
# MAGIC LastHireInvoiceRaisedDate AS LastHireInvoiceRaisedDate,
# MAGIC
# MAGIC --TOW INVOICES
# MAGIC
# MAGIC ifnull(TowPURCHASEINVOICE.PurchaseInvoicesRaised,0) AS TowPurchaseInvoicesRaised,
# MAGIC ifnull(TowPURCHASEINVOICE.TotalInvoiceAmount,0) AS TowPurchaseTotalInvoiceAmount,
# MAGIC TowPURCHASEINVOICE.LastInvoiceDate AS TowPurchaseLastInvoiceDate,
# MAGIC
# MAGIC
# MAGIC ifnull(TowSalesInvoicesRaised,0) AS TowSalesInvoicesRaised,
# MAGIC ifnull(TotalTowSalesInvoiceRaisedAmount,0) AS TotalTowSalesInvoiceRaisedAmount,
# MAGIC ifnull(TowSalesInvoicesOutstanding,0) AS TowSalesInvoicesOutstanding,
# MAGIC ifnull(TotalTowSalesInvoiceOutstandingAmount,0) AS TotalTowSalesInvoiceOutstandingAmount,
# MAGIC LastTowInvoiceRaisedDate  AS LastTowInvoiceRaisedDate,
# MAGIC
# MAGIC Towstatus.towstatus,
# MAGIC
# MAGIC CASE WHEN RepairComplete.claim_id is not null then 'Repairs Complete' ELSE NULL END AS RepairsCompleted
# MAGIC
# MAGIC from 
# MAGIC
# MAGIC hive_metastore.ice_aas.tbl_aas_financials F
# MAGIC
# MAGIC LEFT JOIN 
# MAGIC (
# MAGIC select 
# MAGIC claim_id,
# MAGIC COUNT(distinct(invoice_number)) AS PurchaseInvoicesRaised,
# MAGIC SUM(GrossAmount) AS TotalInvoiceAmount,
# MAGIC MAX(Invoice_Date) AS LastInvoiceDate
# MAGIC from
# MAGIC hive_metastore.ice_aas.tbl_aas_invoices
# MAGIC WHERE Ledger_type = 'Purchase'  and status <> 'Rejected'  and ifnull(invoice_category_description,'Repair Cost') = 'Repair Cost'
# MAGIC GROUP BY CLAIM_ID
# MAGIC ) REPAIRPURCHASEINVOICE ON F.CLAIM_ID = REPAIRPURCHASEINVOICE.CLAIM_ID
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC LEFT JOIN 
# MAGIC (
# MAGIC select 
# MAGIC claim_id,
# MAGIC COUNT(distinct(invoice_number)) AS PurchaseInvoicesRaised,
# MAGIC SUM(GrossAmount) AS TotalInvoiceAmount,
# MAGIC MAX(Invoice_Date) AS LastInvoiceDate
# MAGIC from
# MAGIC hive_metastore.ice_aas.tbl_aas_invoices
# MAGIC WHERE Ledger_type = 'Purchase'  and status <> 'Rejected'  and invoice_category_description in ('Service Provision Fee','Recovery','Tow Outlay') --and claim_id = 33310
# MAGIC GROUP BY CLAIM_ID
# MAGIC ) TOWPURCHASEINVOICE ON F.CLAIM_ID = TOWPURCHASEINVOICE.CLAIM_ID
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC LEFT JOIN 
# MAGIC
# MAGIC (
# MAGIC select 
# MAGIC
# MAGIC claim_id,
# MAGIC COUNT(distinct(invoice_number)) AS SalesInvoicesRaised,
# MAGIC SUM(GrossAmount) AS TotalInvoiceAmount,
# MAGIC MAX(Invoice_Date) AS LastInvoiceDate
# MAGIC from
# MAGIC hive_metastore.ice_aas.tbl_aas_invoices
# MAGIC WHERE Ledger_type = 'Sale'  and status <> 'Rejected'
# MAGIC GROUP BY CLAIM_ID
# MAGIC ) SALESINVOICE ON F.CLAIM_ID = SALESINVOICE.CLAIM_ID
# MAGIC
# MAGIC --REPAIR INVOICES
# MAGIC
# MAGIC LEFT JOIN 
# MAGIC
# MAGIC (
# MAGIC select 
# MAGIC
# MAGIC claim_id,
# MAGIC COUNT(distinct(invoice_number)) AS RepairSalesInvoicesOutstanding,
# MAGIC SUM(GrossAmount) AS TotalRepairInvoiceOutstandingAmount,
# MAGIC MAX(Invoice_Date) AS LastRepairInvoiceOutstandingDate
# MAGIC
# MAGIC from
# MAGIC hive_metastore.ice_aas.tbl_aas_invoices
# MAGIC WHERE Ledger_type = 'Sale' 
# MAGIC and ifnull(invoice_category_description,'Repair Cost') = 'Repair Cost'   and status in ('Active') 
# MAGIC GROUP BY CLAIM_ID
# MAGIC ) REPAIRSALESINVOICEOUTSTANDING ON F.CLAIM_ID = RepairSALESINVOICEOUTSTANDING.CLAIM_ID
# MAGIC
# MAGIC
# MAGIC
# MAGIC LEFT JOIN 
# MAGIC (
# MAGIC select 
# MAGIC claim_id,
# MAGIC COUNT(distinct(invoice_number)) AS RepairSalesInvoicesRaised,
# MAGIC SUM(RepairCost) AS TotalRepairInvoiceRaisedAmount,
# MAGIC max(Invoice_Date) AS LastRepairInvoiceRaisedDate
# MAGIC
# MAGIC from
# MAGIC hive_metastore.ice_aas.tbl_aas_invoices
# MAGIC WHERE Ledger_type = 'Sale' and ifnull(invoice_category_description,'Repair Cost') in ('Repair Cost','Outlay')     and status in ('Authorised')
# MAGIC GROUP BY CLAIM_ID
# MAGIC
# MAGIC ) REPAIRSALESINVOICERAISED ON F.CLAIM_ID = RepairSALESINVOICERAISED.CLAIM_ID
# MAGIC
# MAGIC
# MAGIC LEFT JOIN 
# MAGIC (
# MAGIC select 
# MAGIC claim_id,
# MAGIC COUNT(distinct(claim_id)) AS RepairSalesInvoicesRaised,
# MAGIC SUM(RecoveryDemanded) AS TotalRepairInvoiceRaisedAmount,
# MAGIC MAX(RecoveryRequestedDate) AS LastRepairInvoiceRaisedDate
# MAGIC from
# MAGIC hive_metastore.ice_aas.tbl_aas_recovery_basic
# MAGIC WHERE invoice_category_description = 'Outlay' and Charge_Type_Description = 'Repair' AND invoicetype = 'Invoice'
# MAGIC GROUP BY CLAIM_ID
# MAGIC
# MAGIC ) REPAIRSALESINVOICERAISEDOUTLAY ON F.CLAIM_ID = RepairSALESINVOICERAISEDOutlay.CLAIM_ID
# MAGIC
# MAGIC
# MAGIC --HIRE INVOICES
# MAGIC LEFT JOIN 
# MAGIC
# MAGIC (
# MAGIC select 
# MAGIC
# MAGIC claim_id,
# MAGIC COUNT(distinct(invoice_number)) AS HireSalesInvoicesOutstanding,
# MAGIC SUM(GrossAmount) AS TotalHireInvoiceOutstandingAmount,
# MAGIC MAX(Invoice_Date) AS LastHireInvoiceOutstandingDate
# MAGIC from
# MAGIC hive_metastore.ice_aas.tbl_aas_invoices
# MAGIC WHERE Ledger_type = 'Sale'and invoice_category_description = 'Hire Cost'   and status in ('Active') 
# MAGIC GROUP BY CLAIM_ID
# MAGIC ) HIRESALESINVOICEOUTSTANDING ON F.CLAIM_ID = HIRESALESINVOICEOUTSTANDING.CLAIM_ID
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC LEFT JOIN 
# MAGIC
# MAGIC (
# MAGIC select 
# MAGIC
# MAGIC claim_id,
# MAGIC COUNT(distinct(invoice_number)) AS HireSalesInvoicesRaised,
# MAGIC SUM(GrossAmount) AS TotalHireInvoiceRaisedAmount,
# MAGIC MAX(Invoice_Date) AS LastHireInvoiceRaisedDate
# MAGIC from
# MAGIC hive_metastore.ice_aas.tbl_aas_invoices
# MAGIC WHERE Ledger_type = 'Sale'and invoice_category_description = 'Hire Cost'   and status in ('Authorised') 
# MAGIC GROUP BY CLAIM_ID
# MAGIC ) HIRESALESINVOICERAISED ON F.CLAIM_ID = HIRESALESINVOICERAISED.CLAIM_ID
# MAGIC
# MAGIC
# MAGIC --TOW INVOICES
# MAGIC LEFT JOIN 
# MAGIC
# MAGIC (
# MAGIC select 
# MAGIC
# MAGIC claim_id,
# MAGIC COUNT(invoice_number) AS TowSalesInvoicesOutstanding,
# MAGIC SUM(GrossAmount) AS TotalTowSalesInvoiceOutstandingAmount,
# MAGIC MAX(Invoice_Date) AS LastTowInvoiceDate
# MAGIC from
# MAGIC hive_metastore.ice_aas.tbl_aas_invoices
# MAGIC WHERE Ledger_type = 'Sale'and invoice_category_description in ('Service Provision Fee','Recovery','Tow Outlay')   and status in ('Active')  
# MAGIC GROUP BY CLAIM_ID
# MAGIC ) TOWSALESINVOICEOUTSTANDING ON F.CLAIM_ID = TOWSALESINVOICEOUTSTANDING.CLAIM_ID
# MAGIC
# MAGIC LEFT JOIN 
# MAGIC
# MAGIC (
# MAGIC select 
# MAGIC
# MAGIC claim_id,
# MAGIC COUNT(invoice_number) AS TowSalesInvoicesRaised,
# MAGIC SUM(GrossAmount) AS TotalTowSalesInvoiceRaisedAmount,
# MAGIC MAX(Invoice_Date) AS LastTowInvoiceRaisedDate
# MAGIC from
# MAGIC hive_metastore.ice_aas.tbl_aas_invoices
# MAGIC WHERE Ledger_type = 'Sale'and invoice_category_description in ('Service Provision Fee','Recovery','Tow Outlay')   and status in ('Authorised') 
# MAGIC GROUP BY CLAIM_ID
# MAGIC ) TOWSALESINVOICERAISED ON F.CLAIM_ID = TOWSALESINVOICERAISED.CLAIM_ID
# MAGIC
# MAGIC
# MAGIC
# MAGIC LEFT JOIN 
# MAGIC (
# MAGIC SELECT 
# MAGIC claim_id 
# MAGIC FROM hive_metastore.ice_aas.tbl_aas_deployments
# MAGIC WHERE CURRENT_FLAG = 'Y' 
# MAGIC AND   job_component_status_description = 'Complete'  
# MAGIC AND JobCompType = 'Motor Repair'  
# MAGIC AND PARTY_TYPE = 'First Party'
# MAGIC ) RepairComplete ON F.CLAIM_ID = REPAIRCOMPLETE.CLAIM_ID
# MAGIC
# MAGIC
# MAGIC LEFT JOIN 
# MAGIC (
# MAGIC SELECT 
# MAGIC row_number() OVER (PARTITION BY CLAIM_ID ORDER BY transactiondate desc) AS RowNumber
# MAGIC ,claim_id
# MAGIC ,transactiondate
# MAGIC       ,CASE WHEN job_component_status_description = 'Accepted' and status_code in ('OPEN','REOPENED') THEN 'Deployed'
# MAGIC 	       WHEN job_component_status_description = 'Complete' THEN 'Completed'
# MAGIC 	   ELSE NULL END AS TowStatus
# MAGIC       ,deployed_date
# MAGIC       ,completed_date
# MAGIC      
# MAGIC FROM hive_metastore.ice_aas.tbl_aas_deployments
# MAGIC WHERE JobCompType = 'Recovery Agents'  AND CURRENT_FLAG = 'Y'  AND job_component_status_description in ('Accepted','Complete') and job_component_id not in ( '34751','38103') 
# MAGIC ) TowStatus ON F.CLAIM_ID = TowStatus.CLAIM_ID  AND ROWNumber = 1
# MAGIC
# MAGIC )A
# MAGIC )B
# MAGIC
# MAGIC )
# MAGIC
# MAGIC
