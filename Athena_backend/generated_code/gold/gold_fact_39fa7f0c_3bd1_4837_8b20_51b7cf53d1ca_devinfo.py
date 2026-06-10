from pyspark.sql import SparkSession
from pyspark.sql.functions import count, current_timestamp

spark = SparkSession.builder.getOrCreate()

SOURCE_TABLE = r"silver.silver_devinfo"
TARGET_TABLE = r"gold.gold_devinfo"

print(f"Loading Silver table {SOURCE_TABLE} for Gold KPI generation")
df = spark.table(SOURCE_TABLE)
if df.rdd.isEmpty():
    raise ValueError(f'Silver source table is empty: {SOURCE_TABLE}')

summary = df.select(`RemoteMessage_Conciliation`, `RemoteMessage_Details__Currency`, `RemoteMessage_Details_countings__valid`, `RemoteMessage_Details_countings_counted`, `RemoteMessage_Details_total`, `RemoteMessage_L2Countings`, `RemoteMessage_L3Countings`, `RemoteMessage_L4bCountings`, `RemoteMessage_TransactionID`, `RemoteMessage_User`, `RemoteMessage_UserAlternateID`, `RemoteMessage_UserLevel`, `RemoteMessage_UserName`, `RemoteMessage__CustomerCode`, `RemoteMessage__Date`, `RemoteMessage__DeviceID`, `RemoteMessage__NOP`, `RemoteMessage__TestMode`, `RemoteMessage__Time`, `RemoteMessage__operation`, `RemoteMessage_accountingDate`, `RemoteMessage_authenticationMode`, `RemoteMessage_cheques`, `RemoteMessage_contentAfter_count`, `RemoteMessage_contentBefore_count`, `RemoteMessage_destDetails_count`, `RemoteMessage_endShiftDeposit`, `RemoteMessage_groupID`, `RemoteMessage_groupName`, `RemoteMessage_isKit`, `RemoteMessage_offlineMachines`, `RemoteMessage_tickets`, `RemoteMessage_BDM_RecFw`, `RemoteMessage_BDM_RecFw__version`, `RemoteMessage_BDM_RecTemplates_Template`, `RemoteMessage_BDM_bagsConfig_bag`, `RemoteMessage_BDM_fw`, `RemoteMessage_BDM_fw__version`, `RemoteMessage_BDM_machineId`, `RemoteMessage_BDM_model`, `RemoteMessage_BDM_model__name`, `RemoteMessage_BDM_model__sn`, `RemoteMessage_BDM_stocksConfig_stock`, `RemoteMessage_BDM_videoPack`, `RemoteMessage_BDM_videoPack__version`, `RemoteMessage_CDM`, `RemoteMessage_DDM`, `RemoteMessage_os`, `RemoteMessage_software_sw`, `RemoteMessage_software_sw__name`, `RemoteMessage_software_sw__version`, `RemoteMessage_sourceDetails_count`)
result = summary.agg(count('*').alias('record_count')).withColumn('gold_generated_at', current_timestamp())
result.write.format('delta').mode('overwrite').saveAsTable(TARGET_TABLE)
print(f"Gold script completed for {TARGET_TABLE}")