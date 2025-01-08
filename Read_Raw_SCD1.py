# Databricks notebook source

dbutils.widgets.text("TriggerName","Tr_sample_csv")
TriggerName = dbutils.widgets.get("TriggerName")

# COMMAND ----------

# DBTITLE 1,Import Lib


# COMMAND ----------

# DBTITLE 1,Common Functions
# MAGIC %run ./Common_functions

# COMMAND ----------

# DBTITLE 1,Read Config
jdbcUrl,connectionProperties = jdbc_connect_azure_sql()
pushdown_query = f"""(select a.trigger_name, b.*,c.job_dtls_id,c.dtl_key,c.dtl_value 
from dbo.tbl_trigger a 
join dbo.tbl_job b on a.trigger_id = b.trigger_id
join dbo.tbl_job_dtls c on b.jobid=c.jobid
where a.trigger_name = '{TriggerName}'
) test"""
df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
display(df)

# COMMAND ----------

# DBTITLE 1,getconfig
src_acc = df.filter(df.dtl_key == 'TargetAccount').collect()[0]['dtl_value']
src_dir = df.filter(df.dtl_key == 'tgt_dir').collect()[0]['dtl_value']
source_file =df.filter(df.dtl_key == 'Sourcefile').collect()[0]['dtl_value']

# COMMAND ----------

# DBTITLE 1,get config
file_loc=src_acc+src_dir+'/'+source_file
file_loc=file_loc.replace('://','://bronze@').replace('https:','abfss:').replace('.blob.','.dfs.').replace('.csv','.parquet')

# COMMAND ----------

# DBTITLE 1,Read Data
df=spark.read.parquet(file_loc)

# COMMAND ----------

# DBTITLE 1,Display
display(df)

# COMMAND ----------

# DBTITLE 1,Drop Duplicate
df=df.dropDuplicates()

# COMMAND ----------

# DBTITLE 1,add a new column
from  pyspark.sql.functions import input_file_name

df=df.withColumn("filename", input_file_name())

# COMMAND ----------

# DBTITLE 1,de dupe based on OrderID
deduplicated_df = df.dropDuplicates(["OrderID"])

# COMMAND ----------

display(deduplicated_df)

# COMMAND ----------

# DBTITLE 1,Save to a delta table using scd-1
if not spark.catalog.tableExists( "vb_de_training.default.sales_report"):
  print("Table not exists")
  deduplicated_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("vb_de_training.default.sales_report")
else:
  print("Table exist")
  deduplicated_df.createOrReplaceTempView("temp_sales_report")

  merge_query = """
  MERGE INTO vb_de_training.default.sales_report AS target
  USING temp_sales_report AS source
  ON target.OrderID = source.OrderID
  WHEN MATCHED THEN
    UPDATE SET *
  WHEN NOT MATCHED THEN
    INSERT *
  """

  spark.sql(merge_query)


