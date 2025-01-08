# Databricks notebook source

dbutils.widgets.text("TriggerName","Tr_sample_excel")
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
file_loc=file_loc.replace('://','://bronze@').replace('https:','abfss:').replace('.blob.','.dfs.').replace('.xlsx','.xlsx.parquet')

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
from  pyspark.sql.functions import *
from pyspark.sql.types import TimestampType

df=df.withColumn("filename", input_file_name()).withColumn("is_active",lit(1)).withColumn("effective_start",current_timestamp()).withColumn('effective_end', lit(None).cast(TimestampType()))


# COMMAND ----------

display(df)

# COMMAND ----------

# DBTITLE 1,de dupe based on OrderID
deduplicated_df = df.dropDuplicates(["Sku"])

# COMMAND ----------

display(deduplicated_df)

# COMMAND ----------

# DBTITLE 1,Save to a delta table using scd-2
if not spark.catalog.tableExists( "vb_de_training.default.product_details"):
  print("Table not exists")
  deduplicated_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("vb_de_training.default.product_details")
else:
  # print("Table exist")
  deduplicated_df.createOrReplaceTempView("temp_product_details")

  merge_query = """
  MERGE INTO vb_de_training.default.product_details AS c USING (
  SELECT    -- UPDATES
    ur.Sku as merge_key,
    ur.*
  FROM
    temp_product_details ur
  UNION ALL
  SELECT    -- INSERTS
    NULL as merge_key,
    ur.*
  FROM
    temp_product_details ur
    JOIN vb_de_training.default.product_details c ON c.Sku = ur.Sku
    AND c.is_active = 1
  WHERE -- ignore records with no changes
       c.Style_Id<>ur.Style_Id
      or c.Catalog<>ur.Catalog
      or c.Category<>ur.Category
      or c.Weight<>ur.Weight
      or c.TP_1<>ur.TP_1
      or c.TP_2<>ur.TP_2
      or c.MRP_Old<>ur.MRP_Old
      or c.Final_MRP_Old<>ur.Final_MRP_Old
      or c.Ajio_MRP<>ur.Ajio_MRP
      or c.Amazon_MRP<>ur.Amazon_MRP
      or c.Amazon_FBA_MRP<>ur.Amazon_FBA_MRP
      or c.Flipkart_MRP<>ur.Flipkart_MRP
      or c.Limeroad_MRP<>ur.Limeroad_MRP
      or c.Myntra_MRP<>ur.Myntra_MRP
      or c.Paytm_MRP<>ur.Paytm_MRP
      or c.Snapdeal_MRP<>ur.Snapdeal_MRP

    
) u ON c.Sku = u.merge_key -- Match record condition
WHEN MATCHED AND -- ignore records with no changes
       c.Style_Id<>u.Style_Id
      or c.Catalog<>u.Catalog
      or c.Category<>u.Category
      or c.Weight<>u.Weight
      or c.TP_1<>u.TP_1
      or c.TP_2<>u.TP_2
      or c.MRP_Old<>u.MRP_Old
      or c.Final_MRP_Old<>u.Final_MRP_Old
      or c.Ajio_MRP<>u.Ajio_MRP
      or c.Amazon_MRP<>u.Amazon_MRP
      or c.Amazon_FBA_MRP<>u.Amazon_FBA_MRP
      or c.Flipkart_MRP<>u.Flipkart_MRP
      or c.Limeroad_MRP<>u.Limeroad_MRP
      or c.Myntra_MRP<>u.Myntra_MRP
      or c.Paytm_MRP<>u.Paytm_MRP
      or c.Snapdeal_MRP<>u.Snapdeal_MRP

  THEN
  UPDATE SET -- Update fields on 'old' records
      is_active = 0,
      effective_end = current_timestamp()
WHEN NOT MATCHED THEN
  INSERT
    *

  """

  spark.sql(merge_query)



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from product_details
