# Databricks notebook source
# MAGIC %md
# MAGIC **DATA PIPELINE TO IDENTITY THE VALUEABLE CUSTOMERS IN AN ORGANISATION**

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 1:Data Extraction**

# COMMAND ----------

#Importing required libraries
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------


#Reading data from first file
vip1 = spark.table("default.vips_20201101").filter(col("country").like("%Netherlands%"))
vip1.show()

# COMMAND ----------

#Reading data from Second file
vip2 = spark.table("default.vips_20201115").filter(col("country").like("%Netherlands%"))
vip2.show()

# COMMAND ----------

#Reading data from third file
vip3 = spark.table("default.vips_20201125").filter(col("country").like("%Netherlands%"))
vip3.show()

# COMMAND ----------

#Reading data from User manager mapping file
umd = spark.table("default.umd_vip_to_profile_mapping")
umd.show()

# COMMAND ----------

#Reading data from transacions File
transaction = spark.table("default.vip_transactions")
transaction.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2: Data Engineering**

# COMMAND ----------

#Combining the data from all three customer files
modified_dt = "modified_dt"
vipunion = vip1.select("vip_id", "first_name", "last_name", "vip_type", "country", "email") \
    .withColumn(modified_dt,lit("20201101")) \
.union(vip2.select("vip_id", "first_name", "last_name", "vip_type", "country", "email") \
       .withColumn(modified_dt,lit("20201115"))) \
.union(vip3.select("vip_id", "first_name", "last_name", "vip_type", "country", "email") \
    .withColumn(modified_dt,lit("20201125")))
vipunion.createOrReplaceTempView("vw_vipunion")
vipunion.show()

# COMMAND ----------

#Finding the latest costemer details
vip1= vipunion.withColumn("rownum",row_number().over(Window.partitionBy("vip_id").orderBy(col("modified_dt").desc())))
viplatest= vip1.filter(col("rownum")==1)
viplatest.show()

# COMMAND ----------

#Filtering the Active customer details based on metadata_change_date
umd1 = umd.withColumn("rownumb",row_number().over(Window.partitionBy("vip_id").orderBy(col("meta_change_date").desc())))
umdlatest= umd1.filter(col("rownumb")==1)
umdlatest.display()

# COMMAND ----------

#Processing the quantity,recommended_retail_price_per_unit and discount_amount_per_unit column values based on cancellation_flag
transaction1= transaction.select("profile_id","transaction_date","order_number","line_number","product_id",abs("quantity").alias("quantity"),"currency", \
when(col("cancellation_flag")=="yes",-abs(col("recommended_retail_price_per_unit"))).otherwise(col("recommended_retail_price_per_unit")).alias("recommended_retail_price_per_unit"),\
when(col("cancellation_flag")=="yes",when(col("discount_amount_per_unit").isNull(),col("discount_amount_per_unit")).otherwise(-abs(col("discount_amount_per_unit")))).otherwise(col("discount_amount_per_unit")).alias("discount_amount_per_unit"),\
"cancellation_flag")
transaction1.display()

# COMMAND ----------

#Calculating the row level transaction amount and aggregating the transaction amount for each customer
transaction2= transaction1.withColumn("Transac_value",col("quantity")*(col("recommended_retail_price_per_unit")-coalesce(col("discount_amount_per_unit"),lit(0)))).orderBy("profile_id","transaction_date","order_number","line_number","product_id")
transcationlatest = transaction2.withColumn("Total_Trans_value",sum("Transac_value").over(Window.partitionBy("profile_id"))).orderBy("profile_id","transaction_date","order_number","line_number","product_id")
validtran= transcationlatest.select("profile_id","Total_Trans_value").distinct()
validtran.display()

# COMMAND ----------

#Combining the data from the customer,sales and marketing files to get the active vip customer details, residing in Netherlands
Combiningvip = viplatest.alias("vip") \
    .join(umdlatest.alias("umd"),
          col("vip.vip_id")==col("umd.vip_id"), "inner") \
              .join(validtran.alias("tran"),
                    col("umd.profile_id")==col("tran.profile_id"),
                    "Left") \
                        .filter(col("umd.active")=="yes") \
.select(concat(col("vip.first_name"),lit(" "),col("vip.last_name")).alias ("VIP_Name"),col("vip.email").alias("Email_Address"), coalesce(col("Total_Trans_value"),lit(0)).alias("Total_Sales_Value"))
Combiningvip.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3: Data Loading**

# COMMAND ----------

#creating a delta table to store the output data
path = f'<file path>'
print(path)
db_sql = f'''create table if not exists vip.vip_nl (
VIP_Name string,
Email_Address bigint,
Total_Sales_Value double
 )
USING delta
PARTITIONED BY (VIP_Name)
LOCATION "{path}"'''

# COMMAND ----------

#Inserting data to the delta table vip.vip_nl
insert_vip_details = '''
    INSERT INTO vip.vip_nl
    SELECT * FROM Combiningvip  
    '''
