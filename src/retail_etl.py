from pyspark.sql import DataFrame, Window, functions as F

def build_sales_fact(orders:DataFrame)->DataFrame:
 required={"order_id","product_id","customer_id","order_ts","quantity","unit_price","updated_at"}
 missing=required-set(orders.columns)
 if missing:raise ValueError(f"missing columns: {sorted(missing)}")
 latest=Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())
 return (orders.withColumn("row_number",F.row_number().over(latest)).filter("row_number = 1")
  .filter((F.col("quantity")>0)&(F.col("unit_price")>=0)).drop("row_number")
  .withColumn("order_date",F.to_date("order_ts"))
  .withColumn("revenue",F.round(F.col("quantity")*F.col("unit_price"),2)))

def merge_delta(spark,updates:DataFrame,target:str):
 from delta.tables import DeltaTable
 if DeltaTable.isDeltaTable(spark,target):
  DeltaTable.forPath(spark,target).alias("t").merge(updates.alias("s"),"t.order_id=s.order_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
 else:updates.write.format("delta").partitionBy("order_date").save(target)