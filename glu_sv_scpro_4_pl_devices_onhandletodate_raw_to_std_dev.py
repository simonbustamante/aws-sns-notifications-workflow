
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from datetime import datetime,timedelta
from pyspark.sql.functions import col,date_format,concat,udf,current_date,lit,to_date
from pyspark.sql.types import StringType,IntegerType,ShortType,DateType,TimestampType
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
#I N I C I O
fecha_actual = datetime.now().date()
fecha_anterior = fecha_actual - timedelta(days=1)
fecha_formateada = fecha_anterior.strftime('%Y%m%d')
print(fecha_formateada)
df=spark.read.options(encoding='UTF-8').parquet("s3a://s3-sv-raw-dev-scpro/4-pl-devices/onhandletodate/DatePartKey="+fecha_formateada+"/")
df.show()
####  Run this cell to set up and start your interactive session.
new_order = ["ORGANIZATION_CODE", "SUBINVENTORY_CODE", "SEGMENT1", "DESCRIPTION", "COST", "ON_HAND", "ORGANIZATION_NAME"]  # Define el nuevo orden deseado
df = df.select(*new_order)
df.show()
df = df.withColumnRenamed('ORGANIZATION_CODE', 'ORG_CODE') \
       .withColumnRenamed('SUBINVENTORY_CODE', 'SUB_INV_CODE')\
       .withColumnRenamed('SEGMENT1', 'ITEM_VALUE')\
       .withColumnRenamed('DESCRIPTION', 'DESCRIPTION')\
       .withColumnRenamed('COST', 'ITEM_COST')\
       .withColumnRenamed('ON_HAND', 'TOTAL_QTY')\
       .withColumnRenamed('ORGANIZATION_NAME', 'ORGANIZATION_NAME')
    
df = df.withColumn("FECHA_REF",lit(fecha_formateada))
df.show()
df = df.withColumn('ORG_CODE', col("ORG_CODE").cast("string")) \
       .withColumn('SUB_INV_CODE', col("SUB_INV_CODE").cast("string"))\
       .withColumn('ITEM_VALUE', col("ITEM_VALUE").cast("string"))\
       .withColumn('DESCRIPTION', col("DESCRIPTION").cast("string"))\
       .withColumn('ITEM_COST', col("ITEM_COST").cast("double"))\
       .withColumn('TOTAL_QTY', col("TOTAL_QTY").cast("int"))\
       .withColumn('ORGANIZATION_NAME', col("ORGANIZATION_NAME").cast("string"))

df.show()
df.write.format("parquet").mode("overwrite").save("s3a://s3-sv-std-dev-scpro/4-pl-devices/onhandletodate/DatePartKey="+fecha_formateada)
df.show()
job.commit()