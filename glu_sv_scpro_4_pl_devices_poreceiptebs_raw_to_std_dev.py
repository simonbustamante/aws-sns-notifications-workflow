
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
df=spark.read.options(encoding='UTF-8').parquet("s3a://s3-sv-raw-dev-scpro/4-pl-devices/poreceiptebs/DatePartKey="+fecha_formateada+"/")
df.show()
df = df.withColumn("FECHA_REF",lit(fecha_formateada))
df.show()
df = df.withColumn('PO_NUMBER', col("PO_NUMBER").cast("string")) \
       .withColumn('ITEM', col("ITEM").cast("string"))\
       .withColumn('CANTIDAD', col("CANTIDAD").cast("int"))\
       .withColumn('BODEGA', col("BODEGA").cast("string"))\
       .withColumn('FECHA_TRX', col("FECHA_TRX").cast("int"))\
       .withColumn('HORA_TRX', col("HORA_TRX").cast("int"))

df.show()
df.write.format("parquet").mode("overwrite").save("s3a://s3-sv-std-dev-scpro/4-pl-devices/poreceiptebs/DatePartKey="+fecha_formateada)
df.show()
job.commit()