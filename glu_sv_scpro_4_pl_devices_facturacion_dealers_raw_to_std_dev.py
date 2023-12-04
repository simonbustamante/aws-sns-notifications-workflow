#%help

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
df=spark.read.options(encoding='UTF-8').parquet("s3a://s3-sv-raw-dev-scpro/4-pl-devices/facturaciondealer/DatePartKey="+fecha_formateada+"/")
df = df.withColumnRenamed('TRX_NUMBER', 'TRX_NUM').withColumnRenamed('CUSTOMER_NUMBER', 'CUSTOMER_NUM').withColumnRenamed('ORDERED_QUANTITY', 'ORDERED_QTY')
df=df.withColumn("TRX_DATE", to_date("TRX_DATE", "dd/MM/yyyy"))
df.write.format("parquet").mode("overwrite").save("s3a://s3-sv-std-dev-scpro/4-pl-devices/facturacion-dealers/DatePartKey="+fecha_formateada)
job.commit()