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
df=spark.read.options(encoding='UTF-8').parquet("s3a://s3-sv-raw-dev-scpro/4-pl-devices/inventario/DatePartKey="+fecha_formateada+"/")
df=df.withColumn("FECHA_REF",lit(fecha_formateada))
df = df.withColumnRenamed('BODEGA', 'COD_BOD').withColumnRenamed('DESCRIPCION', 'DESC_BOD')
df = df.withColumnRenamed('INVENTARIO', 'TIPO_INV').withColumnRenamed('CODIGO', 'COD_SKU').withColumnRenamed('MODELO_DESCRIPCION', 'DESC_EBS_MOD')
df = df.withColumnRenamed('CANTIDAD', 'CANT_INV').withColumnRenamed('CODIGO_SUB_BODEGA', 'COD_SUB_BOD').withColumnRenamed('CANAL_BODEGA_ENVIA', 'CANAL_BOD_ENVIA')
df.write.format("parquet").mode("overwrite").save("s3a://s3-sv-std-dev-scpro/4-pl-devices/inventario/DatePartKey="+fecha_formateada)
job.commit()