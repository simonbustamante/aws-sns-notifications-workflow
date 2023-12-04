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
df=spark.read.options(encoding='UTF-8').parquet("s3a://s3-sv-raw-dev-scpro/4-pl-devices/facturacioncomercial/DatePartKey="+fecha_formateada+"/")
df.show()
df = df.withColumnRenamed('IVADOC', 'IVA_DOC').withColumnRenamed('IVMDOC', 'IVM_DOC').withColumnRenamed('IVDDOC', 'IVD_DOC')
df = df.withColumnRenamed('COD_PROVEEDOR', 'COD_PROV').withColumnRenamed('BODEGA_ENVIA', 'BOD_ENVIA').withColumnRenamed('CODIGO_MODELO', 'COD_MOD').withColumnRenamed('CANTIDAD', 'CANT_FACT')
df = df.withColumnRenamed('CODIGO_SUB_BODEGA', 'COD_SUB_BOD').withColumnRenamed('CANAL_BODEGA_ENVIA', 'CANAL_BOD_ENVIA').withColumnRenamed('NOMBRE_MODELO', 'NOMBRE_MOD')
df.write.format("parquet").mode("overwrite").save("s3a://s3-sv-std-dev-scpro/4-pl-devices/facturacion-comercial/DatePartKey="+fecha_formateada)
job.commit()