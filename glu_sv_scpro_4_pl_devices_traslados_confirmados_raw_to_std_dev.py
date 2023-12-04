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
import boto3
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
s3_client = boto3.client('s3')
fecha_actual = datetime.now().date()
fecha_anterior = fecha_actual - timedelta(days=1)
fecha_formateada = fecha_anterior.strftime('%Y%m%d')
print(fecha_formateada)
try:
    response = s3_client.list_objects(Bucket="s3-sv-raw-dev-scpro", Prefix="4-pl-devices/trasladosconfirmados/DatePartKey=" + fecha_formateada + "/")
    if 'Contents' in response:
        df=spark.read.options(encoding='UTF-8').parquet("s3a://s3-sv-raw-dev-scpro/4-pl-devices/trasladosconfirmados/DatePartKey="+fecha_formateada+"/")
        df = df.withColumnRenamed('BODEGA_ENVIA', 'BOD_ENVIA').withColumnRenamed('DESCRIPCION', 'DESC_MOD').withColumnRenamed('CANTIDAD', 'CANT_TRAS').withColumnRenamed('BODEGA_RECIBE', 'BOD_RECIBE')
        df = df.withColumnRenamed('MES_CONFIR', 'MES_CONFI').withColumnRenamed('CODIGO_SUB_BODEGA', 'COD_SUB_BOD').withColumnRenamed('CANAL_BODEGA_ENVIA', 'CANAL_BOD_ENVIA').withColumnRenamed('CANAL_BODEGA_RECIBE', 'CANAL_BOD_RECIBE')
        df.write.format("parquet").mode("overwrite").save("s3a://s3-sv-std-dev-scpro/4-pl-devices/traslados-confirmados/DatePartKey="+fecha_formateada)
        df.show()
    else:
        #print(f"La ruta '{ruta}' no existe en el bucket.")
        print(1)
except Exception as e:
    print(f"Ocurrió un error: {e}")
job.commit()