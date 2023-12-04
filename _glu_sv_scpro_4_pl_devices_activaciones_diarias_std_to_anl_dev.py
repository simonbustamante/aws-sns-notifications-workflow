#%help
#Librerias necesarias 

import pandas as pd

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from datetime import datetime,timedelta
from pyspark.sql.functions import col,date_format,concat,udf,current_date,lit,to_date,coalesce
from pyspark.sql.types import StringType,IntegerType,ShortType,DateType,TimestampType
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
import boto3
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
#obtener el nombre del archivo del dia anterior (dia-1)
#obtener la fecha actual
fecha_actual = datetime.now().date()

#traemos la fecha anterior
fecha_anterior = fecha_actual - timedelta(days=1)

print(fecha_anterior)

#formateamos la fecha
fecha_formateada = fecha_anterior.strftime('%Y%m%d')
#formateamos la fecha
fecha_formateada = fecha_anterior.strftime('%Y%m%d')
df = spark.read.options(encoding='UTF-8').parquet("s3a://s3-sv-std-dev-scpro/4-pl-devices/activaciones-diarias/DatePartKey=" + fecha_formateada)
df.write.format("parquet").mode("overwrite").save("s3a://s3-sv-anl-dev-scpro/4-pl-devices/activaciones-diarias/")
job.commit()