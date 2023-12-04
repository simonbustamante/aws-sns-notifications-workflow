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
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
#obtener la fecha actual
fecha_actual = datetime.now().date()

#traemos la fecha anterior
fecha_anterior = fecha_actual - timedelta(days=1)

print(fecha_anterior)

#formateamos la fecha
fecha_formateada = fecha_anterior.strftime('%Y%m%d')

fecha_formateada = "20230702"
#traemos el archivo actual
#df_anl=spark.read.options(encoding='UTF-8').parquet("s3a://s3-sv-anl-dev-scpro/4-pl-devices/inventario/DatePartKey="+fecha_formateada)
#df_anl.show()

df_anl = "s3a://s3-sv-anl-dev-scpro/4-pl-devices/inventario/DatePartKey="+fecha_formateada
df = pd.read_parquet(df_anl)
ruta_csv = "s3a://s3-sv-anl-dev-scpro/4-pl-devices/inventario/DatePartKey="+fecha_formateada+"/inventario.csv"
df.to_csv(ruta_csv, index=False)
job.commit()