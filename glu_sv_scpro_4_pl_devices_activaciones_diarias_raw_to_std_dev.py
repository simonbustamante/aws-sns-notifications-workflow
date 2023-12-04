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
df=spark.read.options(encoding='UTF-8').parquet("s3a://s3-sv-raw-dev-scpro/4-pl-devices/activacionesdiarias/DatePartKey="+fecha_formateada+"/")
df = df.withColumnRenamed('CANTIDAD', 'CANT_ACT').withColumnRenamed('TIPO_LINEA', 'TIPO_VENTA')
df=df.withColumn("FECHA", to_date("FECHA", "dd/MM/yyyy"))
df_date=df.withColumn('ANO_MES_DIA', col('FECHA'))
df_for=df_date.withColumn('ANO_MES_DIA', date_format(col('ANO_MES_DIA'), 'yyyyMMdd'))
df.write.format("parquet").mode("overwrite").save("s3a://s3-sv-std-dev-scpro/4-pl-devices/activaciones-diarias/DatePartKey="+fecha_formateada)
job.commit()