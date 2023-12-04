#Librerias necesarias 

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
#obtener el nombre del archivo del dia anterior (dia-1)
#obtener la fecha actual
fecha_actual = datetime.now().date()

#traemos la fecha anterior
fecha_anterior = fecha_actual - timedelta(days=2)

print(fecha_anterior)

#formateamos la fecha
fecha_formateada = fecha_anterior.strftime('%Y%m%d')
#traemos el archivo anterior
df=spark.read.options(encoding='UTF-8').parquet("s3://s3-sv-raw-dev-scpro/4-pl-devices/inventario/DatePartKey="+fecha_formateada+"/")
df.show()
#creamos el archivo
#df.write.format("parquet").mode("overwrite").save("s3a://s3-sv-anl-dev-scpro/4-pl-devices/inventario/DatePartKey="+fecha_formateada)
#traemos la fecha anterior
fecha_anterior_comparativo = fecha_anterior + timedelta(days=1)

print(fecha_anterior_comparativo)

#formateamos la fecha
fecha_formateada_comparativo = fecha_anterior_comparativo.strftime('%Y%m%d')
#traemos el archivo actual
df_anl=spark.read.options(encoding='UTF-8').parquet("s3://s3-sv-raw-dev-scpro/4-pl-devices/inventario/DatePartKey="+fecha_formateada_comparativo+"/")
df_anl.show()
df_join = df_anl.alias("A").join(
                df.alias("B"),
                (col("A.BODEGA") == col("B.BODEGA")) & (col("A.CODIGO") == col("B.CODIGO")), 
                "leftouter"
            ).select(
                coalesce(
                    col("B.BODEGA"), col("A.BODEGA")
                ).alias("BODEGA").cast(StringType()),
                coalesce(
                    col("B.DESCRIPCION"), col("A.DESCRIPCION")
                ).alias("DESCRIPCION").cast(StringType()),
                  coalesce(
                    col("B.INVENTARIO"), col("A.INVENTARIO")
                ).alias("INVENTARIO").cast(StringType()),
                 coalesce(
                    col("B.CODIGO"), col("A.CODIGO")
                ).alias("CODIGO").cast(StringType()),
                 coalesce(
                    col("B.MODELO_DESCRIPCION"), col("A.MODELO_DESCRIPCION")
                ).alias("MODELO_DESCRIPCION").cast(StringType()),
                 coalesce(
                    col("B.CANTIDAD"), col("A.CANTIDAD")
                ).alias("CANTIDAD").cast(StringType()),
                 coalesce(
                    col("B.DISPONIBLE"), col("A.DISPONIBLE")
                ).alias("DISPONIBLE").cast(StringType()),
                coalesce(
                    col("B.LINEA"), col("A.LINEA")
                ).alias("LINEA").cast(StringType()),
                coalesce(
                    col("B.CODIGO_SUB_BODEGA"), col("A.CODIGO_SUB_BODEGA")
                ).alias("CODIGO_SUB_BODEGA").cast(StringType()),
                coalesce(
                    col("B.CANAL_BODEGA_ENVIA"), col("A.CANAL_BODEGA_ENVIA")
                ).alias("CANAL_BODEGA_ENVIA").cast(StringType()),
                coalesce(
                    col("B.NOMBRE_MODELO"), col("A.NOMBRE_MODELO")
                ).alias("NOMBRE_MODELO").cast(StringType())
            )
df_join.show()
df_join.write.format("parquet").mode("overwrite").save("s3://s3-sv-raw-dev-scpro/4-pl-devices/traslados-pendientes/DatePartKey="+fecha_formateada+"/")
job.commit()