
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
# Reemplaza 'ruta_del_archivo/nombre_del_archivo.parquet' con la ubicación real del archivo Parquet en S3
df_taslados_p = spark.read.parquet('s3://s3-sv-anl-dev-scpro/4-pl-devices/traslados-pendientes/')
df_traslados_c = spark.read.parquet('s3://s3-sv-anl-dev-scpro/4-pl-devices/traslados-confirmados/')
df_libre_u = spark.read.parquet('s3://s3-sv-anl-dev-scpro/4-pl-devices/libre-utilizacion/')
df_fact_dealers = spark.read.parquet('s3://s3-sv-anl-dev-scpro/4-pl-devices/facturacion-dealers/')
df_fact_com = spark.read.parquet('s3://s3-sv-anl-dev-scpro/4-pl-devices/facturacion-comercial/')
df_inventario = spark.read.parquet('s3://s3-sv-anl-dev-scpro/4-pl-devices/inventario/')
df_act_diarias = spark.read.parquet('s3://s3-sv-anl-dev-scpro/4-pl-devices/activaciones-diarias/')
#df_ingresos = spark.read.parquet('s3://s3-sv-anl-dev-scpro/4-pl-devices/ingresos/')
# Reemplaza 'ruta_del_archivo/nombre_del_archivo.csv' con la ubicación donde deseas guardar el archivo CSV en S3
df_taslados_p.write.csv('s3://s3-sv-anl-dev-scpro/4-pl-devices/archivos-csv/traslados-pendientes', header=True)
df_traslados_c.write.csv('s3://s3-sv-anl-dev-scpro/4-pl-devices/archivos-csv/traslados-confirmados', header=True)
df_libre_u.write.csv('s3://s3-sv-anl-dev-scpro/4-pl-devices/archivos-csv/libre-utilizacion', header=True)
df_fact_dealers.write.csv('s3://s3-sv-anl-dev-scpro/4-pl-devices/archivos-csv/facturacion-dealers', header=True)
df_fact_com.write.csv('s3://s3-sv-anl-dev-scpro/4-pl-devices/archivos-csv/facturacion-comercial', header=True)
df_inventario.write.csv('s3://s3-sv-anl-dev-scpro/4-pl-devices/archivos-csv/inventario', header=True)
df_act_diarias.write.csv('s3://s3-sv-anl-dev-scpro/4-pl-devices/archivos-csv/activaciones-diarias', header=True)
#df_ingresos.overwrite.csv('s3://s3-sv-anl-dev-scpro/4-pl-devices/archivos-csv/ingresos', header=True)
job.commit()