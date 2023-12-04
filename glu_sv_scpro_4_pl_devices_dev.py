#%help

import sys
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, lit, concat,udf, trim, when,coalesce, max, row_number
from pyspark.sql.types import StringType, IntegerType, ShortType, DateType, TimestampType 
from pyspark.sql.window import Window
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
dyf = glueContext.create_dynamic_frame.from_catalog(database='database_name', table_name='table_name')
dyf.printSchema()
df = dyf.toDF()
df.show()
s3output = glueContext.getSink(
  path="s3://bucket_name/folder_name",
  connection_type="s3",
  updateBehavior="UPDATE_IN_DATABASE",
  partitionKeys=[],
  compression="snappy",
  enableUpdateCatalog=True,
  transformation_ctx="s3output",
)
s3output.setCatalogInfo(
  catalogDatabase="demo", catalogTableName="populations"
)
s3output.setFormat("glueparquet")
s3output.writeFrame(DyF)
def getDate(date):
    """
    date debe de estar en la siguiente forma "%Y-%m-%d"
    ó en el formato deseado
    para invocar como UDF en Spark seria algo asi: udf_getDate = udf(self.CommonFunctions.getDate)
    luego usarse de la siguiente forma DF.select(udf_getDate(lit("%Y-%m-%d")))
    """
    today = datetime.today()
    return today.strftime(date)
df = spark.read.options(encoding='UTF-8',characterEncoding="UTF-8").parquet("s3a://s3-sv-raw-dev-scpro/4-pl-devices/20230619/id0-activaciones_diarias.snappy.parquet")
df.show()
#SOBREESCRIBE
df.write.format("parquet").mode("overwrite").save("s3a://s3-sv-std-dev-scpro/4-pl-devices/activaciones-diarias/")
#AGREGA
df.write.format("parquet").mode("append").save("s3a://s3-sv-std-dev-scpro/4-pl-devices/activaciones-diarias/")
df1 = df.withColumn("NUEVO_CAMPO",concat( lit("ESTO ES UNA CONSTANTE"),col("FECHA") ))
df1.show()
udf_getDate = udf(getDate)
df2 = df1.withColumn("FECHA_OTRA", udf_getDate(lit("%d-%m-%Y %H:%M:%S")))
df2.show()
job.commit()