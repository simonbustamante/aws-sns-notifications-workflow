
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
#dyf = glueContext.create_dynamic_frame.from_catalog(database='database_name', table_name='table_name')
#dyf.printSchema()
#df = dyf.toDF()
#df.show()
df = spark.read.parquet("s3://s3-sv-anl-dev-ntwrk/RBSDim/")
df.show()
df.count()
df.printSchema()
job.commit()