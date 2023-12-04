#%help

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
spark.read.parquet("s3://s3-sv-anl-dev-ntwrk/RBSDim/").show()
spark.read.parquet("s3://s3-sv-anl-dev-ntwrk/RBSDim/").printSchema()
job.commit()