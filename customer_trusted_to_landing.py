import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1718620233539 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-dragos/customer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1718620233539")

# Script generated for node Filter
Filter_node1718620377862 = Filter.apply(frame=AmazonS3_node1718620233539, f=lambda row: (not(row["shareWithResearchAsOfDate"] == 0)), transformation_ctx="Filter_node1718620377862")

# Script generated for node Amazon S3
AmazonS3_node1718620560377 = glueContext.write_dynamic_frame.from_options(frame=Filter_node1718620377862, connection_type="s3", format="json", connection_options={"path": "s3://stedi-dragos/customer/trusted/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="AmazonS3_node1718620560377")

job.commit()