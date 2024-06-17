import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1718628705151 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AmazonS3_node1718628705151")

# Script generated for node Amazon S3
AmazonS3_node1718628706001 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="AmazonS3_node1718628706001")

# Script generated for node Join
Join_node1718628762354 = Join.apply(frame1=AmazonS3_node1718628706001, frame2=AmazonS3_node1718628705151, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1718628762354")

# Script generated for node Drop Fields
DropFields_node1718628925592 = DropFields.apply(frame=Join_node1718628762354, paths=["timestamp", "email", "phone", "serialnumber", "sharewithpublicasofdate", "birthday", "registrationdate", "sharewithresearchasofdate", "customername", "sharewithfriendsasofdate", "lastupdatedate"], transformation_ctx="DropFields_node1718628925592")

# Script generated for node Amazon S3
AmazonS3_node1718629113961 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1718628925592, connection_type="s3", format="json", connection_options={"path": "s3://stedi-dragos/accelerometer/trusted/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1718629113961")

job.commit()
