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
AmazonS3_node1719059537406 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://dragos-stedi/customer/trusted/"], "recurse": True}, transformation_ctx="AmazonS3_node1719059537406")

# Script generated for node Amazon S3
AmazonS3_node1719059530135 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://dragos-stedi/accelerometer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1719059530135")

# Script generated for node Join
Join_node1719059572585 = Join.apply(frame1=AmazonS3_node1719059530135, frame2=AmazonS3_node1719059537406, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1719059572585")

# Script generated for node Drop Fields
DropFields_node1719059590470 = DropFields.apply(frame=Join_node1719059572585, paths=["customerName", "email", "phone", "birthDay", "serialNumber", "registrationDate", "lastUpdateDate", "shareWithResearchAsOfDate", "shareWithPublicAsOfDate", "shareWithFriendsAsOfDate"], transformation_ctx="DropFields_node1719059590470")

# Script generated for node Amazon S3
AmazonS3_node1719059726977 = glueContext.getSink(path="s3://dragos-stedi/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1719059726977")
AmazonS3_node1719059726977.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1719059726977.setFormat("json")
AmazonS3_node1719059726977.writeFrame(DropFields_node1719059590470)
job.commit()