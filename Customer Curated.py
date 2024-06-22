import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node Drop fields and duplicates
SqlQuery5621 = '''
select distinct customername, email, phone, birthday, 
serialnumber, registrationdate, lastupdatedate, 
sharewithresearchasofdate, sharewithpublicasofdate,
sharewithfriendsasofdate from myDataSource
'''
Dropfieldsandduplicates_node1719060468963 = sparkSqlQuery(glueContext, query = SqlQuery5621, mapping = {"myDataSource":Join_node1719059572585}, transformation_ctx = "Dropfieldsandduplicates_node1719060468963")

# Script generated for node Amazon S3
AmazonS3_node1719060489988 = glueContext.getSink(path="s3://dragos-stedi/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1719060489988")
AmazonS3_node1719060489988.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
AmazonS3_node1719060489988.setFormat("json")
AmazonS3_node1719060489988.writeFrame(Dropfieldsandduplicates_node1719060468963)
job.commit()