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

# Script generated for node AC Trusted
ACTrusted_node1719147920031 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://dragos-stedi/accelerometer/trusted/"], "recurse": True}, transformation_ctx="ACTrusted_node1719147920031")

# Script generated for node ST Trusted
STTrusted_node1719147919650 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://dragos-stedi/step_trainer/trusted/"], "recurse": True}, transformation_ctx="STTrusted_node1719147919650")

# Script generated for node Cust Curated
CustCurated_node1719147920228 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://dragos-stedi/customer/curated/"], "recurse": True}, transformation_ctx="CustCurated_node1719147920228")

# Script generated for node Change Schema
ChangeSchema_node1719148618600 = ApplyMapping.apply(frame=STTrusted_node1719147919650, mappings=[("sensorreadingtime", "long", "st_sensorreadingtime", "long"), ("serialnumber", "string", "st_serialnumber", "string"), ("distancefromobject", "int", "st_distancefromobject", "int")], transformation_ctx="ChangeSchema_node1719148618600")

# Script generated for node SQL Query
SqlQuery109 = '''
select * from customer c join accelerometer a on c.email = a.user
'''
SQLQuery_node1719147997327 = sparkSqlQuery(glueContext, query = SqlQuery109, mapping = {"customer":CustCurated_node1719147920228, "accelerometer":ACTrusted_node1719147920031}, transformation_ctx = "SQLQuery_node1719147997327")

# Script generated for node SQL Query
SqlQuery108 = '''
select * from stepTrainer s join joinedPrev j on s.st_serialnumber = j.serialnumber
'''
SQLQuery_node1719148110745 = sparkSqlQuery(glueContext, query = SqlQuery108, mapping = {"joinedPrev":SQLQuery_node1719147997327, "stepTrainer":ChangeSchema_node1719148618600}, transformation_ctx = "SQLQuery_node1719148110745")

# Script generated for node SQL Query
SqlQuery110 = '''
SELECT
    y,
    z,
    x,
    user,
    st_sensorReadingTime,
    st_distanceFromObject,
    serialNumber,
    COUNT(timeStamp) as timeStamp_count
FROM
    myDataSource
GROUP BY
    y,
    z,
    x,
    user,
    st_sensorReadingTime,
    st_distanceFromObject,
    serialNumber
'''
SQLQuery_node1719148503915 = sparkSqlQuery(glueContext, query = SqlQuery110, mapping = {"myDataSource":SQLQuery_node1719148110745}, transformation_ctx = "SQLQuery_node1719148503915")

# Script generated for node Amazon S3
AmazonS3_node1719148738794 = glueContext.getSink(path="s3://dragos-stedi/ml_new/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1719148738794")
AmazonS3_node1719148738794.setCatalogInfo(catalogDatabase="stedi",catalogTableName="ml_new")
AmazonS3_node1719148738794.setFormat("json")
AmazonS3_node1719148738794.writeFrame(SQLQuery_node1719148503915)
job.commit()
