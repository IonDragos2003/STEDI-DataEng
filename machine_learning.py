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

# Script generated for node ACC
ACC_node1719150600348 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://dragos-stedi/accelerometer/trusted/"], "recurse": True}, transformation_ctx="ACC_node1719150600348")

# Script generated for node ST
ST_node1719150600514 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://dragos-stedi/step_trainer/trusted/"], "recurse": True}, transformation_ctx="ST_node1719150600514")

# Script generated for node SQL Query
SqlQuery5230 = '''
select * from accelerometer_trusted a join step_trainer_trusted s on a.timestamp = s.sensorreadingtime
'''
SQLQuery_node1719150609262 = sparkSqlQuery(glueContext, query = SqlQuery5230, mapping = {"accelerometer_trusted":ACC_node1719150600348, "step_trainer_trusted":ST_node1719150600514}, transformation_ctx = "SQLQuery_node1719150609262")

# Script generated for node Amazon S3
AmazonS3_node1719150611137 = glueContext.getSink(path="s3://dragos-stedi/ml_good/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1719150611137")
AmazonS3_node1719150611137.setCatalogInfo(catalogDatabase="stedi",catalogTableName="ml_good")
AmazonS3_node1719150611137.setFormat("json")
AmazonS3_node1719150611137.writeFrame(SQLQuery_node1719150609262)
job.commit()
