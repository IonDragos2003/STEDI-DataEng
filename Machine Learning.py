import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1719062686505 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://dragos-stedi/step_trainer/trusted/"], "recurse": True}, transformation_ctx="StepTrainerTrusted_node1719062686505")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1719062686968 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://dragos-stedi/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1719062686968")

# Script generated for node Customer Curated
CustomerCurated_node1719062687477 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://dragos-stedi/customer/curated/"], "recurse": True}, transformation_ctx="CustomerCurated_node1719062687477")

# Script generated for node Join Acc Cust
JoinAccCust_node1719062863201 = Join.apply(frame1=AccelerometerTrusted_node1719062686968, frame2=CustomerCurated_node1719062687477, keys1=["user"], keys2=["email"], transformation_ctx="JoinAccCust_node1719062863201")

# Script generated for node Join prevJoin StepTrainer
JoinAccCust_node1719062863201DF = JoinAccCust_node1719062863201.toDF()
StepTrainerTrusted_node1719062686505DF = StepTrainerTrusted_node1719062686505.toDF()
JoinprevJoinStepTrainer_node1719062929086 = DynamicFrame.fromDF(JoinAccCust_node1719062863201DF.join(StepTrainerTrusted_node1719062686505DF, (JoinAccCust_node1719062863201DF['serialnumber'] == StepTrainerTrusted_node1719062686505DF['serialNumber']), "left"), glueContext, "JoinprevJoinStepTrainer_node1719062929086")

# Script generated for node Change Schema
ChangeSchema_node1719063780161 = ApplyMapping.apply(frame=JoinprevJoinStepTrainer_node1719062929086, mappings=[("z", "double", "z", "double"), ("user", "string", "user", "string"), ("y", "double", "y", "double"), ("x", "double", "x", "double"), ("timestamp", "bigint", "timestamp", "bigint"), ("customername", "string", "customername", "string"), ("email", "string", "email", "string"), ("phone", "string", "phone", "string"), ("birthday", "string", "birthday", "string"), ("serialnumber", "string", "serialNumber_prevJoin", "string"), ("registrationdate", "bigint", "registrationdate", "bigint"), ("lastupdatedate", "bigint", "lastupdatedate", "bigint"), ("sharewithresearchasofdate", "bigint", "sharewithresearchasofdate", "bigint"), ("sharewithpublicasofdate", "bigint", "sharewithpublicasofdate", "bigint"), ("sharewithfriendsasofdate", "bigint", "sharewithfriendsasofdate", "bigint"), ("sensorReadingTime", "bigint", "sensorReadingTime", "bigint"), ("serialNumber", "string", "serialNumber", "string"), ("distanceFromObject", "int", "distanceFromObject", "int")], transformation_ctx="ChangeSchema_node1719063780161")

# Script generated for node Aggregate
Aggregate_node1719063069340 = sparkAggregate(glueContext, parentFrame = ChangeSchema_node1719063780161, groups = ["x", "y", "z", "user", "sensorReadingTime", "serialNumber", "distanceFromObject", "serialNumber_prevJoin"], aggs = [["timestamp", "count"]], transformation_ctx = "Aggregate_node1719063069340")

# Script generated for node Amazon S3
AmazonS3_node1719063968586 = glueContext.getSink(path="s3://dragos-stedi/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1719063968586")
AmazonS3_node1719063968586.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning")
AmazonS3_node1719063968586.setFormat("json")
AmazonS3_node1719063968586.writeFrame(Aggregate_node1719063069340)
job.commit()