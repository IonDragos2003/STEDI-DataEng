import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1719061984635 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://dragos-stedi/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1719061984635")

# Script generated for node Customer Curated
CustomerCurated_node1719062005990 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://dragos-stedi/customer/curated/"], "recurse": True}, transformation_ctx="CustomerCurated_node1719062005990")

# Script generated for node Rename Keys for Join
RenameKeysforJoin_node1719062055716 = ApplyMapping.apply(frame=CustomerCurated_node1719062005990, mappings=[("customername", "string", "right_customername", "string"), ("email", "string", "right_email", "string"), ("phone", "string", "right_phone", "string"), ("birthday", "string", "right_birthday", "string"), ("serialnumber", "string", "right_serialnumber", "string"), ("registrationdate", "bigint", "registrationdate", "long"), ("lastupdatedate", "bigint", "lastupdatedate", "long"), ("sharewithresearchasofdate", "bigint", "sharewithresearchasofdate", "long"), ("sharewithpublicasofdate", "bigint", "sharewithpublicasofdate", "long"), ("sharewithfriendsasofdate", "bigint", "sharewithfriendsasofdate", "long")], transformation_ctx="RenameKeysforJoin_node1719062055716")

# Script generated for node Join
StepTrainerLanding_node1719061984635DF = StepTrainerLanding_node1719061984635.toDF()
RenameKeysforJoin_node1719062055716DF = RenameKeysforJoin_node1719062055716.toDF()
Join_node1719062248532 = DynamicFrame.fromDF(StepTrainerLanding_node1719061984635DF.join(RenameKeysforJoin_node1719062055716DF, (StepTrainerLanding_node1719061984635DF['serialNumber'] == RenameKeysforJoin_node1719062055716DF['right_serialnumber']), "leftsemi"), glueContext, "Join_node1719062248532")

# Script generated for node Amazon S3
AmazonS3_node1719062332500 = glueContext.getSink(path="s3://dragos-stedi/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1719062332500")
AmazonS3_node1719062332500.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
AmazonS3_node1719062332500.setFormat("json")
AmazonS3_node1719062332500.writeFrame(Join_node1719062248532)
job.commit()
