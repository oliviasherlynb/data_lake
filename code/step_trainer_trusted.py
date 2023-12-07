import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node step_trainer_landing
AWSGlueDataCatalog_node1701941610891 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-database",
    table_name="step_trainer_landing",
    transformation_ctx="AWSGlueDataCatalog_node1701941610891",
)

# Script generated for node customer_curated
AWSGlueDataCatalog_node1701941620998 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-database",
    table_name="customer_curated",
    transformation_ctx="AWSGlueDataCatalog_node1701941620998",
)

# Script generated for node Join
Join_node1701941630465 = Join.apply(
    frame1=AWSGlueDataCatalog_node1701941620998,
    frame2=AWSGlueDataCatalog_node1701941610891,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1701941630465",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1701941678060 = DynamicFrame.fromDF(
    Join_node1701941630465.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1701941678060",
)

# Script generated for node Drop Fields
DropFields_node1701941683948 = DropFields.apply(
    frame=DropDuplicates_node1701941678060,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
        "`.serialnumber`",
    ],
    transformation_ctx="DropFields_node1701941683948",
)

# Script generated for node Amazon S3
AmazonS3_node1701941704318 = glueContext.getSink(
    path="s3://os-stedi-lakehouse/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1701941704318",
)
AmazonS3_node1701941704318.setCatalogInfo(
    catalogDatabase="stedi-database", catalogTableName="step_trainer_trusted"
)
AmazonS3_node1701941704318.setFormat("json")
AmazonS3_node1701941704318.writeFrame(DropFields_node1701941683948)

job.commit()
