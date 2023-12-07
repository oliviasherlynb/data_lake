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

# Script generated for node step_trainer_trusted
AWSGlueDataCatalog_node1701943779126 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-database",
    table_name="step_trainer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1701943779126",
)

# Script generated for node accelerometer_trusted
AWSGlueDataCatalog_node1701943822552 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-database",
    table_name="accelerometer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1701943822552",
)

# Script generated for node Join
Join_node1701943919837 = Join.apply(
    frame1=AWSGlueDataCatalog_node1701943779126,
    frame2=AWSGlueDataCatalog_node1701943822552,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="Join_node1701943919837",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1701944217871 = DynamicFrame.fromDF(
    Join_node1701943919837.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1701944217871",
)

# Script generated for node Drop Fields
DropFields_node1701944321996 = DropFields.apply(
    frame=DropDuplicates_node1701944217871,
    paths=["user"],
    transformation_ctx="DropFields_node1701944321996",
)

# Script generated for node machine_learning_curated
AmazonS3_node1701944330266 = glueContext.getSink(
    path="s3://os-stedi-lakehouse/final_data/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1701944330266",
)
AmazonS3_node1701944330266.setCatalogInfo(
    catalogDatabase="stedi-database", catalogTableName="machine_learning_curated"
)
AmazonS3_node1701944330266.setFormat("json")
AmazonS3_node1701944330266.writeFrame(DropFields_node1701944321996)

job.commit()
