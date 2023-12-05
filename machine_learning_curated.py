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
step_trainer_trusted_node1701807919569 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-database",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_node1701807919569",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1701807995037 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-database",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1701807995037",
)

# Script generated for node Join
Join_node1701808012239 = Join.apply(
    frame1=accelerometer_trusted_node1701807995037,
    frame2=step_trainer_trusted_node1701807919569,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="Join_node1701808012239",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1701809688990 = DynamicFrame.fromDF(
    Join_node1701808012239.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1701809688990",
)

# Script generated for node Drop Fields
DropFields_node1701808101873 = DropFields.apply(
    frame=DropDuplicates_node1701809688990,
    paths=["user"],
    transformation_ctx="DropFields_node1701808101873",
)

# Script generated for node machine_learning_curated
machine_learning_curated_node1701808110690 = glueContext.getSink(
    path="s3://os-stedi-lakehouse/final_data/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_curated_node1701808110690",
)
machine_learning_curated_node1701808110690.setCatalogInfo(
    catalogDatabase="stedi-database", catalogTableName="machine_learning_curated"
)
machine_learning_curated_node1701808110690.setFormat("json")
machine_learning_curated_node1701808110690.writeFrame(DropFields_node1701808101873)
job.commit()
