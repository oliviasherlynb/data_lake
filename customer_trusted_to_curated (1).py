import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1701801082615 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-database",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1701801082615",
)

# Script generated for node customer_trusted
customer_trusted_node1701801066937 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-database",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1701801066937",
)

# Script generated for node Join
Join_node1701801098857 = Join.apply(
    frame1=accelerometer_trusted_node1701801082615,
    frame2=customer_trusted_node1701801066937,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1701801098857",
)

# Script generated for node Drop Fields
DropFields_node1701801126156 = DropFields.apply(
    frame=Join_node1701801098857,
    paths=["user", "x", "y", "z", "timestamp"],
    transformation_ctx="DropFields_node1701801126156",
)

# Script generated for node customer_curated
customer_curated_node1701801146237 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1701801126156,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://os-stedi-lakehouse/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="customer_curated_node1701801146237",
)

job.commit()
