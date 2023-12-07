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

# Script generated for node ac_trusted
ac_trusted_node1701804428083 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-database",
    table_name="accelerometer_trusted",
    transformation_ctx="ac_trusted_node1701804428083",
)

# Script generated for node cus_trusted
cus_trusted_node1701804450943 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-database",
    table_name="customer_trusted",
    transformation_ctx="cus_trusted_node1701804450943",
)

# Script generated for node Join
Join_node1701804465084 = Join.apply(
    frame1=ac_trusted_node1701804428083,
    frame2=cus_trusted_node1701804450943,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1701804465084",
)

# Script generated for node Drop Fields
DropFields_node1701804496966 = DropFields.apply(
    frame=Join_node1701804465084,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1701804496966",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1701806399802 = DynamicFrame.fromDF(
    DropFields_node1701804496966.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1701806399802",
)

# Script generated for node customer_curated
customer_curated_node1701804559042 = glueContext.getSink(
    path="s3://os-stedi-lakehouse/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_curated_node1701804559042",
)
customer_curated_node1701804559042.setCatalogInfo(
    catalogDatabase="stedi-database", catalogTableName="customer_curated"
)
customer_curated_node1701804559042.setFormat("json")
customer_curated_node1701804559042.writeFrame(DropDuplicates_node1701806399802)
job.commit()
