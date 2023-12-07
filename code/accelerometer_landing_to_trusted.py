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

# Script generated for node customer_trusted
customer_trusted_node1701878781731 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-database",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1701878781731",
)

# Script generated for node accelerometer_landing
accelerometer_landing_node1701798125961 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-database",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_node1701798125961",
)

# Script generated for node Join
Join_node1701798174945 = Join.apply(
    frame1=accelerometer_landing_node1701798125961,
    frame2=customer_trusted_node1701878781731,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1701798174945",
)

# Script generated for node Drop Fields
DropFields_node1701798415414 = DropFields.apply(
    frame=Join_node1701798174945,
    paths=[
        "serialnumber",
        "birthday",
        "registrationdate",
        "sharewithresearchasofdate",
        "customername",
        "sharewithfriendsasofdate",
        "email",
        "lastupdatedate",
        "phone",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1701798415414",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1701798341829 = glueContext.getSink(
    path="s3://os-stedi-lakehouse/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1701798341829",
)
AccelerometerTrusted_node1701798341829.setCatalogInfo(
    catalogDatabase="stedi-database", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1701798341829.setFormat("json")
AccelerometerTrusted_node1701798341829.writeFrame(DropFields_node1701798415414)
job.commit()
