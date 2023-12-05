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

# Script generated for node c_t
c_t_node1701798281372 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://os-stedi-lakehouse/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="c_t_node1701798281372",
)

# Script generated for node a_l
a_l_node1701798125961 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-database",
    table_name="accelerometer_landing",
    transformation_ctx="a_l_node1701798125961",
)

# Script generated for node Join
Join_node1701798174945 = Join.apply(
    frame1=a_l_node1701798125961,
    frame2=c_t_node1701798281372,
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
AccelerometerTrusted_node1701798341829 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1701798415414,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://os-stedi-lakehouse/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrusted_node1701798341829",
)

job.commit()
