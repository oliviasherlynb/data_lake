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

# Script generated for node step_trainer_landing
step_trainer_landing_node1701805328219 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-database",
    table_name="step_trainer_landing",
    transformation_ctx="step_trainer_landing_node1701805328219",
)

# Script generated for node customer_curated
customer_curated_node1701805374745 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-database",
    table_name="customer_curated",
    transformation_ctx="customer_curated_node1701805374745",
)

# Script generated for node Join
Join_node1701805401850 = Join.apply(
    frame1=customer_curated_node1701805374745,
    frame2=step_trainer_landing_node1701805328219,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1701805401850",
)

# Script generated for node Drop Fields
DropFields_node1701805432659 = DropFields.apply(
    frame=Join_node1701805401850,
    paths=[
        "`.serialnumber`",
        "sharewithfriendsasofdate",
        "sharewithpublicasofdate",
        "sharewithresearchasofdate",
        "lastupdatedate",
        "phone",
        "email",
        "customername",
        "birthday",
        "registrationdate",
    ],
    transformation_ctx="DropFields_node1701805432659",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1701805464515 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1701805432659,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://os-stedi-lakehouse/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="step_trainer_trusted_node1701805464515",
)

job.commit()
