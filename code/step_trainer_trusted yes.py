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

# Script generated for node customer_curated
customer_curated_node1701882239575 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi-database",
    table_name="customer_curated",
    transformation_ctx="customer_curated_node1701882239575",
)

# Script generated for node Amazon S3
AmazonS3_node1701884977791 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://os-stedi-lakehouse/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1701884977791",
)

# Script generated for node Join
Join_node1701882330408 = Join.apply(
    frame1=customer_curated_node1701882239575,
    frame2=AmazonS3_node1701884977791,
    keys1=["serialnumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1701882330408",
)

# Script generated for node Drop Fields
DropFields_node1701882350791 = DropFields.apply(
    frame=Join_node1701882330408,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1701882350791",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1701882385162 = glueContext.getSink(
    path="s3://os-stedi-lakehouse/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_node1701882385162",
)
step_trainer_trusted_node1701882385162.setCatalogInfo(
    catalogDatabase="stedi-database", catalogTableName="step_train_trusted"
)
step_trainer_trusted_node1701882385162.setFormat("json")
step_trainer_trusted_node1701882385162.writeFrame(DropFields_node1701882350791)
job.commit()
