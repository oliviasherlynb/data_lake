import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1701795506105 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://os-stedi-lakehouse/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1701795506105",
)

# Script generated for node Filter
Filter_node1701795670965 = Filter.apply(
    frame=AmazonS3_node1701795506105,
    f=lambda row: (row["shareWithResearchAsOfDate"] >= 1),
    transformation_ctx="Filter_node1701795670965",
)

# Script generated for node Trusted Customers
TrustedCustomers_node1701795832245 = glueContext.getSink(
    path="s3://os-stedi-lakehouse/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="TrustedCustomers_node1701795832245",
)
TrustedCustomers_node1701795832245.setCatalogInfo(
    catalogDatabase="stedi-database", catalogTableName="customer_trusted"
)
TrustedCustomers_node1701795832245.setFormat("json")
TrustedCustomers_node1701795832245.writeFrame(Filter_node1701795670965)
job.commit()
