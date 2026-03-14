import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node customer_landing
customer_landing_node1773280433023 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="customer_landing_node1773280433023")

# Script generated for node Filter customers agreed to research
SqlQuery6932 = '''
select * from myDataSource where shareWithResearchAsOfDate is not null;

'''
Filtercustomersagreedtoresearch_node1773281041556 = sparkSqlQuery(glueContext, query = SqlQuery6932, mapping = {"myDataSource":customer_landing_node1773280433023}, transformation_ctx = "Filtercustomersagreedtoresearch_node1773281041556")

# Script generated for node customer_trusted
customer_trusted_node1773281203328 = glueContext.write_dynamic_frame.from_catalog(frame=Filtercustomersagreedtoresearch_node1773281041556, database="stedi", table_name="customer_trusted", additional_options={"enableUpdateCatalog": True, "updateBehavior": "UPDATE_IN_DATABASE"}, transformation_ctx="customer_trusted_node1773281203328")

job.commit()