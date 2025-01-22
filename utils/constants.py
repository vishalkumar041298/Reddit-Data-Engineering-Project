import configparser
import os


config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), '../config/config.conf'))


CLIENT_ID = config.get('api_keys', 'reddit_client_id')
SECRET = config.get('api_keys', 'reddit_secret_key')

DATABASE_HOST = config.get('database', 'database_host')
DATABASE_NAME = config.get('database', 'database_name')
DATABASE_PORT = config.get('database', 'database_port')
DATABASE_USERNAME = config.get('database', 'database_username')
DATABASE_PASSWORD = config.get('database', 'database_password')

FILE_INPUT_PATH = config.get('file_paths', 'input_path')
FILE_OUTPUT_PATH = config.get('file_paths', 'output_path')


AWS_ACCESS_KEY_ID = config.get('aws', 'aws_access_key_id')
AWS_SECRET_ACCESS_KEY = config.get('aws', 'aws_secret_access_key')
AWS_SESSION_TOKEN = config.get('aws', 'aws_session_token')
AWS_REGION = config.get('aws', 'aws_region')
AWS_BUCKET_NAME = config.get('aws', 'aws_bucket_name')

POST_FIELDS = (
    'id',
    'title',
    'score',
    'num_comments',
    'author',
    'created_utc',
    'url',
    'over_18',
    'edited',
    'spoiler',
    'stickied'
)


# import sys
# from awsglue.transforms import *
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# from awsglue.context import GlueContext
# from awsglue.job import Job
# from awsgluedq.transforms import EvaluateDataQuality
# from awsglue import DynamicFrame
# from pyspark.sql.functions import concat_ws

# args = getResolvedOptions(sys.argv, ['JOB_NAME'])
# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
# job = Job(glueContext)
# job.init(args['JOB_NAME'], args)

# # Default ruleset used by all target nodes with data quality enabled
# DEFAULT_DATA_QUALITY_RULESET = """
#     Rules = [
#         ColumnCount > 0
#     ]
# """

# # Script generated for node Amazon S3
# AmazonS3_node1736289620224 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ","}, connection_type="s3", format="csv", connection_options={"paths": ["s3://reddit-data-engg-e/raw/reddit_20250107.csv"], "recurse": True}, transformation_ctx="AmazonS3_node1736289620224")


# df = AmazonS3_node1736289620224.toDF()

# df_combined = df.withColumn('ESS_updated', concat_ws(df['edited'],
#     df['spoiler'],
#     df['stickied']))

# df_combined = df.drop('edited', 'spoiler', 'stickied')
    
# S3_Bucket_node_combined = DynamicFrame.fromDF(df_combined, glueContext, S3_Bucket_node_combined)
# # Script generated for node Amazon S3
# EvaluateDataQuality().process_rows(frame=S3_Bucket_node_combined, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1736289615309", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
# AmazonS3_node1736289623233 = glueContext.write_dynamic_frame.from_options(frame=S3_Bucket_node_combined, connection_type="s3", format="csv", connection_options={"path": "s3://reddit-data-engg-e/transformed/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1736289623233")

# job.commit()