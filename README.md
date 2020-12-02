# s3 Elasticsearch Ingest

This python script is used for ingesting documents stored in the ECS S3 buckets into Elasticsearch for indexing. The ingest-attachment plugin in used in Elasticsearch to extract content from the documents before indexing it. Below is the steps taken by the script

1) Establish connection to S3 bucket and iterate through the keys
2) Check the last modified timestamp of the key against the last update extract time stored in the timestamp file
3) Pull the objects for the newly created files from S3 in binary format
4) Encode the the data stream into Base64 encoding
5) Performs a POST to the Elasticsearch endpoint with the pipeline transformation

# How to use the script
Create an .env file for the below key/value pairs. These keys are used in the s3ingest.py at runtime

ca=''
es_api_key=''
es_hostname=''
aws_access_key_id=''
aws_secret_access_key=''
aws_s3_endpoint_url=''

./s3ingest.sh
