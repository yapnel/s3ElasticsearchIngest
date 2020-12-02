import sys
import os
import boto3
import base64
import urllib.parse
import json
import requests
from datetime import datetime,date
import datetime as dt
from dotenv import load_dotenv
load_dotenv()

f = open("timestamp", "r")
start_date= datetime.strptime(f.read().strip(),'%d-%m-%Y %H:%M:%S')
end_date=dt.datetime.now()
bucketname=sys.argv[1]
hostname=os.getenv('es_hostname')
headers = {'Content-Type': 'application/json', 'Authorization': os.getenv('es_api_key')}
CA=os.getenv('ca')

session = boto3.session.Session()
s3 = session.client(
  service_name='s3',
  aws_access_key_id=os.getenv('aws_access_key_id'),
  aws_secret_access_key=os.getenv('aws_secret_access_key'),
  endpoint_url=os.getenv('aws_s3_endpoint_url'),
  verify=CA
)

success_counter = 0
failed_counter = 0  
continuation_token = ''

while True:

  objects = s3.list_objects_v2(Bucket=bucketname,ContinuationToken=continuation_token)
  
  for obj in objects['Contents']:

    if start_date <= obj['LastModified'].replace(tzinfo=None) <= end_date :
     
     file = s3.get_object(Bucket=bucketname,Key=obj['Key'])
     
     base64_bytes = base64.b64encode(file['Body'].read())
     content = base64_bytes.decode('utf-8')

     filename = urllib.parse.quote(obj["Key"])      

     doc = {
        'source': 'S3',
        'ingest_date': datetime.now().isoformat(),
        'data': content
     }

     x = requests.post(
        url='https://'+hostname+':9200/library/_doc/'+filename+'?pipeline=content-extract',
        data=json.dumps(doc),
        headers=headers,
        verify=CA,
        timeout=180
     )
  
     try:
       response=json.loads(x.text)["_shards"]["successful"]
       result={}
       result['Filename']=obj["Key"]
       result['LastModified']=obj["LastModified"].replace(tzinfo=None)
       result['Size']=str(round(obj["Size"]/1024/1024,2))+"MB"
       result['Response']=response < 1 and "Fail" or "Success"
       print(json.dumps(result, sort_keys=True, indent=4, default=str))
       success_counter = success_counter + 1
     except Exception as e:
       print(json.dumps(json.loads(x.text), sort_keys=True, indent=4, default=str))
       failed_counter = failed_counter + 1

  if not objects.get('IsTruncated'):  # At the end of the list?
    break
  continuation_token = objects.get('NextContinuationToken')


print("Total documents indexed is " + str(success_counter) + " and total documents failed to index is " + str(failed_counter))  

f = open("timestamp", "w")
f.write(end_date.strftime('%d-%m-%Y %H:%M:%S'))
f.close()  
