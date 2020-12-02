#!/bin/bash

echo "Starting Ingesting Document from S3"

cd /opt/app/nce/py/s3EsIngest/
source ./env/bin/activate
python3.7 s3ingest.py portal-content

ret=$?
if [ $ret -ne 0 ]; then
  deactivate
  exit 1
fi

deactivate
echo "Finished"
