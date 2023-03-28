#!/bin/bash
set -eo pipefail
ARTIFACT_BUCKET=$(cat bucket-name.txt)
aws s3 cp testdata/rawdata.csv s3://$ARTIFACT_BUCKET/inbound/rawdata.csv
TEMPLATE=template.yml
if [ $1 ]
then
  if [ $1 = mvn ]
  then
    TEMPLATE=template-mvn.yml
    mvn package
  fi
else
  gradle build -i -x test
fi
aws cloudformation package --template-file $TEMPLATE --s3-bucket $ARTIFACT_BUCKET --output-template-file deid-deploy-out.yml
aws cloudformation deploy --template-file deid-deploy-out.yml --stack-name deid-java --capabilities CAPABILITY_NAMED_IAM
