#!/bin/bash -x
set -eo pipefail
FUNCTION=$(aws cloudformation describe-stack-resource --stack-name deid-java --logical-resource-id function --query 'StackResourceDetail.PhysicalResourceId' --output text)
BUCKET_NAME=$(aws cloudformation describe-stack-resource --stack-name deid-java --logical-resource-id bucket --query 'StackResourceDetail.PhysicalResourceId' --output text)

if [ ! -f event.json ]; then
  cp event.json.template event.json
  sed -i'' -e "s/BUCKET_NAME/$BUCKET_NAME/" event.json

fi

  aws lambda invoke --function-name $FUNCTION --cli-binary-format raw-in-base64-out --payload file://event.json out.json
  cat out.json
  echo ""
