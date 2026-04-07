#!/bin/bash
# scripts/deploy_dashboard.sh
set -e

BUCKET=$(aws cloudformation describe-stacks --stack-name WxStack \
  --query "Stacks[0].Outputs[?OutputKey=='DashboardBucketName'].OutputValue" \
  --output text --region us-east-1)

DIST_ID=$(aws cloudformation describe-stacks --stack-name WxStack \
  --query "Stacks[0].Outputs[?OutputKey=='DashboardDistributionId'].OutputValue" \
  --output text --region us-east-1)

echo "Syncing to s3://$BUCKET ..."
aws s3 sync dashboard/ "s3://$BUCKET" \
  --exclude ".DS_Store" \
  --cache-control "max-age=300"

echo "Invalidating CloudFront distribution $DIST_ID ..."
aws cloudfront create-invalidation \
  --distribution-id "$DIST_ID" \
  --paths "/*" \
  --region us-east-1

echo "Done."
