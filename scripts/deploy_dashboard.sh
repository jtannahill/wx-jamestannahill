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
# HTML / JS / CSS: short TTL + must-revalidate so fixes show up quickly
# (browser asks the CDN for an etag check on each load — cheap 304 if unchanged).
aws s3 sync dashboard/ "s3://$BUCKET" \
  --exclude ".DS_Store" \
  --exclude "*.html" --exclude "*.js" --exclude "*.css" \
  --cache-control "max-age=300"
aws s3 sync dashboard/ "s3://$BUCKET" \
  --exclude "*" \
  --include "*.html" --include "*.js" --include "*.css" \
  --cache-control "public, max-age=60, must-revalidate"

echo "Invalidating CloudFront distribution $DIST_ID ..."
aws cloudfront create-invalidation \
  --distribution-id "$DIST_ID" \
  --paths "/*" \
  --region us-east-1

echo "Done."
