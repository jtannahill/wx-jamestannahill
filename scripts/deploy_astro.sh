#!/bin/bash
# scripts/deploy_astro.sh — sync astro/dist/client/ to the WxStack S3 bucket and invalidate CF.
set -e

REPO="$(cd "$(dirname "$0")/.." && pwd)"

BUCKET=$(aws cloudformation describe-stacks --stack-name WxStack \
  --query "Stacks[0].Outputs[?OutputKey=='DashboardBucketName'].OutputValue" \
  --output text --region us-east-1)

DIST_ID=$(aws cloudformation describe-stacks --stack-name WxStack \
  --query "Stacks[0].Outputs[?OutputKey=='DashboardDistributionId'].OutputValue" \
  --output text --region us-east-1)

cd "$REPO/astro"
npm ci
npm run build

echo "Syncing astro/dist/client/ to s3://$BUCKET ..."
aws s3 sync dist/client/ "s3://$BUCKET" \
  --exclude ".DS_Store" \
  --exclude "*.html" --exclude "*.js" --exclude "*.css" \
  --cache-control "max-age=300"
aws s3 sync dist/client/ "s3://$BUCKET" \
  --exclude "*" \
  --include "*.html" --include "*.js" --include "*.css" \
  --cache-control "public, max-age=60, must-revalidate"

echo "Invalidating CloudFront distribution $DIST_ID ..."
aws cloudfront create-invalidation \
  --distribution-id "$DIST_ID" \
  --paths "/*" \
  --region us-east-1

echo "Done."
