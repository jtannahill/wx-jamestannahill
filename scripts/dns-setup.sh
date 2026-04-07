#!/bin/bash
# scripts/dns-setup.sh
# DNS and CloudFront custom domain setup for wx.jamestannahill.com
# Run once after ACM cert is validated.
#
# ACM Cert ARN: arn:aws:acm:us-east-1:216890068001:certificate/dd33b0b7-ce5b-40d9-b4f1-ce957383ed2b
# Status: PENDING_VALIDATION (add CNAMEs below to Cloudflare, then re-run)
#
# Step 1: Add these DNS validation CNAMEs to Cloudflare (jamestannahill.com zone, DNS-only):
#
#   _dff08477881993a7ec63b3b926eda4c7.wx.jamestannahill.com
#     -> _dbe7cc1d9c7f312a48fc018dc9c4189a.jkddzztszm.acm-validations.aws
#
#   _4224a27b72078ec6eb99a6eedb0013bf.api.wx.jamestannahill.com
#     -> _e2af2e1754d126940464d92aa8fe00ed.jkddzztszm.acm-validations.aws
#
# Step 2: Add site CNAMEs to Cloudflare (DNS-only, proxied=false):
#
#   wx.jamestannahill.com        -> dtekmqlot1bhf.cloudfront.net
#   api.wx.jamestannahill.com    -> d2d2b3fftwrbn2.cloudfront.net
#
# Step 3: After cert shows ISSUED, run this script to attach it to CloudFront distributions.

set -e

CERT_ARN="arn:aws:acm:us-east-1:216890068001:certificate/dd33b0b7-ce5b-40d9-b4f1-ce957383ed2b"
DASHBOARD_DIST="E2OIRPWQ2L8LB6"
API_DIST_DOMAIN="d2d2b3fftwrbn2.cloudfront.net"

# Check cert status
STATUS=$(aws acm describe-certificate \
  --certificate-arn "$CERT_ARN" \
  --region us-east-1 \
  --query "Certificate.Status" \
  --output text)

echo "Cert status: $STATUS"

if [ "$STATUS" != "ISSUED" ]; then
  echo "Cert not yet validated. Add the DNS CNAMEs listed above to Cloudflare first."
  exit 1
fi

# Update dashboard CloudFront distribution with custom domain + cert
ETAG=$(aws cloudfront get-distribution-config --id "$DASHBOARD_DIST" --region us-east-1 --query 'ETag' --output text)
aws cloudfront get-distribution-config --id "$DASHBOARD_DIST" --region us-east-1 \
  --query 'DistributionConfig' > /tmp/dash-config.json

# Patch aliases and cert into config
python3 - <<'PYEOF'
import json

with open('/tmp/dash-config.json') as f:
    config = json.load(f)

config['Aliases'] = {'Quantity': 1, 'Items': ['wx.jamestannahill.com']}
config['ViewerCertificate'] = {
    'ACMCertificateArn': 'arn:aws:acm:us-east-1:216890068001:certificate/dd33b0b7-ce5b-40d9-b4f1-ce957383ed2b',
    'SSLSupportMethod': 'sni-only',
    'MinimumProtocolVersion': 'TLSv1.2_2021',
    'CertificateSource': 'acm'
}
if 'CloudFrontDefaultCertificate' in config.get('ViewerCertificate', {}):
    del config['ViewerCertificate']['CloudFrontDefaultCertificate']

with open('/tmp/dash-config-patched.json', 'w') as f:
    json.dump(config, f)

print("Config patched.")
PYEOF

ETAG=$(aws cloudfront get-distribution-config --id "$DASHBOARD_DIST" --region us-east-1 --query 'ETag' --output text)
aws cloudfront update-distribution \
  --id "$DASHBOARD_DIST" \
  --distribution-config "file:///tmp/dash-config-patched.json" \
  --if-match "$ETAG" \
  --region us-east-1

echo "Dashboard distribution updated: wx.jamestannahill.com -> dtekmqlot1bhf.cloudfront.net"
echo ""
echo "NOTE: The API distribution (d2d2b3fftwrbn2.cloudfront.net) must be updated"
echo "via CDK (wx_stack.py) — add aliases + cert to the API CloudFront resource."
echo "Dashboard is live at https://wx.jamestannahill.com once DNS propagates."
