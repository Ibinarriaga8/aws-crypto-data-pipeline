#!/usr/bin/env bash
# deploy.sh – Deploy the AWS Crypto Data Pipeline via CloudFormation.
#
# Usage:
#   ./infrastructure/deploy.sh [--env <dev|staging|prod>] [--region <aws-region>]
#
# Prerequisites:
#   - AWS CLI configured with appropriate credentials
#   - S3 bucket to upload templates and Lambda packages (set TEMPLATES_BUCKET env var)

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
ENVIRONMENT="${ENVIRONMENT:-dev}"
AWS_REGION="${AWS_REGION:-us-east-1}"
PROJECT_NAME="${PROJECT_NAME:-crypto-pipeline}"
TEMPLATES_BUCKET="${TEMPLATES_BUCKET:-}"
LAMBDA_CODE_BUCKET="${LAMBDA_CODE_BUCKET:-${TEMPLATES_BUCKET}}"
ALERT_EMAIL="${ALERT_EMAIL:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── Argument parsing ───────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --env)        ENVIRONMENT="$2"; shift 2 ;;
    --region)     AWS_REGION="$2"; shift 2 ;;
    --bucket)     TEMPLATES_BUCKET="$2"; LAMBDA_CODE_BUCKET="$2"; shift 2 ;;
    --alert-email) ALERT_EMAIL="$2"; shift 2 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "${TEMPLATES_BUCKET}" ]]; then
  echo "ERROR: TEMPLATES_BUCKET is required. Set via env var or --bucket flag." >&2
  exit 1
fi

echo "=== Deploying ${PROJECT_NAME} to ${ENVIRONMENT} (region: ${AWS_REGION}) ==="

# ── Upload templates and scripts ───────────────────────────────────────────────
echo "Uploading CloudFormation templates..."
aws s3 sync "${SCRIPT_DIR}/cloudformation/" \
  "s3://${TEMPLATES_BUCKET}/infrastructure/cloudformation/" \
  --region "${AWS_REGION}"

echo "Uploading Glue scripts..."
aws s3 sync "${REPO_ROOT}/glue/" \
  "s3://${LAMBDA_CODE_BUCKET}/glue/" \
  --region "${AWS_REGION}"

# ── Package Lambda functions ───────────────────────────────────────────────────
echo "Packaging Lambda functions..."

package_lambda() {
  local function_dir="$1"
  local zip_key="$2"
  local tmp_dir
  tmp_dir="$(mktemp -d)"
  cp -r "${function_dir}/." "${tmp_dir}/"
  cp -r "${REPO_ROOT}/src" "${tmp_dir}/"
  pip install -r "${function_dir}/requirements.txt" -t "${tmp_dir}/" -q
  (cd "${tmp_dir}" && zip -r /tmp/lambda_package.zip . -q)
  aws s3 cp /tmp/lambda_package.zip \
    "s3://${LAMBDA_CODE_BUCKET}/${zip_key}" \
    --region "${AWS_REGION}"
  rm -rf "${tmp_dir}" /tmp/lambda_package.zip
  echo "  Uploaded ${zip_key}"
}

package_lambda "${REPO_ROOT}/lambda/crypto_ingestion"  "lambda/crypto_ingestion.zip"
package_lambda "${REPO_ROOT}/lambda/stream_processor"  "lambda/stream_processor.zip"

# ── Deploy main CloudFormation stack ──────────────────────────────────────────
STACK_NAME="${PROJECT_NAME}-${ENVIRONMENT}"

echo "Deploying CloudFormation stack: ${STACK_NAME}..."
aws cloudformation deploy \
  --stack-name "${STACK_NAME}" \
  --template-file "${SCRIPT_DIR}/cloudformation/main.yaml" \
  --parameter-overrides \
    Environment="${ENVIRONMENT}" \
    ProjectName="${PROJECT_NAME}" \
    TemplatesBucket="${TEMPLATES_BUCKET}" \
    LambdaCodeBucket="${LAMBDA_CODE_BUCKET}" \
    AlertEmail="${ALERT_EMAIL}" \
  --capabilities CAPABILITY_NAMED_IAM \
  --region "${AWS_REGION}" \
  --no-fail-on-empty-changeset

echo ""
echo "=== Deployment complete ==="
aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}" \
  --region "${AWS_REGION}" \
  --query "Stacks[0].Outputs" \
  --output table
