# AWS Deployment & Configuration Guide

**Informatica PowerCenter → Databricks Migration Tool**

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Prerequisites](#2-prerequisites)
3. [Step 1 — AWS Infrastructure Setup](#3-step-1--aws-infrastructure-setup)
4. [Step 2 — Secrets Manager Configuration](#4-step-2--secrets-manager-configuration)
5. [Step 3 — IAM Roles & Policies](#5-step-3--iam-roles--policies)
6. [Step 4 — EFS File Systems](#6-step-4--efs-file-systems)
7. [Step 5 — Build & Push Docker Image to ECR](#7-step-5--build--push-docker-image-to-ecr)
8. [Step 6 — ECS Fargate Deployment](#8-step-6--ecs-fargate-deployment)
9. [Step 7 — Databricks Asset Bundle Deployment](#9-step-7--databricks-asset-bundle-deployment)
10. [Step 8 — Databricks Secret Scope Configuration](#10-step-8--databricks-secret-scope-configuration)
11. [Running a Migration](#11-running-a-migration)
12. [Environment Variables Reference](#12-environment-variables-reference)
13. [Monitoring & Logs](#13-monitoring--logs)
14. [Troubleshooting](#14-troubleshooting)

---

## 1. Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│  AWS GovCloud (us-gov-west-1)                               │
│                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐  │
│  │     EFS      │    │  ECS Fargate │    │     ECR      │  │
│  │   /input     │───▶│  Container   │◀───│  Docker img  │  │
│  │   /output    │◀───│  (migration) │    │              │  │
│  └──────────────┘    └──────┬───────┘    └──────────────┘  │
│                             │                               │
│  ┌──────────────┐           │ reads secrets                 │
│  │  Secrets     │◀──────────┘                               │
│  │  Manager     │                                           │
│  └──────────────┘    ┌──────────────┐                       │
│                      │  CloudWatch  │                       │
│                      │    Logs      │                       │
│                      └──────────────┘                       │
└─────────────────────────────────────────────────────────────┘
                              │
                              │ HTTPS (Databricks Jobs API)
                              ▼
                   ┌─────────────────────┐
                   │  Databricks Workspace│
                   │  Bronze / Silver /   │
                   │  Gold notebooks      │
                   │  (DAB deployed)      │
                   └─────────────────────┘
```

The tool runs as an **ECS Fargate task** (non-root, read-only filesystem, FedRAMP-compliant). It reads Informatica PowerMart XML exports from EFS, generates PySpark notebooks, and deploys them to Databricks via the Asset Bundle CLI.

---

## 2. Prerequisites

| Tool | Minimum Version | Purpose |
|------|----------------|---------|
| AWS CLI | v2.x | ECR login, ECS, Secrets Manager |
| Docker | 24.x | Build and push image |
| Databricks CLI | 0.18+ | Deploy notebooks via DAB |
| Python | 3.10+ | Local runs / test |
| `jq` | 1.6+ | JSON manipulation in scripts |

Ensure your AWS credentials are configured and have access to GovCloud:

```bash
aws configure --profile govcloud
export AWS_PROFILE=govcloud
aws sts get-caller-identity   # should return your account ID
```

---

## 3. Step 1 — AWS Infrastructure Setup

All commands use `us-gov-west-1`. Adjust `ACCOUNT_ID` and `VPC_ID` for your environment.

```bash
export AWS_REGION=us-gov-west-1
export ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export VPC_ID=<your-vpc-id>
export SUBNET_IDS=<subnet-id-1,subnet-id-2>   # private subnets
```

### Create CloudWatch Log Group

```bash
aws logs create-log-group \
  --log-group-name /ecs/dbx-migration-tool \
  --region $AWS_REGION

aws logs put-retention-policy \
  --log-group-name /ecs/dbx-migration-tool \
  --retention-in-days 90 \
  --region $AWS_REGION
```

---

## 4. Step 2 — Secrets Manager Configuration

The container pulls two secrets at runtime. Create them before registering the task definition.

```bash
# Databricks workspace URL (e.g. https://adb-<id>.azuredatabricks.net)
aws secretsmanager create-secret \
  --name "cder/dbx-migration-tool/databricks-host" \
  --secret-string "https://<your-databricks-host>" \
  --region $AWS_REGION

# Databricks personal access token (or service principal token)
aws secretsmanager create-secret \
  --name "cder/dbx-migration-tool/databricks-token" \
  --secret-string "<your-databricks-pat>" \
  --region $AWS_REGION
```

> **Note:** To rotate the token, update the secret value and restart the ECS task. The container always fetches the latest version on startup.

---

## 5. Step 3 — IAM Roles & Policies

### 5a. ECS Task Execution Role

This is the standard AWS-managed role that allows ECS to pull from ECR and inject secrets. Create it if it doesn't exist:

```bash
# Trust policy
cat > /tmp/ecs-trust.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "ecs-tasks.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
EOF

aws iam create-role \
  --role-name ecsTaskExecutionRole \
  --assume-role-policy-document file:///tmp/ecs-trust.json

aws iam attach-role-policy \
  --role-name ecsTaskExecutionRole \
  --policy-arn arn:aws-us-gov:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy

# Also grant access to read the two Secrets Manager entries
aws iam put-role-policy \
  --role-name ecsTaskExecutionRole \
  --policy-name SecretManagerAccess \
  --policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Action": ["secretsmanager:GetSecretValue"],
      "Resource": [
        "arn:aws-us-gov:secretsmanager:'$AWS_REGION':'$ACCOUNT_ID':secret:cder/dbx-migration-tool/*"
      ]
    }]
  }'
```

### 5b. ECS Task Role (application permissions)

This is the role the container itself uses at runtime (EFS, CloudWatch, ECR pull):

```bash
aws iam create-role \
  --role-name dbx-migration-tool-task-role \
  --assume-role-policy-document file:///tmp/ecs-trust.json

# Apply the policy from the repo (substitute real EFS IDs after Step 4)
sed \
  -e "s/ACCOUNT_ID/$ACCOUNT_ID/g" \
  -e "s/fs-INPUT_EFS_ID/$INPUT_EFS_ID/g" \
  -e "s/fs-OUTPUT_EFS_ID/$OUTPUT_EFS_ID/g" \
  deploy/ecs/iam-task-role-policy.json > /tmp/task-role-policy.json

aws iam put-role-policy \
  --role-name dbx-migration-tool-task-role \
  --policy-name MigrationToolPolicy \
  --policy-document file:///tmp/task-role-policy.json
```

---

## 6. Step 4 — EFS File Systems

Two EFS file systems are required: one for input XML files (read-only to container) and one for generated notebooks (writable).

```bash
# Input EFS
INPUT_EFS_ID=$(aws efs create-file-system \
  --encrypted \
  --performance-mode generalPurpose \
  --throughput-mode bursting \
  --tags Key=Name,Value=dbx-migration-input Key=Project,Value=dbx-migration-tool \
  --region $AWS_REGION \
  --query 'FileSystemId' --output text)

# Output EFS
OUTPUT_EFS_ID=$(aws efs create-file-system \
  --encrypted \
  --performance-mode generalPurpose \
  --throughput-mode bursting \
  --tags Key=Name,Value=dbx-migration-output Key=Project,Value=dbx-migration-tool \
  --region $AWS_REGION \
  --query 'FileSystemId' --output text)

echo "Input EFS:  $INPUT_EFS_ID"
echo "Output EFS: $OUTPUT_EFS_ID"
```

### Create mount targets in each private subnet

```bash
for SUBNET in $(echo $SUBNET_IDS | tr ',' ' '); do
  aws efs create-mount-target \
    --file-system-id $INPUT_EFS_ID \
    --subnet-id $SUBNET \
    --security-groups <efs-sg-id> \
    --region $AWS_REGION

  aws efs create-mount-target \
    --file-system-id $OUTPUT_EFS_ID \
    --subnet-id $SUBNET \
    --security-groups <efs-sg-id> \
    --region $AWS_REGION
done
```

### Create directory structure on EFS

Mount temporarily from a bastion or EC2 instance to create the root directories:

```bash
sudo mount -t efs $INPUT_EFS_ID:/ /mnt/efs-input
sudo mkdir -p /mnt/efs-input/migration/input
sudo umount /mnt/efs-input

sudo mount -t efs $OUTPUT_EFS_ID:/ /mnt/efs-output
sudo mkdir -p /mnt/efs-output/migration/output
sudo umount /mnt/efs-output
```

> Place your Informatica PowerMart XML exports in `/migration/input/` on the input EFS before running the container.

---

## 7. Step 5 — Build & Push Docker Image to ECR

```bash
# Make the script executable
chmod +x deploy/scripts/build_and_push.sh

# Build and push (tag defaults to current git SHA)
./deploy/scripts/build_and_push.sh

# Or with an explicit tag
./deploy/scripts/build_and_push.sh v1.0.0
```

The script automatically:
- Creates the ECR repository with scan-on-push enabled if it doesn't exist
- Authenticates Docker to ECR
- Builds `linux/amd64` image targeting the `runtime` stage
- Pushes both the versioned tag and `latest`

---

## 8. Step 6 — ECS Fargate Deployment

### 8a. Register the task definition

Substitute real values into the template, then register it:

```bash
sed \
  -e "s/ACCOUNT_ID/$ACCOUNT_ID/g" \
  -e "s/fs-INPUT_EFS_ID/$INPUT_EFS_ID/g" \
  -e "s/fs-OUTPUT_EFS_ID/$OUTPUT_EFS_ID/g" \
  deploy/ecs/task-definition.json > /tmp/task-def-resolved.json

aws ecs register-task-definition \
  --cli-input-json file:///tmp/task-def-resolved.json \
  --region $AWS_REGION
```

### 8b. Create an ECS cluster (if not already exists)

```bash
aws ecs create-cluster \
  --cluster-name dbx-migration-cluster \
  --capacity-providers FARGATE \
  --region $AWS_REGION
```

### 8c. Run the migration task

The tool is designed as a **run-once task** (not a long-running service). Trigger it manually or via EventBridge:

```bash
aws ecs run-task \
  --cluster dbx-migration-cluster \
  --task-definition dbx-migration-tool \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={
    subnets=[$SUBNET_IDS],
    securityGroups=[<task-sg-id>],
    assignPublicIp=DISABLED
  }" \
  --overrides '{
    "containerOverrides": [{
      "name": "dbx-migration-tool",
      "command": ["--input", "/app/input", "--output", "/app/output"]
    }]
  }' \
  --region $AWS_REGION
```

### 8d. Security group requirements

The ECS task security group needs:

| Direction | Port | Protocol | Destination | Purpose |
|-----------|------|----------|-------------|---------|
| Outbound | 2049 | TCP | EFS SG | EFS mount |
| Outbound | 443 | TCP | 0.0.0.0/0 | ECR, Secrets Manager, Databricks API |
| Outbound | 443 | TCP | VPC endpoints | (if using VPC endpoints instead) |
| Inbound | — | — | None | No inbound needed |

---

## 9. Step 7 — Databricks Asset Bundle Deployment

The `databricks.yml` at the repo root defines three targets: `dev`, `staging`, and `prod`.

### 9a. Configure Databricks CLI

```bash
databricks configure --token
# Enter your Databricks host and PAT when prompted
# Or use environment variables:
export DATABRICKS_HOST=https://<your-workspace>.azuredatabricks.net
export DATABRICKS_TOKEN=<your-pat>
```

### 9b. Deploy notebooks and jobs

```bash
# Deploy to dev (default target)
databricks bundle deploy

# Deploy to staging
databricks bundle deploy --target staging

# Deploy to prod
databricks bundle deploy --target prod
```

This deploys all three notebooks (`bronze`, `silver`, `gold`) and the `migration_orchestrator` job to the Databricks workspace.

### 9c. Override variables at deploy time

```bash
databricks bundle deploy --target prod \
  --var catalog=cder_prod \
  --var secret_scope=cder-secrets \
  --var node_type=Standard_DS4_v2
```

### 9d. Run the Databricks job

```bash
# Trigger a one-off run
databricks bundle run migration_orchestrator

# Or via the Jobs API directly
databricks jobs run-now --job-id <JOB_ID>
```

---

## 10. Step 8 — Databricks Secret Scope Configuration

The notebooks authenticate to Oracle via Databricks secret scopes (not environment variables).

### Create the secret scope

```bash
# Backed by AWS Secrets Manager (recommended for GovCloud)
databricks secrets create-scope cder-secrets \
  --scope-backend-type DATABRICKS

# Store Oracle credentials
databricks secrets put-secret cder-secrets oracle-jdbc-url \
  --string-value "jdbc:oracle:thin:@//<host>:<port>/<service>"

databricks secrets put-secret cder-secrets oracle-user \
  --string-value "<oracle-username>"

databricks secrets put-secret cder-secrets oracle-password \
  --string-value "<oracle-password>"
```

### Grant access to the job cluster principal

```bash
databricks secrets put-acl cder-secrets \
  "<service-principal-or-group>" READ
```

---

## 11. Running a Migration

### End-to-end flow

1. **Export** your Informatica mapping(s) as PowerMart XML from the Informatica Designer.
2. **Upload** the XML file(s) to the input EFS:
   ```bash
   # From a bastion with EFS mounted
   cp my_mapping.xml /mnt/efs-input/migration/input/
   ```
3. **Run the ECS task** (Step 8c above) — the container parses the XML, converts transformations, and writes PySpark notebooks to `/app/output` (EFS output).
4. **Retrieve generated notebooks** from the output EFS, review them, then commit to your repo.
5. **Deploy the notebooks** to Databricks via `databricks bundle deploy`.
6. **Trigger the Databricks job** to execute the Bronze → Silver → Gold pipeline.

### Running locally (for development)

```bash
# Install dependencies
pip install -r requirements.txt

# Set credentials
export DATABRICKS_HOST=https://<workspace>.azuredatabricks.net
export DATABRICKS_TOKEN=<pat>

# Run the parser on a local XML file
python -m src.migration.parser --input path/to/export.xml

# Run tests
python -m pytest tests/ -v
```

---

## 12. Environment Variables Reference

### ECS Container

| Variable | Default | Description |
|----------|---------|-------------|
| `AWS_DEFAULT_REGION` | `us-gov-west-1` | AWS region |
| `DBX_CATALOG` | `cder_prod` | Unity Catalog catalog name |
| `DBX_SECRET_SCOPE` | `cder-secrets` | Databricks secret scope for Oracle credentials |
| `LOG_LEVEL` | `INFO` | Python logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `PYTHONUNBUFFERED` | `1` | Flush stdout/stderr immediately (required for CloudWatch) |
| `DATABRICKS_HOST` | *(from Secrets Manager)* | Databricks workspace URL |
| `DATABRICKS_TOKEN` | *(from Secrets Manager)* | Databricks PAT or service principal token |

### Databricks Bundle Variables (`databricks.yml`)

| Variable | Default | Description |
|----------|---------|-------------|
| `catalog` | `cder_prod` | Unity Catalog catalog |
| `bronze_schema` | `bronze` | Bronze layer schema |
| `silver_schema` | `silver` | Silver layer schema |
| `gold_schema` | `gold` | Gold layer schema |
| `secret_scope` | `cder-secrets` | Databricks secret scope name |
| `environment` | `dev` | Target environment label |
| `node_type` | `Standard_DS3_v2` | Cluster node type |
| `spark_version` | `14.3.x-scala2.12` | Databricks Runtime version |

---

## 13. Monitoring & Logs

### CloudWatch Logs

All container stdout/stderr is shipped to:

```
Log group:  /ecs/dbx-migration-tool
Log stream: ecs/dbx-migration-tool/<task-id>
```

View the latest logs:

```bash
aws logs tail /ecs/dbx-migration-tool --follow --region $AWS_REGION
```

### ECS Task Status

```bash
# List recent task runs
aws ecs list-tasks \
  --cluster dbx-migration-cluster \
  --region $AWS_REGION

# Describe a specific task
aws ecs describe-tasks \
  --cluster dbx-migration-cluster \
  --tasks <task-arn> \
  --region $AWS_REGION
```

### Databricks Job Runs

```bash
# List recent runs
databricks runs list --job-id <JOB_ID> --limit 10

# Get run output
databricks runs get-output --run-id <RUN_ID>
```

---

## 14. Troubleshooting

### Container fails to start — `CannotPullContainerError`

- Verify the ECS task execution role has ECR pull permissions.
- Confirm the image URI in the task definition matches the ECR repository.
- Check that ECR is accessible from the VPC (via VPC endpoint or NAT gateway).

### `SecretNotFound` on startup

- Confirm secret ARNs in `task-definition.json` match the names created in Step 2.
- Ensure `ACCOUNT_ID` was substituted correctly before registering the task definition.
- The task execution role must have `secretsmanager:GetSecretValue` on those ARNs.

### EFS mount failure — task stops immediately

- Verify EFS mount targets exist in the same subnets as the ECS task.
- The task security group must allow outbound TCP 2049 to the EFS security group.
- The EFS security group must allow inbound TCP 2049 from the task security group.

### `ParseError: XML or text declaration not at start of entity`

- The input XML file may have been saved with a BOM or leading whitespace.
- Re-export from Informatica Designer as UTF-8 without BOM, or strip it:
  ```bash
  sed -i '1s/^\xEF\xBB\xBF//' my_mapping.xml
  ```

### Databricks bundle deploy fails — `RESOURCE_DOES_NOT_EXIST`

- Run `databricks bundle validate` first to catch config errors.
- Confirm `DATABRICKS_HOST` and `DATABRICKS_TOKEN` are set and valid.
- The workspace user/service principal must have `CAN_MANAGE` on the target folder.

### Oracle JDBC connection timeout in Databricks notebook

- Confirm the secret scope entries (`oracle-jdbc-url`, `oracle-user`, `oracle-password`) are correct.
- Check that Databricks cluster network policy allows outbound TCP to the Oracle host/port.
- For VPN-gated Oracle, ensure the Databricks cluster VPC is peered or connected appropriately.
