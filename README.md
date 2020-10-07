# Amazon Athena Cross Account Data Catalog

🌉 Reference implementation for granting cross-account AWS Glue Data Catalog access from Amazon Athena

-------------------------------------

**WARNING**

This is one of many ways to be able to access cross account data catalogs that currently exist. Please evaluate all your options to see which one fits best for your use case.

-------------------------------------

## Overview

This package uses the Athena External Hive Metastore functionality to allow cross-account data access.

It is implemented as an AWS Lambda function that reads the metadata from the Glue Data Catalog and converts it into
the format necessary for Athena.

Users must have access to the S3 resources that the tables point to and be granted access to execute the Lambda function.

## Deployment

Usage of this package requires the following:
- Lambda function created and registered with Athena as instructed in [Connecting Athena to an Apache Hive Metastore](https://docs.aws.amazon.com/athena/latest/ug/connect-to-data-source-hive.html)
- IAM role for the Lambda function to access Glue
- Cross-account execution access granted to Lambda function

Follow the steps below, replacing the variables as necessary. You can also use the the [crossaccountathena.cf.yaml](crossaccountathena.cf.yaml) CloudFormation template to create the IAM role and Lambda function, but you'll need to perform the [Grant Cross-account Access to Lambda](#grant-cross-account-access-to-lambda) step manually.

For CloudFormation, download the [function2.zip](target/function2.zip) and upload it to your S3 bucket as it needs to be provided in the CloudFormation template. Then, while launching the [CFN stack](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#/stacks/create/review?templateURL=https://aws-bigdata-blog.s3.amazonaws.com/artifacts/aws-blog-cross-account-athena/cross_account_athena_stack.yaml), specify this S3 bucket, key path, source AWS account ID and Athena catalog name. It'll create the Lambda execution role, function and Athena Catalog.


### Create IAM Role

For now, we'll create a role that has trust relationship with Lambda service and attaches couple of basic policies to grant necesary Glue, S3 and CloudWatch logging permissions. Best practice would be to only give this role the minimum permissions necessary.

```shell
export ROLE_NAME=AthenaCrossAccountExecution
export ATHENA_ACCOUNT_ID="<requester_account>"

aws iam create-role --role-name $ROLE_NAME \
  --assume-role-policy-document '
  { 
    "Version": "2012-10-17",
    "Statement": [ 
    { 
      "Effect": "Allow",
      "Principal": { 
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
    ]
  }'

aws iam create-policy --policy-name AWSGlueReadOnlyAndS3SpillAccess --policy-document '
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "glue:BatchGetPartition",
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:GetPartition",
                "glue:GetPartitions",
                "glue:GetTable",
                "glue:GetTables",
                "glue:GetTableVersion",
                "glue:GetTableVersions"
            ],
            "Resource": "*",
            "Effect": "Allow"
        },
        {
            "Action": [
                "s3:PutObject",
                "s3:GetObject"
            ],
            "Resource": "arn:aws:s3:::<your-spill-bucket>/<prefix>/*",
            "Effect": "Allow"
        }
    ]
}'

aws iam create-policy --policy-name AWSLambdaBasicExecutionRole --policy-document '
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents"
            ],
            "Resource": "*",
            "Effect": "Allow"
        }
    ]
}'

aws iam attach-role-policy --role-name $ROLE_NAME \
  --policy-arn arn:aws:iam::$ATHENA_ACCOUNT_ID:policy/AWSGlueReadOnlyAndS3SpillAccess

aws iam attach-role-policy --role-name $ROLE_NAME \
  --policy-arn arn:aws:iam::$ATHENA_ACCOUNT_ID:policy/AWSLambdaBasicExecutionRole
```

### Build deployment package

You must be using Python3 - I tend to use a virtualenv.

```shell
virtualenv -p python3 venv
source venv/bin/activate
pip install -r dev_requirements.txt
```

```shell
make build
```

This creates a zip file in `target/` that can be uploaded to your Lambda function.

### Create function

Create a new function in the account where you will be running your Athena queries.

```shell
export GLUE_ACCOUNT_ID="<cross account id where Glue Data Catalog exists>"
export ATHENA_CATALOG_NAME="<catalog_name>"
export GLUE_CATALOG_REGION="<region-id>"
export S3_SPILL_LOCATION = "s3://<bucket>/<prefix>"
expoet S3_SPILL_TTL = 3600   # Spilled content will be valid for 1 hour (60x60 seconds)
export FUNCTION_NAME="<your-desirect-function-name>"
export LAMBDA_ROLE="arn:aws:iam::${ATHENA_ACCOUNT_ID}:role/${ROLE_NAME}"


aws lambda create-function \
  --function-name ${FUNCTION_NAME} \
  --runtime python3.7 \
  --role ${LAMBDA_ROLE} \
  --environment Variables="{CATALOG_NAME=${ATHENA_CATALOG_NAME},TARGET_ACCOUNT_ID=${GLUE_ACCOUNT_ID},CATALOG_REGION=${GLUE_CATALOG_REGION},SPILL_LOCATION=${S3_SPILL_LOCATION},SPILL_TTL=${S3_SPILL_TTL}}" \
  --zip-file fileb://target/functionv2.zip \
  --handler "heracles.lambda.handler" \
  --memory-size 256 \
  --timeout 60
```

### Register Data Source with Athena

Follow the instructions in the [Hive Metastore blog post](https://aws.amazon.com/blogs/big-data/connect-amazon-athena-to-your-apache-hive-metastore-and-use-user-defined-functions/) to create a workgroup to access the preview functionality, then follow the instructions for [Connecting Athena to an Apache Hive Metastore](https://docs.aws.amazon.com/athena/latest/ug/connect-to-data-source-hive.html). On the **Connection details** page, for **Lambda function**, select the Lambda function that was created above. Name your catalog "crossaccountcatalog".

### Grant Cross-account Access to Lambda

Finally we need to [grant cross-account access](https://docs.aws.amazon.com/glue/latest/dg/cross-account-access.html) using a resource policy. 

This command allows the Lamba function you created in the first account to read any Glue Data Catalog database or table in second account. Run this command in second account where Glue Data Catalog exists.

```shell
aws glue put-resource-policy --policy-in-json '{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetPartition",
        "glue:GetPartitions",
        "glue:GetTable",
        "glue:GetTables"
      ],
      "Principal": {"AWS": [
        "arn:aws:sts::'${ATHENA_ACCOUNT_ID}':assumed-role/'${ROLE_NAME}'/'${FUNCTION_NAME}'"
      ]},
      "Resource": [
        "arn:aws:glue:us-east-1:'${GLUE_ACCOUNT_ID}':catalog",
        "arn:aws:glue:us-east-1:'${GLUE_ACCOUNT_ID}':database/*",
        "arn:aws:glue:us-east-1:'${GLUE_ACCOUNT_ID}':table/*/*"
      ]
    }
  ]
}'
```

### Test it...

- You can invoke the Lambda function directly to test if the output matches as expected.

```shell
aws lambda invoke --function-name ${FUNCTION_NAME} \
  --log-type Tail \
  --payload '{"apiName":"getAllDatabases","apiRequest":{}}' \
  outfile.txt
```

```shell
aws lambda invoke --function-name ${FUNCTION_NAME} \
  --log-type Tail \
  --payload '{"apiName":"getTable","apiRequest":{"dbName":"db", "tableName": "table"}}' \
  getTable.txt
```

- You can also invoke Athena commands from the CLI

```shell
aws athena start-query-execution --query-string "select * from <CATALOG_NAME>.<DATABASE_NAME>.<TABLE_NAME>" \
  --query-execution-context "Database=<CATALOG_NAME>.<DATABASE_NAME>" \
  --result-configuration "OutputLocation=s3://<bucket>/tmp/athenalambda/results" \
  --work-group AmazonAthenaPreviewFunctionality
```

```shell
aws athena get-query-results --query-execution-id <QUERY_ID>
```

- You must include the catalog name when running queries from the Athena console

```sql
SELECT * FROM <CATALOG>.<DATABASE>.<TABLE> limit 10
```

### Update it

If you want to update the code, do so, run `make build` and then update the Lambda function.

```shell
aws lambda update-function-code --function-name ${FUNCTION_NAME} --zip-file fileb://target/functionv2.zip
```

## Limitations
Read only: The currently implementation only implements the necessary functions for read only access as we assume the centralized data catalog is managed by a central team as well.

## Additional Considerations
- For tables containing large number of partitions, the Glue response size can be very large and can exceed Lambda payload response size limit. To address this, the response will be spilled to S3 and Athena will fetch it from there. Additionally, if you prefer to reuse the spilled content, you could also set Spill TTL to longer duration (default 0). With this, the subsequent queries will get the metadata information from spilled content, thus optimizing query time performance.
- Do not set Spill TTL to higher value if your tables get modified often to add/remove partitions. In that case, the queries may not get the latest partition information. So, set this value based on your unique use case. If not sure, set it to very low value and gradually increase it till you get your ideal results.
- Similarly, if you're setting higher spill TTL and rarely add/remove partitions, you could also invalidate the spilled content either by deleting them from S3 location or by temporarily setting spill TTL to lower value (Lambda Environament Variable: SPILL_TTL).