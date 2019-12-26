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
- An `AmazonAthenaPreviewFunctionality` workgroup as per the [Amazon Athena Hive Metastore](https://aws.amazon.com/blogs/big-data/connect-amazon-athena-to-your-apache-hive-metastore-and-use-user-defined-functions/) blog post.
- Lambda function created and registered with Athena as instructed in [Connecting Athena to an Apache Hive Metastore](https://docs.aws.amazon.com/athena/latest/ug/connect-to-data-source-hive.html)
- IAM role for the Lambda function to access Glue
- Cross-account execution access granted to Lambda function

Follow the steps below, replacing the variables as necessary. You can also use the the [crossaccountathena.cf.yaml](crossaccountathena.cf.yaml) CloudFormation template to create the IAM role and Lambda function, but you'll need to perform the [Grant Cross-account Access to Lambda](#grant-cross-account-access-to-lambda) step manually.

### Register account for preview

Follow the instructions on the [Amazon Athena Hive Metastore](https://aws.amazon.com/blogs/big-data/connect-amazon-athena-to-your-apache-hive-metastore-and-use-user-defined-functions/) blog post and create a workgroup with the name `AmazonAthenaPreviewFunctionality`.

### Create IAM Role

For now we'll create a role that just uses the default managed Glue Service role and Lambda Execution role.

Best practice would be to only give this role the minimum permissions necessary.

```shell
export ROLE_NAME=AthenaCrossAccountExecution


aws iam create-role --role-name $ROLE_NAME \
  --assume-role-policy-document '{ "Version": "2012-10-17", "Statement": [ { "Effect": "Allow", "Principal": { "Service": "lambda.amazonaws.com" }, "Action": "sts:AssumeRole" } ] }'

aws iam attach-role-policy --role-name $ROLE_NAME \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

aws iam attach-role-policy --role-name $ROLE_NAME \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole
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
export GLUE_ACCOUNT_ID=<cross account id where Glue Data Catalog exists>
export ATHENA_ACCOUNT_ID=<current account from which cross account Glue Data Catalog will be queried>
export FUNCTION_NAME=cross-account-athena
export LAMBDA_ROLE=arn:aws:iam::${ATHENA_ACCOUNT_ID}:role/${ROLE_NAME}


aws lambda create-function \
  --function-name ${FUNCTION_NAME} \
  --runtime python3.7 \
  --role ${LAMBDA_ROLE} \
  --environment Variables={TARGET_ACCOUNT_ID=${GLUE_ACCOUNT_ID}} \
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
- Read only: The currently implementation only implements the necessary functions for read only access as we assume the centralized data catalog is managed by a central team as well.
