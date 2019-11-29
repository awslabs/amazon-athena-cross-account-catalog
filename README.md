# Amazon Athena Cross Account Data Catalog

🌉 Reference implementation for granting cross-account AWS Glue Data Catalog access from Amazon Athena

-------------------------------------

**WARNING**

This is intended to be a proof of concept.

While functional, this may not be the recommended way to enable cross-account Athena queries in the future.

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
export GLUE_ACCOUNT_ID=1111111
export FUNCTION_NAME=cross-account-athena
export LAMBDA_ROLE=arn:aws:iam::${GLUE_ACCOUNT_ID}:role/${ROLE_NAME}


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

### Grant Cross-account Access to Lambda

Finally we need to [grant cross-account access](https://docs.aws.amazon.com/glue/latest/dg/cross-account-access.html) using a resource policy. 

This command allows the Lamba function you created in the first account to read any Glue Data Catalog database or table.

```shell
export ATHENA_ACCOUNT_ID=2222222

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
# ~~~TODO: Fix apiRequest parameters~~~
aws lambda invoke --function-name ${FUNCTION_NAME} \
  --log-type Tail \
  --payload '{"apiName":"getTable","apiRequest":{"databaseName": "db", "tableName": "table"}}' \
  getTable.txt
```

- You can also invoke Athena commands from the CLI

```shell
aws athena start-query-execution --query-string "SHOW tables" \
  --query-execution-context "Database=<CATALOG_NAME>.<DATABASE_NAME>" \
  --result-configuration "OutputLocation=s3://<bucket>/tmp/athenalambda/results"
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

## Other thoughts

### Public access?

```shell
aws lambda add-permission --function-name ${FUNCTION_NAME} --statement-id opendata-public --action "lambda:InvokeFunction" --principal '*'
```

### TODO

We want to make this a little flexible:

- Inclusions/exclusions for databases
- AuthZ?
- Write access

## Limitations
- Read only