# Redshift to BigQuery Migrator

## AWS User

The provided AWS IAM user must have at least the following permissions in order to properly function:
 - AmazonRedshiftDataFullAccess
 - AmazonRedshiftQueryEditor
 - AmazonRedshiftReadOnlyAccess
 - AmazonS3FullAccess

##  Deploying on GCP

Ideally, you should deploy this to Google Cloud Platform as a function and run it there. I've included the file `deploy.sh` for this purpose. Just run it and after a minute it should create a new function on your GCP project. See the following example:

```
PROJECT_ID=test-import-369954 FUNCTION_NAME=redshift-to-bigquery-migrator ./deploy.sh
```

Explanation: this will create a function named `redshift-to-bigquery-migrator` under the project `test-import-369954` using whatever account is currently setup on your GCP CLI installation.

**Important**: Again, if you are using Windows, this command may be slightly different for you. Please feel free to ask any questions.

You should also have GCP CLI already configured. And a Secret with the name `redshift_to_bigquery_migration_env` uploaded under the same project you pretend to deploy this function and a credentials key file with the name `gcp_credentials`.

### Example .env file

```
AWS_REGION="sa-east-1"
AWS_ACCESS_KEY_ID="AIUFGIEUHFHFAHUHAF"
AWS_SECRET_ACCESS_KEY="8SF8FNS8UNE8FN8N8NEN8UFUN8EUN8S8UFU8U8"
AWS_S3_BUCKET_NAME="migration-staging-redshift"
AWS_S3_OUTPUT_DIR="unload_test"
AWS_IAM_ROLE="arn:aws:iam::123456789:role/test-redshift-role"
AWS_REDSHIFT_DB="dev"
AWS_REDSHIFT_SCHEMA="public"
AWS_REDSHIFT_TABLES="category,date,event,listing,sales,users,venue"
AWS_REDSHIFT_CLUSTER="redshift-cluster-1"
AWS_REDSHIFT_DB_USER="awsuser"

GCP_BIGQUERY_DESTINATION_DATASET="migration_tests"
```

### Example Output

```
[1/3] Unloading tables to S3...
        Will unload 7.
        Done.

[2/3] Getting table schemas...
        -> [1/7] Getting schema for table "category"...
        -> [2/7] Getting schema for table "date"...
        -> [3/7] Getting schema for table "event"...
        -> [4/7] Getting schema for table "listing"...
        -> [5/7] Getting schema for table "sales"...
        -> [6/7] Getting schema for table "users"...
        -> [7/7] Getting schema for table "venue"...
        Done.

[3/3] Uploading tables to BigQuery...
        -> [1/7] Downloading objects from S3 folder "category"...
                Found 2 objects.
                -> [1/2] Downloading unload_test/category/0000_part_00.
                -> [2/2] Downloading unload_test/category/0001_part_00.
                Done.

        Done.

        Uploading to BigQuery...
                -> [1/2] Uploading /redshift-to-bigquery-migrator/tables/category/objects/0000_part_00...
                        [1/2] Reading CSV...
                        [2/2] Uploading...
                        Done.

                -> [2/2] Uploading /redshift-to-bigquery-migrator/tables/category/objects/0001_part_00...
                        [1/2] Reading CSV...
                        [2/2] Uploading...
                        Done.

        Done.

[...]
```

## Running Locally

If you just want to run it locally (e.g. for testing purposes), you can by simply putting the GCP credentials key file in the root directory of this project and renaming it to `credentials.json`. The same goes for the environment variables file, but you must name it env`. After doing the steps on [Installation](#installation), run the following command:

```
python3 main.py
```

### Installation

Run the following commands to install the necessary dependencies:

```
python3 -m venv venv
source venv\bin\activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
```

**Important**: If you are on Windows these commands are slightly different. Please consult the relevant documentation on [how to use venv](https://docs.python.org/3/library/venv.html) if you are unsure.
