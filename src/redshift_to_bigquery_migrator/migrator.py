import boto3, os, shutil, json

from pathlib import Path
from dotenv import load_dotenv

from google.cloud import bigquery

import pandas


pandas.set_option("display.precision", 5)

DIR = Path(os.path.dirname(__file__))

if os.getenv("IS_GCP") == "TRUE":
    load_dotenv("/data/env/current.env")
else:
    load_dotenv()


session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)
redshift_client = session.client("redshift-data")
s3_client = session.client("s3")

database = os.getenv("AWS_REDSHIFT_DB", "dev")
schema = os.getenv("AWS_REDSHIFT_SCHEMA", "public")
tables = os.getenv("AWS_REDSHIFT_TABLES", "").split(",")
iam_role = os.getenv("AWS_IAM_ROLE")
cluster_name = os.getenv("AWS_REDSHIFT_CLUSTER")
db_user = os.getenv("AWS_REDSHIFT_DB_USER")
s3_bucket_name = os.getenv("AWS_S3_BUCKET_NAME")
s3_output_dir = os.getenv("AWS_S3_OUTPUT_DIR")

bq_dst_dataset = os.getenv("GCP_BIGQUERY_DESTINATION_DATASET")


s3_prefix = f"s3://{s3_bucket_name}/{s3_output_dir}/"


if os.getenv("IS_GCP") == "TRUE":
    bigquery_client = bigquery.Client.from_service_account_json(
        "/data/credentials/current.json"
    )
else:
    bigquery_client = bigquery.Client.from_service_account_json("credentials.json")


def get_sql_file(filename: str):
    with open(DIR / ("sql/" + filename + ".sql"), "r") as file:
        return file.read()


def get_unload_query(table_name: str):
    return get_sql_file("unload").format(
        AWS_REDSHIFT_DB=database,
        AWS_REDSHIFT_SCHEMA=schema,
        AWS_REDSHIFT_TABLE=table_name,
        AWS_S3_PREFIX=s3_prefix,
        AWS_IAM_ROLE=iam_role,
    )


def unload_tables():
    print(f"[1/3] Unloading tables to S3...")
    print(f"\tWill unload {len(tables)}.")
    batch_queries = [get_unload_query(table_name) for table_name in tables]
    redshift_client.batch_execute_statement(
        Sqls=batch_queries,
        Database=database,
        ClusterIdentifier=cluster_name,
        DbUser=db_user,
    )
    print("\tDone.\n")


def get_table_schemas():
    print(f"[2/3] Getting table schemas...")

    step_idx = 1
    for table_name in tables:
        print(
            f'\t-> [{step_idx}/{len(tables)}] Getting schema for table "{table_name}"...'
        )
        table_schema = redshift_client.describe_table(
            ClusterIdentifier=cluster_name,
            Database=database,
            DbUser=db_user,
            Schema=schema,
            Table=table_name,
        )
        schema_path = f"tables/{table_name}/schema.json"
        os.makedirs(os.path.dirname(schema_path), exist_ok=True)
        with open(schema_path, "w") as file:
            file.write(json.dumps(table_schema, indent=2))
        step_idx += 1
    print("\tDone.\n")


def get_table_folder_objects(table_folder: str):
    objects = []

    os.makedirs(f"tables/{table_folder}/objects", exist_ok=True)
    objects_in_folder = s3_client.list_objects_v2(
        Bucket=s3_bucket_name, Prefix=f"{s3_output_dir}/{table_folder}/"
    ).get("Contents")
    print(f"\t\tFound {len(objects_in_folder)} objects.")
    step_idx = 1
    for object in objects_in_folder:
        key = object["Key"]
        name = key.split("/")[-1]
        full_path = f"tables/{table_folder}/objects/{name}"
        objects.append(full_path)
        print(f"\t\t-> [{step_idx}/{len(objects_in_folder)}] Downloading {key}", end="")

        with s3_client.get_object(Bucket=s3_bucket_name, Key=key)["Body"] as stream:
            with open(full_path, "wb") as file:
                chunk = stream.read(1024)
                while chunk:
                    file.write(chunk)
                    chunk = stream.read(1024)
                    print(".", end="")
        print("")
        step_idx += 1

    print("\t\tDone.\n")
    return objects


REDSHIFT_TO_BIGQUERY_COLUMN_TYPE = {
    "int2": "integer",  # "smallint"
    "varchar": "string",
    "date": "date",
    "bpchar": "string",
    "bool": "bool",
    "int4": "integer",
    "timestamp": "timestamp",
    "numeric": "float64",
}


def get_table_schema(table_folder: str):
    with open(f"tables/{table_folder}/schema.json", "r") as file:
        schema_json = json.load(file)

    columns = []
    names = []

    for column in schema_json["ColumnList"]:
        type = REDSHIFT_TO_BIGQUERY_COLUMN_TYPE[column["typeName"]]
        if type == "string":
            field = bigquery.SchemaField(
                name=column["name"],
                field_type=type,
                mode="NULLABLE" if column["nullable"] else "REQUIRED",
                max_length=column["length"],
            )
        elif type == "numeric":
            field = bigquery.SchemaField(
                name=column["name"],
                field_type=type,
                mode="NULLABLE" if column["nullable"] else "REQUIRED",
                scale=column["scale"],
                precision=column["precision"],
            )
        else:
            field = bigquery.SchemaField(
                name=column["name"],
                field_type=type,
                mode="NULLABLE" if column["nullable"] else "REQUIRED",
            )

        columns.append(field)
        names.append(column["name"])

    return columns, names


def upload_object(object_path: Path, table_folder: str):
    schema_columns, schema_names = get_table_schema(table_folder)
    job_config = bigquery.LoadJobConfig(
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.CSV,
        schema=schema_columns,
        autodetect=False,
    )
    print("\t\t\t[1/2] Reading CSV...")
    df = pandas.read_csv(object_path, header=0, names=schema_names)

    print("\t\t\t[2/2] Uploading...")
    load_job = bigquery_client.load_table_from_dataframe(
        df, destination=f"{bq_dst_dataset}.{table_folder}", job_config=job_config
    )
    if load_job.result().error_result == None:
        print("\t\t\tDone.\n")


def upload_tables():
    print(f"[3/3] Uploading tables to BigQuery...")

    step_idx = 1
    for table_folder in tables:
        print(
            f'\t-> [{step_idx}/{len(tables)}] Downloading objects from S3 folder "{table_folder}"...'
        )
        folder_object_paths = get_table_folder_objects(table_folder)
        if not folder_object_paths:
            return

        print("\tDone.\n")
        print("\tUploading to BigQuery...")
        substep_idx = 1
        for object_full_path in folder_object_paths:
            print(
                f"\t\t-> [{substep_idx}/{len(folder_object_paths)}] Uploading {str(object_full_path)}..."
            )
            upload_object(object_full_path, table_folder)
            substep_idx += 1
        print("\tDone.\n")

        shutil.rmtree(f"tables/{table_folder}")
        step_idx += 1
    print("\tDone.\n")


def migrate():
    unload_tables()
    get_table_schemas()
    upload_tables()
    print("Finished successfully!")


if __name__ == "__main__":
    migrate()
