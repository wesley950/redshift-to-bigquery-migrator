# build the image
gcloud builds submit \
--pack image=gcr.io/${PROJECT_ID}/redshift-to-bigquery-migrator \
--project=${PROJECT_ID}

# create function
gcloud beta run jobs create ${FUNCTION_NAME} \
--project=${PROJECT_ID} \
--image gcr.io/${PROJECT_ID}/redshift-to-bigquery-migrator \
--tasks 1 \
--max-retries 1 \
--region us-central1 \
--memory=16G \
--cpu=4 \
--set-env-vars=IS_GCP="TRUE" \
--set-secrets=/data/env/current.env=redshift_to_bigquery_migration_env:latest,/data/credentials/current.json=gcp_credentials:latest \
--task-timeout=3600
