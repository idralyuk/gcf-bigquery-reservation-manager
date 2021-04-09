## Google Cloud Function to manage BigQuery Reservations.

### How to deploy gcf-bigquery-reservation-manager

  Set up some variables
  
    export GCP_PROJECT=foobar
    export GCP_REGION=europe-west1
    export GCP_RUNTIME=python39
    export GCP_CLOUD_FUNCTION_NAME=bq-reservation-manager
    export GCP_SERVICE_ACCOUNT=${GCP_CLOUD_FUNCTION_NAME}-sa

  Create a service account for the function

    gcloud --project ${GCP_PROJECT} iam service-accounts create ${GCP_SERVICE_ACCOUNT} --description "BQ Reservation Manager Service Account" --display-name "${GCP_SERVICE_ACCOUNT}"

  Grant the service account _BigQuery Resource Admin_ role to manage BigQuery reservations

    gcloud --project ${GCP_PROJECT} projects add-iam-policy-binding ${GCP_PROJECT} --member="serviceAccount:${GCP_SERVICE_ACCOUNT}@${GCP_PROJECT}.iam.gserviceaccount.com" --role "roles/bigquery.resourceAdmin"

  Deploy the function
  
    gcloud --project ${GCP_PROJECT} functions deploy ${GCP_CLOUD_FUNCTION_NAME} \
      	--quiet \
      	--region ${GCP_REGION} \
      	--runtime ${GCP_RUNTIME} \
      	--security-level secure-always \
      	--trigger-http \
      	--entry-point main_http \
      	--timeout 180s \
      	--memory 128MB \
      	--service-account ${GCP_SERVICE_ACCOUNT}@${GCP_PROJECT}.iam.gserviceaccount.com

  If using the same service account to invoke the function from Cloud Scheduler, also grant the _Cloud Functions Invoker_ role for the function
  
    gcloud --project ${GCP_PROJECT} functions add-iam-policy-binding bq-reservation-manager --region=${GCP_REGION} --member=serviceAccount:${GCP_SERVICE_ACCOUNT}@${GCP_PROJECT}.iam.gserviceaccount.com --role=roles/cloudfunctions.invoker

## How to invoke

  POST the following to retrieve a report of current capacity commitments, reservations and assignments

    {
        "operation": "report",
        "project_id": "foobar",
        "location": "EU"
    }

  POST the following to remove all current capacity commitments, reservations and assignments

    {
        "operation": "cleanup",
        "project_id": "foobar",
        "location": "EU"
    }

  POST the following to purchase a capacity commitment, and create a reservation and an assignment

    {
        "operation": "purchase",
        "project_id": "foobar",
        "location": "EU",
        "slots": 400
    }
