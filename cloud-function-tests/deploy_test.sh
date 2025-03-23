gcloud functions deploy run-tests \
  --gen2 --runtime=python312 --region=us-central1 \
  --source=. --entry-point=run_tests --trigger-http \
  --allow-unauthenticated
