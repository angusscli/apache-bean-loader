## To login GCP to run in local machine
gcloud auth application-default login
gcloud auth activate-service-account --key-file=/Users/angusleigh/.config/cloud-service.json
export GOOGLE_APPLICATION_CREDENTIALS=/Users/angusleigh/.config/cloud-service.json

## To Compile
mvn clean install

## To run
mvn compile exec:java -Dexec.mainClass=com.sample.cloud.loader.StreamLoaderPipeline -Dexec.args="--runner=DataflowRunner" -Pdataflow-runner

## Create BigQuery Dataset & Bucket
	$ bq mk --dataset --data_location US poc
	$ gsutil mb gs://staging-testing-43541281