## To login GCP to run in local machine
gcloud auth application-default login
gcloud auth activate-service-account --key-file=/Users/angus/.config/cloud-service.json
export GOOGLE_APPLICATION_CREDENTIALS=/Users/angus/.config/cloud-service.json


## Bucket
	$ gsutil mb gs://hackathon-staging-alphastock

## To Compile & Run
`Maven build`

```
mvn clean install -DskipTests=true
```

`build template`

```
mvn compile exec:java \
     -Dexec.mainClass=com.sample.cloud.loader.StreamLoaderPipeline \
     -Dexec.args="--runner=DataflowRunner \
                  --project=techfest-hackathon-1 \
                  --stagingLocation=gs://hackathon-staging-alphastock/staging \
                  --templateLocation=gs://hackathon-staging-alphastock/templates/StreamLoaderTemplate"
```

## To Run

`curl https://us-central1-techfest-hackathon-1.cloudfunctions.net/dataflow-trigger`

