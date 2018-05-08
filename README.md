## To login GCP to run in local machine
gcloud auth application-default login
gcloud auth activate-service-account --key-file=/Users/angus/.config/cloud-service.json
export GOOGLE_APPLICATION_CREDENTIALS=/Users/angus/.config/cloud-service.json


## Bucket
	$ gsutil mb gs://staging-testing-43541281

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
                  --project=traded-risk-project-1 \
                  --stagingLocation=gs://staging-testing-43541281/staging \
                  --templateLocation=gs://staging-testing-43541281/templates/StreamLoaderTemplate"
```


