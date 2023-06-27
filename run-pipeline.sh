# Set up environment variables
export PROJECT_ID=$(gcloud config get-value project)
export REGION='us-central1'
export PIPELINE_FOLDER=gs://dataflow-poc-test-divya
export MAIN_CLASS_NAME=com.mypackage.pipeline.MyPipeline
export RUNNER=DataflowRunner

mvn compile exec:java \
-Dexec.mainClass=${MAIN_CLASS_NAME} \
-Dexec.cleanupDaemonThreads=false \
-Dexec.args=" \
--project=${PROJECT_ID} \
--region=${REGION} \
--stagingLocation=${PIPELINE_FOLDER}/staging \
--tempLocation=${PIPELINE_FOLDER}/temp \
--runner=${RUNNER}"