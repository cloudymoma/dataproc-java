#!/bin/bash

# --- Configuration Variables ---
# Your GCP Project ID
export PROJECT_ID=$(gcloud config get-value project)

# The GCP region to run the job in. Choose one near you or your data.
# Since you are in Beijing, asia-east2 (Hong Kong) or asia-northeast1 (Tokyo) are good choices.
export REGION="us-central1" # Example: Changhua County, Taiwan

# A unique name for this specific batch job run
export BATCH_ID="list-gcs-$(date +%Y%m%d-%H%M%S)"

LOCAL_JAR_PATH=/usr/local/google/home/binwu/workspace/customers/yeahmobi/listgcs/target/listgcs-1.0-SNAPSHOT-jar-with-dependencies.jar
# The GCS path to your JAR, defined in the previous step
GCS_JAR_PATH="gs://dingoproc/jars/listgcs-1.0-SNAPSHOT-jar-with-dependencies.jar"

# The GCS bucket for staging dependencies and logs
BUCKET_NAME="dingoproc"

export PHS_CLUSTER_NAME="dingohist"
export PHS_RESOURCE_NAME="projects/$PROJECT_ID/regions/$REGION/clusters/$PHS_CLUSTER_NAME"

#sbt clean package
gcloud storage cp $LOCAL_JAR_PATH $GCS_JAR_PATH

# --- The gcloud Command ---
gcloud dataproc batches submit spark \
    --project=$PROJECT_ID \
    --region=$REGION \
    --batch=$BATCH_ID \
    --class=com.example.App \
    --jars=$GCS_JAR_PATH \
    --deps-bucket=gs://$BUCKET_NAME/staging \
    --subnet=default \
    --version 2.3 \
    --history-server-cluster=$PHS_RESOURCE_NAME \
    --autotuning-scenarios=auto \
    --properties \
      "spark.executor.cores=4, \
      spark.executor.memory=25g, \
      spark.executor.memoryOverhead=4g, \
      spark.executor.instances=2, \
      spark.driver.cores=4, \
      spark.driver.memory=25g, \
      spark.driver.memoryOverhead=4g, \
      spark.dynamicAllocation.enabled=true, \
      spark.dynamicAllocation.minExecutors=2, \
      spark.dynamicAllocation.maxExecutors=3, \
      spark.dynamicAllocation.executorAllocationRatio=1.0, \
      spark.decommission.maxRatio=0.3, \
      spark.reducer.fetchMigratedShuffle.enabled=true, \
      spark.shuffle.service.enabled=true, \
      spark.dataproc.scaling.version=2, \
      spark.dataproc.driver.compute.tier=premium, \
      spark.dataproc.executor.compute.tier=premium, \
      spark.dataproc.driver.disk.tier=premium, \
      spark.dataproc.driver.disk.size=375g, \
      spark.dataproc.executor.disk.tier=premium, \
+     spark.dataproc.executor.disk.size=375g, \
      spark.default.parallelism=1000, \
      spark.sql.shuffle.partitions=1000, \
      spark.memory.fraction=0.6, \
      spark.memory.storageFraction=0.5, \
      spark.sql.adaptive.enabled=true, \
      spark.sql.adaptive.coalescePartitions.enabled=true, \
      spark.sql.adaptive.skewJoin.enabled=true, \
      spark.dataproc.enhanced.optimizer.enabled=true, \
      spark.dataproc.enhanced.execution.enabled=true, \
      spark.network.timeout=300s, \
      spark.executor.heartbeatInterval=60s, \
      spark.speculation=true, \
      spark.dataproc.enableNativeExecution=true, \
      spark.dataproc.nativeExecution.jvmHeapSizeFraction=0.8, \
      dataproc.gcsConnector.version=3.1.2, \
      dataproc.sparkBqConnector.version=0.42.3, \
      dataproc.profiling.enabled=true, \
      dataproc.profiling.name=dingoserverless" \
    -- \
    gs://dingoproc/flink_state_backend \
    --spark.driver.log.level=INFO
