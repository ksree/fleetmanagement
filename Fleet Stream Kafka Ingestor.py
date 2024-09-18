# Databricks notebook source
# DBTITLE 1,Confluent Kafka Config
confluentClusterName = "cluster_0"
confluentBootstrapServers = "pkc-12576z.us-west2.gcp.confluent.cloud:9092"

confluentApiKey = dbutils.secrets.get(scope="dev-ksr", key="confluentApiKey")
confluentSecret = dbutils.secrets.get(scope="dev-ksr", key="confluentSecret")
confluentSchemaRegistryApiKey = dbutils.secrets.get(scope="dev-ksr", key="confluentSchemaRegistryApiKey")
confluentSchemaRegistrySecret = dbutils.secrets.get(scope="dev-ksr", key="confluentSchemaRegistrySecret")

confluentTopicName = "fleet_management_sensors"
checkpointPath = "/Volumes/kapil_osm/default/operational/fleetmanagmentcheckpoint"

# COMMAND ----------

# DBTITLE 1,Confluent Schema Registry Config

schema_registry_address = "https://psrc-1x85j.us-central1.gcp.confluent.cloud"
schema_registry_options = {
    "confluent.schema.registry.basic.auth.credentials.source": "USER_INFO",
    "confluent.schema.registry.basic.auth.user.info": f"{confluentSchemaRegistryApiKey}:{confluentSchemaRegistrySecret}",
    "avroSchemaEvolutionMode": "restart",
    "mode": "FAILFAST",
}

# COMMAND ----------

# DBTITLE 1,Temporary fix for timeouts on Serverless Streaming jobs
from time import sleep
import dbruntime.databricks_connect_streaming_listener as dcsl
listener = display.__self__.streaming_listener
on_start = dcsl.DatabricksNotebookStreamingCallback(listener, streaming_timeout=1_000_000)
spark._register_streaming_callback(on_start.get_callback())

# COMMAND ----------

# DBTITLE 1,Read stream from Kafka topic and parse Avro
from pyspark.sql.avro.functions import *
from pyspark.sql.functions import col

fleetManagementSensorsBronzeDf = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", confluentBootstrapServers)
    .option("kafka.security.protocol", "SASL_SSL")
    .option(
        "kafka.sasl.jaas.config",
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(
            confluentApiKey, confluentSecret
        ),
    )
    .option("kafka.ssl.endpoint.identification.algorithm", "https")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("subscribe", confluentTopicName)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
    .select(
        "topic",
        "partition",
        "offset",
        "timestamp",
        from_avro(
            data=col("value"),
            options=schema_registry_options,
            subject="fleet_management_sensors-value",
            schemaRegistryAddress=schema_registry_address,
        ).alias("value"),
    )
    .select("topic", "partition", "offset", "timestamp", "value.*")
)

# COMMAND ----------

# DBTITLE 1,Write stream sensor data to delta table
fleetManagementSensorsBronzeDf.writeStream.option(
    "checkpointLocation", checkpointPath
).option("mergeSchema", "true").outputMode("append").trigger(
    availableNow=True
).queryName(
    "fleetManagementSensorsBronzeWriteStream"
).toTable(
    "kapil_osm.default.fleet_management_sensors_bronze"
)
