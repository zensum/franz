package franz.internal

import org.apache.kafka.clients.consumer.KafkaConsumer

private fun defaultsFromEnv() = System.getenv("KAFKA_HOST").let {
    if (it == null || it.length < 1)
        emptyMap()
    else
        mapOf("bootstrap.servers" to listOf(it))
}

private val sensibleDefaults = mapOf(
        "key.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer",
        "value.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer",
        "enable.auto.commit" to "false",
        "compression.type" to "gzip",
)

private fun makeConfig(userOpts: Map<String, Any>) =
        userOpts + sensibleDefaults + defaultsFromEnv()

fun kafkaConsumer(opts: Map<String, Any>, topics: List<String>) =
        KafkaConsumer<String, String>(makeConfig(opts)).apply {
            subscribe(topics)
        }
