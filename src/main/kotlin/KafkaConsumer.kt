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
        "enable.auto.commit" to "false"
)

private fun makeConfig(userOpts: Map<String, Any>) =
        userOpts + sensibleDefaults + defaultsFromEnv()

fun <T, U> kafkaConsumer(opts: Map<String, Any>, topics: List<String>) =
        KafkaConsumer<T, U>(makeConfig(opts)).apply {
            subscribe(topics)
        }
