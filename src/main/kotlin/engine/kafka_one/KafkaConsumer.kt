package franz.engine.kafka_one

import org.apache.kafka.clients.consumer.KafkaConsumer

private val sensibleDefaults = mapOf(
        "key.deserializer" to "org.apache.kafka.common.serialization.StringDeserializer",
        "enable.auto.commit" to "false"
)

private fun makeConfig(opts: Map<String, Any>) = createKafkaConfig(opts, sensibleDefaults)

fun <T, U> kafkaConsumer(opts: Map<String, Any>, topics: List<String>) =
        KafkaConsumer<T, U>(makeConfig(opts)).apply {
            subscribe(topics)
        }
