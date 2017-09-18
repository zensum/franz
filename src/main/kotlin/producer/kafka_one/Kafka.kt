package franz.producer.kafka_one

import franz.engine.kafka_one.createKafkaConfig
import org.apache.kafka.clients.producer.KafkaProducer

private val sensibleDefaults = mapOf(
    "key.serializer" to "org.apache.kafka.common.serialization.StringSerializer",
    "acks" to "all",
    "compression.type" to "gzip",
    "request.timeout.ms" to "10000",
    "max.block.ms" to "5000",
    "retries" to "0"
)

private fun makeConfig(userOpts: Map<String, Any>) =
    createKafkaConfig(userOpts, sensibleDefaults)
internal fun <K, V> makeProducer(userOpts: Map<String, Any>) =
    KafkaProducer<K, V>(makeConfig(userOpts))