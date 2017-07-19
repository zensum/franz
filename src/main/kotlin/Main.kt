package franz

import mu.KotlinLogging
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.util.*

fun createProducer(host: String) = mapOf(
        "bootstrap.servers" to listOf(host),
        "key.serializer" to "org.apache.kafka.common.serialization.StringSerializer",
        "value.serializer" to "org.apache.kafka.common.serialization.StringSerializer",
        "acks" to "all",
        "compression.type" to "gzip",
        "timeout.ms" to "10000",
        "request.timeout.ms" to "10000",
        "metadata.fetch.timeout.ms" to "5000",
        "retries" to "0"
).let { KafkaProducer<String, String>(it) }

val logger = KotlinLogging.logger {}

fun main(args: Array<String>) {
    val rnd = Random()
    WorkerBuilder()
            .subscribedTo("my-topic")
            .parallelism(1)
            .running {
                when(value) {
                    "ThisIsFine" -> if (rnd.nextBoolean()) success else transientFailure(RuntimeException("This is fine!"))
                    "ThisIsBad" -> permanentFailure(RuntimeException("It was bad"))
                    else -> success
                }
            }
            .start()

    val p = createProducer("127.0.0.1:9092")
    while(true) {
        p.send(ProducerRecord("my-topic", "foo", "ThisIsFine"))
        p.send(ProducerRecord("my-topic", "foo", "ThisIsBad"))
        p.send(ProducerRecord("my-topic", "foo", "ThisIsGood"))
        p.flush()
        Thread.sleep(2000)
    }
}
