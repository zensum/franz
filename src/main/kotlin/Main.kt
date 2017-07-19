package franz

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord

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

fun main(args: Array<String>) {
    WorkerBuilder()
            .subscribedTo("my-topic")
            .parallelism(1)
            .option("my-option", "some-value")
            .running {
                println("Do someshit with $key and $value")
                if (value == "crazy") {
                    permanentFailure(RuntimeException("yeah"))
                } else {
                    success
                }
            }
            .start()

    val p = createProducer("127.0.0.1:9092")
    while(true) {
        p.send(ProducerRecord("my-topic", "foo", "lol"))
        Thread.sleep(500)
    }
}
