package franz.producer.kafka_one

import franz.producer.Producer
import franz.producer.ProducerFactory

object KProducerFactory: ProducerFactory {
    override fun <K, U> create(opts: Map<String, Any>): Producer<K, U> =
        KProducer(opts)
}