package franz.producer.kafka_one

import franz.producer.Producer
import franz.producer.ProducerFactory

class KProducerFactory<T, U>: ProducerFactory<T, U> {
    override fun create(opts: Map<String, Any>): Producer<T, U> =
        KProducer(opts)
}