package frans.producer.mock

import franz.producer.Producer
import franz.producer.ProducerFactory

object MockProducerFactory: ProducerFactory {
    override fun <K, U> create(opts: Map<String, Any>): Producer<K, U> =
        MockProducer()
}