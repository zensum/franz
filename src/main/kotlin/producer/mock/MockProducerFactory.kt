package franz.producer.mock

import franz.producer.ProducerFactory

class MockProducerFactory<T, U>(val producer: MockProducer<T, U>): ProducerFactory<T, U> {
    override fun create(opts: Map<String, Any>) =
        producer
}