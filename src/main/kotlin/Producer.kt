package franz

import franz.producer.Producer
import franz.producer.ProducerFactory
import franz.producer.kafka_one.KProducerFactory

private val stringSer = "org.apache.kafka.common.serialization.StringSerializer"
private val byteArraySer = "org.apache.kafka.common.serialization.ByteArraySerializer"
private val valueSerKey = "value.serializer"

data class ProducerBuilder<T> private constructor(
    private val opts: Map<String, Any> = emptyMap(),
    private val producer: ProducerFactory = KProducerFactory
) {
    fun option(k: String, v: Any) = options(mapOf(k to v))
    fun options(newOpts: Map<String, Any>) = ProducerBuilder<T>(opts + newOpts)
    fun setProducer(p: ProducerFactory) = copy(producer = p)
    fun create(): Producer<String, T> = producer.create(opts)
    companion object {
        val ofByteArray = ProducerBuilder<ByteArray>().option(valueSerKey, byteArraySer)
        val ofString = ProducerBuilder<String>().option(valueSerKey, stringSer)
    }
}