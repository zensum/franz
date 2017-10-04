package franz

import franz.producer.Producer
import franz.producer.kafka_one.KProducer

private val stringSer = "org.apache.kafka.common.serialization.StringSerializer"
private val byteArraySer = "org.apache.kafka.common.serialization.ByteArraySerializer"
private val valueSerKey = "value.serializer"

class ProducerBuilder<T> private constructor(private val opts: Map<String, Any> = emptyMap()) {
    fun option(k: String, v: Any) = options(mapOf(k to v))
    fun options(newOpts: Map<String, Any>) = ProducerBuilder<T>(opts + newOpts)
    fun create(): Producer<String, T> = KProducer(opts)
    companion object {
        val ofByteArray = ProducerBuilder<ByteArray>().option(valueSerKey, byteArraySer)
        val ofString = ProducerBuilder<String>().option(valueSerKey, stringSer)
    }
}