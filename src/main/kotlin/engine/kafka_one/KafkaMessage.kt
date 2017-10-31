package franz.engine.kafka_one

import franz.Message
import org.apache.kafka.clients.consumer.ConsumerRecord

class KafkaMessage<T, U>(private val rec: ConsumerRecord<T, U>) : Message<T, U> {
    override fun value(): U = rec.value()
    override fun key(): T = rec.key()
    override fun headers(): Array<Pair<String, ByteArray>> = rec.headers()
            .filterNot { null == it.key() || null == it.value() }
            .map { Pair(it.key()!!, it.value()!!) }
            .toTypedArray()
    override fun headers(key: String): Array<ByteArray> = rec.headers()
            .filter { it.key() == key }
            .map { it.value() }
            .toTypedArray()
    override fun timestamp(): Long = rec.timestamp()
    override fun topic(): String = rec.topic()
    fun jobId(): JobId = rec.jobId()
}