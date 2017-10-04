package franz.engine.kafka_one

import franz.Message
import org.apache.kafka.clients.consumer.ConsumerRecord

class KafkaMessage<T, U>(private val rec: ConsumerRecord<T, U>) : Message<T, U> {
    override fun value(): U = rec.value()
    override fun key(): T = rec.key()
    override fun headers(): Array<Pair<String, ByteArray>> = TODO("not implemented")
    override fun headers(key: String): Array<ByteArray> = TODO("not implemented")
    override fun topic(): String = rec.topic()
    fun jobId(): JobId = rec.jobId()
}